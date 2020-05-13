/**
 * Copyright (C) 2017 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.easy.managedeposit.properties

import better.files.File
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositPropertiesRepository.{ FindByDatasetId, ListDepositsToBeCleaned }
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit.properties.graphql.direction.Forwards
import org.joda.time.{ DateTime, DateTimeZone }
import org.json4s.JsonAST.{ JInt, JString }
import org.json4s.JsonDSL._
import org.json4s.{ Formats, JValue }

import scala.util.{ Failure, Success, Try }

class ServiceDepositPropertiesRepository(client: GraphQLClient,
                                         sword2DepositsDir: File,
                                         ingestFlowInbox: File,
                                         ingestFlowInboxArchived: Option[File],
                                        )(implicit formats: Formats) extends DepositPropertiesRepository {

  override def load(depositId: DepositId): Try[DepositProperties] = Try {
    new ServiceDepositProperties(depositId, client)
  }

  override def listReportData(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[Stream[DepositInformation]] = ???

  private def now: DateTime = DateTime.now(DateTimeZone.UTC)

  override def getCurationParametersByDatasetId(datasetId: DatasetId): Try[(DepositId, Option[State])] = {
    for {
      json <- client.doQuery(FindByDatasetId.query, FindByDatasetId.operationName, Map("datasetId" -> datasetId)).toTry
      identifier = json.extract[FindByDatasetId.Data].identifier
      result <- identifier
        .map(identifier => {
          val deposit = identifier.deposit
          Success(deposit.depositId -> deposit.state.map(_.label))
        })
        .getOrElse(Failure(DatasetDoesNotExist(datasetId)))
    } yield result
  }

  override def listDepositsToBeCleaned(filterOnDepositor: Option[DepositorId],
                                       filterOnAge: Age,
                                       filterOnState: State,
                                      ): Try[Stream[DepositProperties with FileSystemDeposit]] = {
    implicit val convertJson: Any => JValue = {
      case s: String => JString(s)
      case i: Int => JInt(i)
    }
    val variables = Map(
      "earlierThan" -> now.minusDays(filterOnAge).toString(dateTimeFormatter),
      "state" -> filterOnState.toString,
      "count" -> 100,
    )
    filterOnDepositor
      .map(depositorId => {
        client.doPaginatedQuery[ListDepositsToBeCleaned.DataWithDepositor](
          query = ListDepositsToBeCleaned.queryWithDepositor,
          operationName = ListDepositsToBeCleaned.operationName,
          variables = variables + ("depositorId" -> depositorId),
          direction = Forwards,
        )(_.depositor.deposits.pageInfo)
          .map(_.map(_.depositor.deposits))
      })
      .getOrElse {
        client.doPaginatedQuery[ListDepositsToBeCleaned.DataWithoutDepositor](
          query = ListDepositsToBeCleaned.queryWithoutDepositor,
          operationName = ListDepositsToBeCleaned.operationName,
          variables = variables,
          direction = Forwards,
        )(_.deposits.pageInfo)
          .map(_.map(_.deposits))
      }
      .map(_.toStream.flatMap(_.edges.map(_.node.depositId).map(mkDepositProperties)))
  }

  private def mkDepositProperties(depositId: DepositId): DepositProperties with FileSystemDeposit = {
    new ServiceDepositProperties(depositId, client) with FileSystemDeposit {
      override protected val depositPath: Deposit = {
        (sword2DepositsDir #:: ingestFlowInbox #:: ingestFlowInboxArchived.toStream)
          .map(_ / depositId)
          .find(_.exists)
          .getOrElse { throw DepositDoesNotExist(depositId) }
      }
    }
  }
}

object ServiceDepositPropertiesRepository {

  object FindByDatasetId {
    case class Data(identifier: Option[Identifier])
    case class Identifier(deposit: Deposit)
    case class Deposit(depositId: DepositId, state: Option[StateObj])
    case class StateObj(label: State)

    val operationName = "FindByDatasetId"
    val query: String =
      """query FindByDatasetId($datasetId: String!) {
        |  identifier(type: FEDORA, value: $datasetId) {
        |    deposit {
        |      depositId
        |      state {
        |        label
        |      }
        |    }
        |  }
        |}""".stripMargin
  }

  object ListDepositsToBeCleaned {
    case class DataWithDepositor(depositor: Depositor)
    case class DataWithoutDepositor(deposits: Deposits)
    case class Depositor(deposits: Deposits)
    case class Deposits(pageInfo: Forwards, edges: Seq[Edge])
    case class Edge(node: Node)
    case class Node(depositId: DepositId)

    val operationName = "ListDepositsToBeCleaned"
    val queryWithDepositor: String =
      """query ListDepositsToBeCleaned($depositorId: String!, $earlierThan: DateTime, $state: StateLabel!, $count: Int!, $after: String) {
        |  depositor(id: $depositorId) {
        |    deposits(earlierThan: $earlierThan, state: {label: $state, filter: LATEST}, first: $count, after: $after) {
        |      pageInfo {
        |        hasNextPage
        |        endCursor
        |      }
        |      edges {
        |        node {
        |          depositId
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
    val queryWithoutDepositor: String =
      """query ListDepositsToBeCleaned($earlierThan: DateTime, $state: StateLabel!, $count: Int!, $after: String) {
        |  deposits(earlierThan: $earlierThan, state: {label: $state, filter: LATEST}, first: $count, after: $after) {
        |    pageInfo {
        |        hasNextPage
        |        endCursor
        |    }
        |    edges {
        |      node {
        |        depositId
        |      }
        |    }
        |  }
        |}""".stripMargin
  }
}
