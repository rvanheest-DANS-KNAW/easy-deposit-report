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
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositPropertiesRepository.{ FindByDatasetId, GetSummaryReportData, ListDepositsToBeCleaned }
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit.properties.graphql.direction.Forwards
import nl.knaw.dans.lib.error._
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

  override def load(depositId: DepositId): Try[DepositProperties with FileSystemDeposit] = {
    // @formatter:off
    (
      (sword2DepositsDir -> "SWORD2") #::
        (ingestFlowInbox -> "INGEST_FLOW") #::
        ingestFlowInboxArchived.map(_ -> "INGEST_FLOW_ARCHIVED").toStream
    )
    // @formatter:on
      .map { case (dir, location) => (dir / depositId) -> location }
      .collectFirst { case (deposit, location) if deposit.exists => Success(from(depositId, deposit, location)) }
      .getOrElse(Failure(DepositDoesNotExist(depositId)))
  }

  private def from(depositId: DepositId, deposit: Deposit, location: String) = {
    val loc = location
    new ServiceDepositProperties(depositId, loc, client) with FileSystemDeposit {
      override val depositPath: Deposit = deposit
      override val location: String = loc
    }
  }

  override def getSummaryReportData(depositor: Option[DepositorId],
                                    datamanager: Option[Datamanager],
                                    age: Option[Age],
                                   ): Try[SummaryReportData] = {
    val dmFilter = datamanager
      .map(datamanager => Map("curator" -> ("userId" -> datamanager) ~ ("filter" -> "LATEST")))
      .getOrElse(Map.empty)
    val ageFilter = age
      .map(age => Map[String, JValue]("laterThan" -> now.minusDays(age).toString(dateTimeFormatter)))
      .getOrElse(Map.empty)

    depositor
      .map(depositorId => {
        val vars = Map[String, JValue]("depositorId" -> depositorId) ++ dmFilter ++ ageFilter
        client.doQuery(GetSummaryReportData.queryWithDepositor, GetSummaryReportData.operationName, vars)
          .map(_.extract[GetSummaryReportData.DataWithDepositor].depositor.mapValues(_.totalCount))
      })
      .getOrElse {
        client.doQuery(GetSummaryReportData.queryWithoutDepositor, GetSummaryReportData.operationName, dmFilter ++ ageFilter)
          .map(_.extract[GetSummaryReportData.DataWithoutDepositor].mapValues(_.totalCount))
      }
      .map(totals => {
        val total = totals("total")
        val states = totals - "total"
        val totalPerState = states.map { case (stateString, total) => State.withName(stateString.toUpperCase) -> total }
        SummaryReportData(total, totalPerState)
      })
      .toTry
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
      .map(_.toStream.flatMap(_.edges.map(_.node.depositId).map(load(_).unsafeGetOrThrow)))
  }
}

object ServiceDepositPropertiesRepository {

  object GetSummaryReportData {
    case class DataWithDepositor(depositor: DataWithoutDepositor)
    type DataWithoutDepositor = Map[String, Total]
    case class Total(totalCount: Int)

    val operationName = "GetSummaryReportData"
    val queryWithDepositor: String =
      """query DepositsPerState($depositorId: String!, $curator: DepositCuratorFilter, $laterThan: DateTime) {
        |  depositor(id: $depositorId) {
        |    total: deposits(curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    draft: deposits(state: {label: DRAFT}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    uploaded: deposits(state: {label: UPLOADED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    finalizing: deposits(state: {label: FINALIZING}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    invalid: deposits(state: {label: INVALID}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    submitted: deposits(state: {label: SUBMITTED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    rejected: deposits(state: {label: REJECTED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    failed: deposits(state: {label: FAILED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    in_review: deposits(state: {label: IN_REVIEW}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    archived: deposits(state: {label: ARCHIVED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |    fedora_archived: deposits(state: {label: FEDORA_ARCHIVED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |      totalCount
        |    }
        |  }
        |}""".stripMargin
    val queryWithoutDepositor: String =
      """query DepositsPerState($curator: DepositCuratorFilter, $laterThan: DateTime) {
        |  total: deposits(curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  draft: deposits(state: {label: DRAFT}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  uploaded: deposits(state: {label: UPLOADED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  finalizing: deposits(state: {label: FINALIZING}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  invalid: deposits(state: {label: INVALID}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  submitted: deposits(state: {label: SUBMITTED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  rejected: deposits(state: {label: REJECTED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  failed: deposits(state: {label: FAILED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  in_review: deposits(state: {label: IN_REVIEW}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  archived: deposits(state: {label: ARCHIVED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |  fedora_archived: deposits(state: {label: FEDORA_ARCHIVED}, curator: $curator, lastModifiedLaterThan: $laterThan) {
        |    totalCount
        |  }
        |}""".stripMargin
  }

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
