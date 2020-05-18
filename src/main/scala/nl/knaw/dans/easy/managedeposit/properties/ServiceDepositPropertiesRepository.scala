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
import nl.knaw.dans.easy.managedeposit.Location.Location
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositPropertiesRepository.{ FindByDatasetId, GetSummaryReportData, ListDepositsToBeCleaned, ListReportData }
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
                                        )(implicit dansDoiPrefixes: List[String], formats: Formats) extends DepositPropertiesRepository {

  override def load(depositId: DepositId): Try[DepositProperties with FileSystemDeposit] = {
    loadOpt(depositId)
      .map(Success(_))
      .getOrElse(Failure(DepositDoesNotExist(depositId)))
  }

  private def loadOpt(depositId: DepositId): Option[ServiceDepositProperties with FileSystemDeposit] = {
    // @formatter:off
    (
      (sword2DepositsDir -> Location.SWORD2) #::
        (ingestFlowInbox -> Location.INGEST_FLOW) #::
        ingestFlowInboxArchived.map(_ -> Location.INGEST_FLOW_ARCHIVED).toStream
    )
    // @formatter:on
      .map { case (dir, location) => (dir / depositId) -> location }
      .collectFirst { case (deposit, location) if deposit.exists => from(depositId, deposit, location) }
  }

  private def from(depositId: DepositId, deposit: Deposit, location: Location) = {
    val loc = location
    new ServiceDepositProperties(depositId, loc, client) with FileSystemDeposit {
      override val depositPath: Deposit = deposit
      override val location: Location = loc
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
    val variables = dmFilter ++ ageFilter

    depositor
      .map(depositorId => {
        val vars = Map[String, JValue]("depositorId" -> depositorId) ++ variables
        client.doQuery(GetSummaryReportData.queryWithDepositor, GetSummaryReportData.operationName, vars)
          .map(_.extract[GetSummaryReportData.DataWithDepositor].depositor.mapValues(_.totalCount))
      })
      .getOrElse {
        client.doQuery(GetSummaryReportData.queryWithoutDepositor, GetSummaryReportData.operationName, variables)
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

  override def listReportData(depositor: Option[DepositorId],
                              datamanager: Option[Datamanager],
                              age: Option[Age],
                             ): Try[Stream[DepositInformation]] = {
    implicit val convertJson: Any => JValue = {
      case s: String => JString(s)
      case i: Int => JInt(i)
      case v: JValue => v
    }
    val dmFilter = datamanager
      .map(datamanager => Map("curator" -> ("userId" -> datamanager) ~ ("filter" -> "LATEST")))
      .getOrElse(Map.empty)
    val ageFilter = age
      .map(age => Map[String, JValue]("laterThan" -> now.minusDays(age).toString(dateTimeFormatter)))
      .getOrElse(Map.empty)
    val variables: Map[String, JValue] = dmFilter ++ ageFilter + ("count" -> 100)

    depositor
      .map(depositorId => {
        client.doPaginatedQuery[ListReportData.DataWithDepositor](
          query = ListReportData.queryWithDepositor,
          operationName = ListReportData.operationName,
          variables = Map[String, JValue]("depositorId" -> depositorId) ++ variables,
          direction = Forwards,
        )(_.depositor.deposits.pageInfo)
          .map(_.map(_.depositor.deposits))
      })
      .getOrElse {
        client.doPaginatedQuery[ListReportData.DataWithoutDepositor](
          query = ListReportData.queryWithoutDepositor,
          operationName = ListReportData.operationName,
          variables = variables,
          direction = Forwards,
        )(_.deposits.pageInfo)
          .map(_.map(_.deposits))
      }
      .map(_.toStream.flatMap(_.edges.map(edge => toDepositInformation(edge.node))))
  }

  private def toDepositInformation(deposit: ListReportData.Deposit): DepositInformation = {
    // @formatter:off
    val location = (
      (sword2DepositsDir -> Location.SWORD2) #::
        (ingestFlowInbox -> Location.INGEST_FLOW) #::
        ingestFlowInboxArchived.map(_ -> Location.INGEST_FLOW_ARCHIVED).toStream
    )
    // @formatter:on
      .map { case (dir, location) => (dir / deposit.depositId) -> location }
      .collectFirst { case (deposit, location) if deposit.exists => location }
      .getOrElse(Location.UNKNOWN)

    DepositInformation(
      depositId = deposit.depositId,
      depositor = deposit.depositor.depositorId,
      datamanager = deposit.curator.map(_.userId),
      dansDoiRegistered = deposit.doiRegistered,
      doiIdentifier = deposit.doi.map(_.value),
      fedoraIdentifier = deposit.fedora.map(_.value),
      state = deposit.state.map(_.label),
      description = deposit.state.map(_.description),
      creationTimestamp = deposit.creationTimestamp,
      lastModified = deposit.lastModified,
      origin = deposit.origin,
      location = location,
      bagDirName = deposit.bagName.getOrElse(notAvailable),
    )
  }

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
      .map(_.toStream.flatMap(_.edges.map(_.node.depositId).flatMap(loadOpt)))
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

  object ListReportData {
    case class DataWithDepositor(depositor: Depositor)
    case class DataWithoutDepositor(deposits: Deposits)
    case class Depositor(deposits: Deposits)
    case class Deposits(pageInfo: Forwards, edges: Seq[Edge])
    case class Edge(node: Deposit)
    case class Deposit(depositId: DepositId,
                       depositor: InnerDepositor,
                       bagName: Option[String],
                       origin: String,
                       creationTimestamp: String,
                       lastModified: String,
                       state: Option[StateObj],
                       doi: Option[Identifier],
                       fedora: Option[Identifier],
                       doiRegistered: Option[Boolean],
                       curator: Option[Curator],
                      )
    case class InnerDepositor(depositorId: DepositorId)
    case class StateObj(label: State, description: String)
    case class Identifier(value: String)
    case class Curator(userId: String)

    val operationName = "ListReportData"
    val queryWithDepositor: String =
      """query ListReportData($depositorId: String!, $curator: DepositCuratorFilter, $laterThan: DateTime, $count: Int!, $after: String) {
        |  depositor(id: $depositorId) {
        |    deposits(curator: $curator, lastModifiedLaterThan: $laterThan, orderBy: {field: CREATION_TIMESTAMP, direction: ASC}, first: $count, after: $after) {
        |      pageInfo {
        |        hasNextPage
        |        endCursor
        |      }
        |      edges {
        |        node {
        |          depositId
        |          depositor {
        |            depositorId
        |          }
        |          bagName
        |          origin
        |          creationTimestamp
        |          lastModified
        |          state {
        |            label
        |            description
        |          }
        |          doi: identifier(type: DOI) {
        |            value
        |          }
        |          fedora: identifier(type: FEDORA) {
        |            value
        |          }
        |          doiRegistered
        |          curator {
        |            userId
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
    val queryWithoutDepositor: String =
      """query ListReportData($curator: DepositCuratorFilter, $laterThan: DateTime, $count: Int!, $after: String) {
        |  deposits(curator: $curator, lastModifiedLaterThan: $laterThan, orderBy: {field: CREATION_TIMESTAMP, direction: ASC}, first: $count, after: $after) {
        |    pageInfo {
        |      hasNextPage
        |      endCursor
        |    }
        |    edges {
        |      node {
        |        depositId
        |        depositor {
        |          depositorId
        |        }
        |        bagName
        |        origin
        |        creationTimestamp
        |        lastModified
        |        state {
        |          label
        |          description
        |        }
        |        doi: identifier(type: DOI) {
        |          value
        |        }
        |        fedora: identifier(type: FEDORA) {
        |          value
        |        }
        |        doiRegistered
        |        curator {
        |          userId
        |        }
        |      }
        |    }
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
