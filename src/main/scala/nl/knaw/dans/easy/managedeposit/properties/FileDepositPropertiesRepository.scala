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
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

class FileDepositPropertiesRepository(sword2DepositsDir: File,
                                      ingestFlowInbox: File,
                                      ingestFlowInboxArchived: Option[File],
                                     )(implicit dansDoiPrefixes: List[String]) extends DepositPropertiesRepository with DebugEnhancedLogging {

  private def listDeposits(dir: File, location: Location): Stream[FileDepositProperties] = {
    dir.list.toStream
      .collect { case file if file.isDirectory => new FileDepositProperties(file, location) }
  }

  override def load(depositId: DepositId): Try[DepositProperties] = {
    // @formatter:off
    (
      (sword2DepositsDir -> Location.SWORD2) #::
        (ingestFlowInbox -> Location.INGEST_FLOW) #::
        ingestFlowInboxArchived.map(_ -> Location.INGEST_FLOW_ARCHIVED).toStream
    )
    // @formatter:on
      .map { case (dir, location) => (dir / depositId) -> location }
      .collectFirst { case (deposit, location) if deposit.exists => Success(new FileDepositProperties(deposit, location)) }
      .getOrElse(Failure(DepositDoesNotExist(depositId)))
  }

  override def getSummaryReportData(depositor: Option[DepositorId],
                                    datamanager: Option[Datamanager],
                                    age: Option[Age],
                                   ): Try[SummaryReportData] = Try {
    SummaryReportData(
      listDeposits(sword2DepositsDir, Location.SWORD2)
        .append(listDeposits(ingestFlowInbox, Location.INGEST_FLOW))
        .append(ingestFlowInboxArchived.map(listDeposits(_, Location.INGEST_FLOW_ARCHIVED)).getOrElse(Stream.empty))
        .withFilter(_.depositIsReadable)
        .withFilter(_.hasDepositor(depositor))
        .withFilter(_.hasDatamanager(datamanager))
        .withFilter(_.isOlderThan(age))
        .flatMap(_.getStateLabel.unsafeGetOrThrow)
        .groupBy(identity)
        .mapValues(_.length)
    )
  }

  override def listReportData(depositor: Option[DepositorId],
                              datamanager: Option[Datamanager],
                              age: Option[Age],
                             ): Try[Stream[DepositInformation]] = Try {
    listDeposits(sword2DepositsDir, Location.SWORD2)
      .append(listDeposits(ingestFlowInbox, Location.INGEST_FLOW))
      .append(ingestFlowInboxArchived.map(listDeposits(_, Location.INGEST_FLOW_ARCHIVED)).getOrElse(Stream.empty))
      .withFilter(_.depositIsReadable)
      .withFilter(_.hasDepositor(depositor))
      .withFilter(_.hasDatamanager(datamanager))
      .withFilter(_.isOlderThan(age))
      .map(_.getDepositInformation.unsafeGetOrThrow)
      .sortBy(_.creationTimestamp)
  }

  override def getCurationParametersByDatasetId(datasetId: DatasetId): Try[(DepositId, Option[State])] = {
    listDeposits(ingestFlowInbox, Location.INGEST_FLOW)
      .map(props => props.validateUserCanReadTheDepositDirectoryAndTheDepositProperties().map(_ => props))
      .collectFirst {
        case f @ Failure(_) => f
        case s @ Success(props) if props.getFedoraIdentifier.fold(_ => false, _.contains(datasetId)) => s
      }
      .getOrElse(Failure(new IllegalArgumentException(s"No deposit found for datatsetId $datasetId")))
      .flatMap(props => {
        for {
          depositId <- props.getDepositId
          stateLabel <- props.getStateLabel
        } yield depositId -> stateLabel
      })
  }

  override def listDepositsToBeCleaned(filterOnDepositor: Option[DepositorId],
                                       filterOnAge: Age,
                                       filterOnState: State,
                                      ): Try[Stream[DepositProperties with FileSystemDeposit]] = Try {
    listDeposits(sword2DepositsDir, Location.SWORD2)
      .append(listDeposits(ingestFlowInbox, Location.INGEST_FLOW))
      .append(ingestFlowInboxArchived.map(listDeposits(_, Location.INGEST_FLOW_ARCHIVED)).getOrElse(Stream.empty))
      .withFilter(_.depositIsReadable)
      .withFilter(_.hasDepositor(filterOnDepositor))
      .withFilter(_.depositAgeIsLargerThanRequiredAge(filterOnAge))
      .withFilter(_.hasState(filterOnState))
      .map(identity)
  }
}
