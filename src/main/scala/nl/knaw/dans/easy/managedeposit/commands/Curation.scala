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
package nl.knaw.dans.easy.managedeposit.commands

import java.net.URI

import com.yourmediashelf.fedora.client.FedoraClient
import nl.knaw.dans.easy.managedeposit.Command.FeedBackMessage
import nl.knaw.dans.easy.managedeposit.State.{ FEDORA_ARCHIVED, IN_REVIEW, REJECTED }
import nl.knaw.dans.easy.managedeposit.fedora.FedoraState.FedoraState
import nl.knaw.dans.easy.managedeposit.fedora.{ Fedora, FedoraState }
import nl.knaw.dans.easy.managedeposit.properties.{ DepositProperties, DepositPropertiesRepository }
import nl.knaw.dans.easy.managedeposit.{ Configuration, DatasetId, DepositId, State }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Try }

class Curation(fedora: Fedora,
               landingPageBaseUrl: URI,
               depositPropertiesFactory: DepositPropertiesRepository,
              ) extends DebugEnhancedLogging {

  def syncFedoraState(datasetId: DatasetId): Try[FeedBackMessage] = {
    for {
      props <- depositPropertiesFactory.findByDatasetId(datasetId)
      depositId <- props.getDepositId
      curationMessage <- curate(props, depositId, datasetId)
    } yield curationMessage
  }

  private def curate(props: DepositProperties, depositId: DepositId, datasetId: DatasetId): Try[FeedBackMessage] = {
    props.getStateLabel.flatMap {
      case Some(State.IN_REVIEW) => getFedoraStateAndUpdateProperties(props, depositId, datasetId)
      case Some(state) => logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has state $state, no action required")
      case None => logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId doesn't have a state set, could not take any action")
    }
  }

  private def getFedoraStateAndUpdateProperties(props: DepositProperties, depositId: DepositId, datasetId: DatasetId): Try[FeedBackMessage] = {
    fetchAmdAndExtractState(depositId, datasetId).flatMap {
      case FedoraState.PUBLISHED => markDepositPublished(props, depositId, datasetId)
      case FedoraState.DELETED => markDepositDeleted(props, depositId, datasetId)
      case fedoraState => logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has state $fedoraState, no action required")
    }
  }

  private def markDepositPublished(props: DepositProperties, depositId: DepositId, datasetId: DatasetId): Try[FeedBackMessage] = {
    for {
      _ <- props.setCurationParameters(
        dansDoiRegistered = true,
        newState = State.FEDORA_ARCHIVED,
        newDescription = s"${ landingPageBaseUrl.resolve(s"./$datasetId") }",
      )
      msg <- logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $FEDORA_ARCHIVED")
    } yield msg
  }

  private def markDepositDeleted(props: DepositProperties, depositId: DepositId, datasetId: DatasetId): Try[FeedBackMessage] = {
    for {
      _ <- props.setCurationParameters(
        dansDoiRegistered = false,
        newState = State.REJECTED,
        newDescription = Curation.requestChangesDescription,
      )
      msg <- logAndReturnMessage(s"[$depositId] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $REJECTED")
    } yield msg
  }

  private[commands] def fetchAmdAndExtractState(depositId: DepositId, datasetId: DatasetId): Try[FedoraState] = {
    logger.info(s"[$depositId] retrieving fedora AMD for $datasetId")
    fedora.datasetIdExists(datasetId)
      .flatMap {
        case false =>
          logger.warn(s"[$depositId] dataset $datasetId was not found in Fedora")
          Failure(new IllegalArgumentException(s"dataset $datasetId was not found in Fedora"))
        case true => fedora.getFedoraState(datasetId)
      }
  }

  private def logAndReturnMessage(msg: String): Try[FeedBackMessage] = Try {
    logger.info(msg)
    msg
  }
}
object Curation {
  val requestChangesDescription = "The DANS data-manager requests changes on this deposit"

  def apply(configuration: Configuration): Curation = {
    new Curation(
      fedora = new Fedora(new FedoraClient(configuration.fedoraCredentials)),
      landingPageBaseUrl = configuration.landingPageBaseUrl,
      depositPropertiesFactory = configuration.depositPropertiesFactory,
    )
  }
}
