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

import nl.knaw.dans.easy.managedeposit.State._
import nl.knaw.dans.easy.managedeposit.fedora.Fedora
import nl.knaw.dans.easy.managedeposit.fedora.FedoraState._
import nl.knaw.dans.easy.managedeposit.fixture.TestSupportFixture
import nl.knaw.dans.easy.managedeposit.properties.{ DepositProperties, DepositPropertiesRepository }
import nl.knaw.dans.easy.managedeposit.{ DatasetId, DepositId, State }
import org.scalamock.scalatest.MockFactory
import org.scalatest.Inside

import scala.util.{ Failure, Success, Try }

class CurationSpec extends TestSupportFixture with Inside with MockFactory {

  private val fedora: Fedora = mock[Fedora]
  private val depositPropertiesFactory: DepositPropertiesRepository = mock[DepositPropertiesRepository]
  private val landingsPageBaseUrl: URI = new URI("http://deasy.dans.knaw.nl/ui/datasets/id/")
  private val curation: Curation = new Curation(fedora, landingsPageBaseUrl, depositPropertiesFactory)
  private val depositId: DepositId = "aba410b6-1a55-40b2-9ebe-6122aad00285"
  private val datasetId: DatasetId = "easy-dataset:12345"

  "fetchAmdAndExtractState" should "return Published if fedora returns published" in {
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Success(PUBLISHED)
    curation.fetchAmdAndExtractState(depositId, datasetId) shouldBe Success(PUBLISHED)
  }

  it should "fail if a unrecognized state is retrieved from fedora" in {
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Failure(new IllegalArgumentException("No valid state was found"))
    curation.fetchAmdAndExtractState(depositId, datasetId) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  it should "fail if no state is retrieved from fedora" in {
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Failure(new IllegalArgumentException("No valid state was found"))
    curation.fetchAmdAndExtractState(depositId, datasetId) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  "syncFedoraState" should "should update the state.label=FEDORA_ARCHIVED, state.description=<landingPage> and curation.performed properties if the current state.label is IN_REVIEW" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Try(PUBLISHED)
    props.setCurationParameters _ expects(true, FEDORA_ARCHIVED, s"$landingsPageBaseUrl$datasetId") returning Success(())

    curation.syncFedoraState(datasetId).success.value shouldBe s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $FEDORA_ARCHIVED"
  }

  it should "should update the state.label=REJECTED, state.description and curation.performed properties if the current state.label is IN_REVIEW and fedora.state=DELETED" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Try(DELETED)
    props.setCurationParameters _ expects(false, REJECTED, Curation.requestChangesDescription) returning Success(())

    curation.syncFedoraState(datasetId).success.value shouldBe s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has been successfully curated, state shifted from $IN_REVIEW to $REJECTED"
  }

  it should "not do anything if a fedoraState is returned that is not published or deleted" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning Try(MAINTENANCE)
    props.setCurationParameters _ expects(*, *, *) never()

    curation.syncFedoraState(datasetId).success.value shouldBe s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has state $MAINTENANCE, no action required"
  }

  it should "fail if the dataset is not known at fedora" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(false)
    (fedora.getFedoraState(_: DatasetId)) expects * never()
    props.setCurationParameters _ expects(*, *, *) never()

    inside(curation.syncFedoraState(datasetId)) {
      case Failure(e: IllegalArgumentException) =>
        e.getMessage shouldBe s"dataset $datasetId was not found in Fedora"
    }
  }

  it should "not do anything if the http-request to fedora fails" in {
    val props = mock[DepositProperties]
    val failure = Failure(new RuntimeException("Connection Refused"))
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning failure
    (fedora.getFedoraState(_: DatasetId)) expects * never()
    props.setCurationParameters _ expects(*, *, *) never()

    curation.syncFedoraState(datasetId) shouldBe failure
  }

  it should "not do anything if no fedoraState is returned" in {
    val props = mock[DepositProperties]
    val failure = Failure(new RuntimeException("Connection Refused"))
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(IN_REVIEW))
    (fedora.datasetIdExists(_: DatasetId)) expects datasetId returning Success(true)
    (fedora.getFedoraState(_: DatasetId)) expects datasetId returning failure
    props.setCurationParameters _ expects(*, *, *) never()

    curation.syncFedoraState(datasetId) shouldBe failure
  }

  it should "not do anything if the state.label is not IN_REVIEW" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> Some(State.SUBMITTED))
    (fedora.datasetIdExists(_: DatasetId)) expects * never()
    (fedora.getFedoraState(_: DatasetId)) expects * never()
    props.setCurationParameters _ expects(*, *, *) never()

    curation.syncFedoraState(datasetId).success.value shouldBe s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId has state ${ State.SUBMITTED }, no action required"
  }

  it should "not do anything if there is no state" in {
    val props = mock[DepositProperties]
    depositPropertiesFactory.findByDatasetId _ expects datasetId returning Success(props)
    props.getCurationParameters _ expects() returning Success(depositId -> None)
    (fedora.datasetIdExists(_: DatasetId)) expects * never()
    (fedora.getFedoraState(_: DatasetId)) expects * never()
    props.setCurationParameters _ expects(*, *, *) never()

    curation.syncFedoraState(datasetId).success.value shouldBe s"[aba410b6-1a55-40b2-9ebe-6122aad00285] deposit with datasetId $datasetId doesn't have a state set, could not take any action"
  }
}
