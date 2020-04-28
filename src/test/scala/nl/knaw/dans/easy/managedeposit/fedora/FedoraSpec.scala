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
package nl.knaw.dans.easy.managedeposit.fedora

import com.yourmediashelf.fedora.client.{ FedoraClient, FedoraCredentials }
import nl.knaw.dans.easy.managedeposit.fedora.FedoraState.{ FedoraState, PUBLISHED, SUBMITTED }
import nl.knaw.dans.easy.managedeposit.fixture.TestSupportFixture

import scala.util.{ Failure, Success }
import scala.xml.Elem

class FedoraSpec extends TestSupportFixture {

  private val creds = new FedoraCredentials("http:///localhost:8080", "fed", "fed")
  private val client = new FedoraClient(creds)
  private val fedora = new Fedora(client)

  "extractState" should "return the submitted state" in {
    val elem = createXMLByteArrayForFedoraState(SUBMITTED)
    fedora.extractState(elem) shouldBe Success(SUBMITTED)
  }

  it should "return the PUBLISHED state" in {
    val elem = createXMLByteArrayForFedoraState(PUBLISHED)
    fedora.extractState(elem) shouldBe Success(PUBLISHED)
  }

  it should "accept a input stream and return the proper state" in {
    val elem = createXMLByteArrayForFedoraState(PUBLISHED)
    fedora.extractState(elem) shouldBe Success(PUBLISHED)
  }

  "extractState(elem: Elem)" should "fail if no state is found in the xml" in {
    val elem = <damd:administrative-md version="0.1"><previousState>DRAFT</previousState></damd:administrative-md>
    fedora.extractState(elem) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  it should "fail a strange state is found in the xml" in {
    val elem = <damd:administrative-md version="0.1"><datasetState>NON_EXISTING</datasetState></damd:administrative-md>
    fedora.extractState(elem) should matchPattern {
      case Failure(iae: IllegalArgumentException) if iae.getMessage == "No valid state was found" =>
    }
  }

  private def createXMLByteArrayForFedoraState(fedoraState: FedoraState): Elem = {
    <damd:administrative-md version="0.1">
      <datasetState>{ fedoraState }</datasetState>
    </damd:administrative-md>
  }
}

