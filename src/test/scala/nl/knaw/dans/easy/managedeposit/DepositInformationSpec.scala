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
package nl.knaw.dans.easy.managedeposit

import nl.knaw.dans.easy.managedeposit.State.{ DRAFT, FAILED }
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DepositInformationSpec extends AnyFlatSpec with Matchers with OptionValues {

  private implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")

  private val deposit = DepositInformation(
    depositId = "DepositId",
    depositor = "123",
    datamanager = Some("my-datamanager"),
    dansDoiRegistered = Some(true),
    doiIdentifier = Some("10.17026/dans-12345"),
    fedoraIdentifier = Some("123"),
    state = Some(State.ARCHIVED), 
    description = Some("description"), 
    creationTimestamp = "2000-01-01", 
    lastModified = "2000-01-02", 
    origin = "ORIGIN-1", 
    location = "LOCATION-1", 
    bagDirName = "baggy",
  )

  "registeredString" should "return a yes when DANS DOI and DOI is registered" in {
    deposit.registeredString shouldBe "yes"
  }

  it should "return a no when DANS DOI and DOI is not registered" in {
    deposit.copy(dansDoiRegistered = Some(false)).registeredString shouldBe "no"
  }

  it should "return a yes when DANS DOI and DOI registration is not given and state is ARCHIVED" in {
    deposit.copy(dansDoiRegistered = None).registeredString shouldBe "yes"
  }

  it should "return a no when DANS DOI and DOI registration is not given and state is not ARCHIVED and not FAILED" in {
    deposit.copy(dansDoiRegistered = None, state = Some(DRAFT)).registeredString shouldBe "no"
  }

  it should "return UNKNOWN when DANS DOI and DOI registration is not given and state is FAILED" in {
    deposit.copy(dansDoiRegistered = None, state = Some(FAILED)).registeredString shouldBe "unknown"
  }

  it should "return a yes when NOT DANS DOI, also when dansDoiRegistered is false" in {
    deposit.copy(dansDoiRegistered = Some(false), doiIdentifier = Some("11.11111/other-123")).registeredString shouldBe "yes"
  }
}
