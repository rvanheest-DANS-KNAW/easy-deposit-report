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

import java.io.FileNotFoundException

import nl.knaw.dans.easy.managedeposit.NotReadableException
import nl.knaw.dans.easy.managedeposit.fixture.{ FileSystemTestDataFixture, TestSupportFixture }
import nl.knaw.dans.easy.managedeposit.{ DepositInformation, State }
import org.apache.commons.configuration.PropertiesConfiguration
import org.joda.time.DateTime
import org.scalatest.BeforeAndAfterEach

import scala.util.{ Failure, Success }

class FileDepositPropertiesSpec extends TestSupportFixture
  with FileSystemTestDataFixture
  with BeforeAndAfterEach {

  private implicit val dansDoiPrefixes: List[String] = Nil

  "properties" should "return the contents of deposit.properties as a key-value map, with an added property 'depositId' -> <deposit dir name>" in {
    val dp = new FileDepositProperties(depositOne, "SWORD2")
    val properties = dp.properties
    properties should contain only(
      "bag-store.bag-id" -> "aba410b6-1a55-40b2-9ebe-6122aad00285",
      "creation.timestamp" -> "2018-11-08T22:05:53.992Z",
      "state.description" -> "Deposit is valid and ready for post-submission processing",
      "state.label" -> "SUBMITTED",
      "depositor.userId" -> "user001",
      "identifier.doi" -> "my-doi",
      "identifier.dans-doi.registered" -> "yes",
      "identifier.fedora" -> "easy-dataset:12345",
      "curation.required" -> "true",
      "curation.performed" -> "false",
      "bag-store.bag-name" -> "baggy",
      "deposit.origin" -> "SWORD2",
      "depositId" -> depositOne.name,
      "curation.datamanager.email" -> "FILL.IN.YOUR@VALID-EMAIL.NL",
      "curation.datamanager.userId" -> "easyadmin",
    )
  }

  "getters" should "retrieve the values from deposit.properties if it exists" in {
    val dp = new FileDepositProperties(depositOne, "SWORD2")
    dp.getDepositId.success.value shouldBe "aba410b6-1a55-40b2-9ebe-6122aad00285"
    dp.getDepositorId.success.value.value shouldBe "user001"
    dp.getDatamanager.success.value.value shouldBe "easyadmin"
    dp.getCreationTime.success.value.value shouldBe new DateTime("2018-11-08T22:05:53.992Z")
    dp.getDansDoiRegistered.success.value.value shouldBe "yes"
    dp.getDoiIdentifier.success.value.value shouldBe "my-doi"
    dp.getFedoraIdentifier.success.value.value shouldBe "easy-dataset:12345"
    dp.getStateLabel.success.value.value shouldBe State.SUBMITTED
    dp.getStateDescription.success.value.value shouldBe "Deposit is valid and ready for post-submission processing"
    dp.getDepositOrigin.success.value.value shouldBe "SWORD2"
  }

  it should "return an empty properties file if there is not a deposit.propertiesFile available" in {
    val dp = new FileDepositProperties(depositDirWithoutProperties, "SWORD2")
    dp.getDepositId should matchPattern { case Failure(_: FileNotFoundException) => }
  }

  "hasDepositor" should "return true if the specified depositor is listed in the deposit.properties" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDepositor(Some("user001")) shouldBe true
  }

  it should "return true if no depositor is specified" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDepositor(None) shouldBe true
  }

  it should "return false if the specified depositor is different from the one listed in the deposit.properties" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDepositor(Some("another")) shouldBe false
  }

  it should "return true if the deposit.properties doesn't list a depositor" in {
    new FileDepositProperties(depositWithoutDepositor, "SWORD2").hasDepositor(Some("user001")) shouldBe true
  }

  "hasDatamanager" should "return true if the specified datamanager is listed in the deposit.properties" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDatamanager(Some("easyadmin")) shouldBe true
  }

  it should "return true if no datamanager is specified" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDatamanager(None) shouldBe true
  }

  it should "return false if the specified datamanager is different from the one listed in the deposit.properties" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").hasDatamanager(Some("another")) shouldBe false
  }

  it should "return true if the deposit.properties doesn't list a datamanager" in {
    new FileDepositProperties(depositWithoutDepositor, "SWORD2").hasDatamanager(Some("user001")) shouldBe true
  }

  "isOlderThan" should "return true if the given age is larger than the lastModifiedTimestamp" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").isOlderThan(Some(2)) shouldBe true
  }

  it should "return true if the given age is equal than the lastModifiedTimestamp" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").isOlderThan(Some(0)) shouldBe true
  }

  it should "return false if the given age is smaller than the lastModifiedTimestamp" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").isOlderThan(Some(-2)) shouldBe false
  }

  it should "return true if no age is given" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").isOlderThan(None) shouldBe true
  }

  it should "fail if the deposit.properties file doesn't exist" in {
    val dp = new FileDepositProperties(depositDirWithoutProperties, "SWORD2")

    a[NotReadableException] should be thrownBy dp.isOlderThan(Some(2))
  }

  "getNumberOfContinuedDeposits" should "count the number of zip parts in a deposit" in {
    new FileDepositProperties(depositOne, "SWORD2").getNumberOfContinuedDeposits.success.value shouldBe 2
  }

  it should "return 0 if no zip parts are present in the deposit" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").getNumberOfContinuedDeposits.success.value shouldBe 0
  }

  it should "return 0 if the deposit doesn't exist" in {
    new FileDepositProperties(depositDir / "non-existing-deposit", "SWORD2").getNumberOfContinuedDeposits.success.value shouldBe 0
  }

  "getBagDirName" should "return the bag name if it is listed in the deposit.properties" in {
    new FileDepositProperties(depositOne, "SWORD2").getBagDirName.success.value.value shouldBe "baggy"
  }

  it should "find the bag name on file system if it's not listed in the deposit.properties" in {
    new FileDepositProperties(ruimteReis02, "SWORD2").getBagDirName.success.value.value shouldBe "bag"
  }

  it should "give up and return None if no bag-name is listed in the deposit.properties and no directories are present" in {
    new FileDepositProperties(ruimteReis05, "SWORD2").getBagDirName.success.value shouldBe empty
  }

  "getDepositInformation" should "read the properties required for generating a report" in {
    new FileDepositProperties(depositOne, "INGEST_FLOW").getDepositInformation.success.value should matchPattern {
      case DepositInformation(
      "aba410b6-1a55-40b2-9ebe-6122aad00285",
      "user001",
      Some("easyadmin"),
      Some(true),
      Some("my-doi"),
      Some("easy-dataset:12345"),
      Some(State.SUBMITTED),
      Some("Deposit is valid and ready for post-submission processing"),
      _,
      _,
      "SWORD2",
      "INGEST_FLOW",
      "baggy",
      ) =>
    }
  }
  
  "setCurationParameters" should "store curation properties in deposit.properties" in {
    new FileDepositProperties(ruimteReis01, "INGEST_FLOW").setCurationParameters(dansDoiRegistered = true, State.FEDORA_ARCHIVED, "deposit is curated") shouldBe a[Success[_]]
    
    val propsAfter = new PropertiesConfiguration() {
      setDelimiterParsingDisabled(false)
      load((ruimteReis01 / "deposit.properties").toJava)
    }
    
    propsAfter.getString("identifier.dans-doi.registered") shouldBe "yes"
    propsAfter.getString("curation.performed") shouldBe "yes"
    propsAfter.getString("state.label") shouldBe "FEDORA_ARCHIVED"
    propsAfter.getString("state.description") shouldBe "deposit is curated"
  }
}
