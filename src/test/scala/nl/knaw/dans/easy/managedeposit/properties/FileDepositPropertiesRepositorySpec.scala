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
import nl.knaw.dans.easy.managedeposit.State
import nl.knaw.dans.easy.managedeposit.fixture.{ FileSystemTestDataFixture, TestSupportFixture }
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData

import scala.util.Success

class FileDepositPropertiesRepositorySpec extends TestSupportFixture with FileSystemTestDataFixture {

  private val sword2Inbox: File = depositDir / "sword2-inbox"
  private val ingestFlowInbox: File = depositDir / "ingest-flow-inbox"
  private val ingestFlowArchivedInbox: File = depositDir / "ingest-flow-archived-inbox"
  
  private implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")
  private val repo = new FileDepositPropertiesRepository(sword2Inbox, ingestFlowInbox, Option(ingestFlowArchivedInbox))

  override def beforeEach(): Unit = {
    super.beforeEach()

    sword2Inbox.createDirectoryIfNotExists()
    ingestFlowInbox.createDirectoryIfNotExists()
    ingestFlowArchivedInbox.createDirectoryIfNotExists()

    ruimteReis01.moveToDirectory(ingestFlowArchivedInbox)
    ruimteReis02.moveToDirectory(ingestFlowInbox)
    ruimteReis03.moveToDirectory(ingestFlowInbox)
    ruimteReis04.moveToDirectory(sword2Inbox)
    ruimteReis05.moveToDirectory(sword2Inbox)
  }

  "load" should "find a deposit in ingest-flow-inbox" in {
    inside(repo.load(ruimteReis03.name)) {
      case Success(props: FileDepositProperties) =>
        props.depositPath shouldBe ingestFlowInbox / ruimteReis03.name
        props.location shouldBe "INGEST_FLOW"
    }
  }
  
  it should "find a deposit in sword2-inbox" in {
    inside(repo.load(ruimteReis04.name)) {
      case Success(props: FileDepositProperties) =>
        props.depositPath shouldBe sword2Inbox / ruimteReis04.name
        props.location shouldBe "SWORD2"
    }
  }

  it should "find a deposit in ingest-flow-archived-inbox" in {
    inside(repo.load(ruimteReis01.name)) {
      case Success(props: FileDepositProperties) =>
        props.depositPath shouldBe ingestFlowArchivedInbox / ruimteReis01.name
        props.location shouldBe "INGEST_FLOW_ARCHIVED"
    }
  }
  
  it should "fail when the deposit does not exist" in {
    repo.load("no-such-deposit").failure.exception shouldBe DepositDoesNotExist("no-such-deposit")
  }
  
  "getSummaryReportData" should "yield the number of datasets per state label" in {
    repo.getSummaryReportData(Option.empty, Option.empty, Option.empty).success.value shouldBe SummaryReportData(
      5,
      Map(
        State.SUBMITTED -> 4,
        State.REJECTED -> 1,
      )
    )
  }
  
  it should "only yield the numbers for the deposits from user001" in {
    repo.getSummaryReportData(Option("user001"), Option.empty, Option.empty).success.value shouldBe SummaryReportData(
      4,
      Map(
        State.SUBMITTED -> 3,
        State.REJECTED -> 1,
      )
    )
  }

  it should "only yield the numbers for the deposits with curator easyadmin" in {
    repo.getSummaryReportData(Option.empty, Option("easyadmin"), Option.empty).success.value shouldBe SummaryReportData(
      3,
      Map(
        State.SUBMITTED -> 3,
      )
    )
  }
  
  // TODO more tests need to be written
}
