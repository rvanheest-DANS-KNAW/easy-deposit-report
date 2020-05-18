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

import java.io.{ ByteArrayOutputStream, PrintStream }
import java.text.SimpleDateFormat
import java.util.{ Calendar, UUID }

import nl.knaw.dans.easy.managedeposit.Location.Location
import nl.knaw.dans.easy.managedeposit.ReportGeneratorSpec.ReportType
import nl.knaw.dans.easy.managedeposit.ReportGeneratorSpec.ReportType.ReportType
import nl.knaw.dans.easy.managedeposit.State._
import nl.knaw.dans.easy.managedeposit.commands.Curation
import nl.knaw.dans.easy.managedeposit.fixture.TestSupportFixture
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData
import org.joda.time.DateTime
import org.scalatest.Inspectors

import scala.language.postfixOps

class ReportGeneratorSpec extends TestSupportFixture with Inspectors {

  private implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")

  "output Summary" should "should contain all deposits" in {
    val summarydata = getSummaryReportData(createDeposits)
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(summarydata)(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :         16
         |
         |Per state:
         |----------
         |ARCHIVED        :     2
         |DRAFT           :     1
         |FEDORA_ARCHIVED :     1
         |FINALIZING      :     1
         |IN_REVIEW       :     1
         |INVALID         :     1
         |REJECTED        :     1
         |SUBMITTED       :     4
         |UNKNOWN         :     4""".stripMargin
  }

  it should "produce a report when no deposits are found" in {
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(getSummaryReportData(Stream.empty))(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :          0
         |
         |Per state:
         |----------""".stripMargin
  }

  it should "align the data per state based on the longest state" in {
    val deposits = getSummaryReportData(createDeposits.filterNot(_.state.exists(FEDORA_ARCHIVED ==)))
    val now = Calendar.getInstance().getTime
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val currentTime = format.format(now)
    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true)
    try {
      ReportGenerator.outputSummary(deposits)(ps)
    } finally {
      ps.close()
    }

    baos.toString.trim shouldBe
      s"""Grand totals:
         |-------------
         |Timestamp          : $currentTime
         |Number of deposits :         15
         |
         |Per state:
         |----------
         |ARCHIVED   :     2
         |DRAFT      :     1
         |FINALIZING :     1
         |IN_REVIEW  :     1
         |INVALID    :     1
         |REJECTED   :     1
         |SUBMITTED  :     4
         |UNKNOWN    :     4""".stripMargin
  }

  "outputErrorReport" should "only print the deposits containing an error" in {
    val baos = new ByteArrayOutputStream()
    val errorDeposit = createDeposit("dans-0", ARCHIVED, Location.INGEST_FLOW).copy(dansDoiRegistered = Some(false)) //violates the rule ARCHIVED must be registered when DANS doi
    val noDansDoiDeposit = createDeposit("dans-1", ARCHIVED, Location.INGEST_FLOW).copy(dansDoiRegistered = Some(false), doiIdentifier = Some("11.11111/other-doi-123"))
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = Stream(
      errorDeposit,
      noDansDoiDeposit, //does not violate any rule
      createDeposit("dans-2", SUBMITTED, Location.INGEST_FLOW), //does not violate any rule
      createDeposit("dans-3", SUBMITTED, Location.INGEST_FLOW), //does not violate any rule
    )
    outputReportManged(ps, deposits, ReportType.ERROR)
    val errorReport = baos.toString
    errorReport should include(createCsvRow(errorDeposit)) // only the first deposit should be added to the report
    errorReport should not include createCsvRow(noDansDoiDeposit)
  }

  it should "not print any csv rows if no deposits violate the rules" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = Stream(
      createDeposit("dans-0", DRAFT, Location.SWORD2).copy(dansDoiRegistered = Some(false)),
      createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW),
      createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW),
    )
    outputReportManged(ps, deposits, ReportType.ERROR)

    val errorReport = baos.toString
    deposits.foreach(deposit => errorReport should not include createCsvRow(deposit)) // None of the deposits should be added to the report
  }

  it should "print any deposit that has one of the states null, UNKNOWN, INVALID, FAILED, REJECTED or ARCHIVED + not-registered" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = Stream(
      createDeposit("dans-0", ARCHIVED, Location.INGEST_FLOW).copy(dansDoiRegistered = Some(false)), //violates the rule ARCHIVED must be registered
      createDeposit("dans-1", FAILED, Location.INGEST_FLOW),
      createDeposit("dans-2", REJECTED, Location.INGEST_FLOW),
      createDeposit("dans-3", INVALID, Location.SWORD2),
      createDeposit("dans-4", UNKNOWN, Location.INGEST_FLOW),
      createDeposit("dans-5", null, Location.INGEST_FLOW),
    )
    outputReportManged(ps, deposits, ReportType.ERROR)

    val errorReport = baos.toString
    forEvery(deposits)(deposit => errorReport should include(createCsvRow(deposit))) //all deposits should be added to the report
  }

  it should "leave out deposits originating from easy-deposit-api that are rejected by a datamanager after review" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val depositsInReport = Stream(
      createDeposit("dans-0", ARCHIVED, Location.INGEST_FLOW).copy(dansDoiRegistered = Some(false)), //violates the rule ARCHIVED must be registered
      createDeposit("dans-1", FAILED, Location.INGEST_FLOW),
      createDeposit("dans-2", REJECTED, Location.INGEST_FLOW),
      createDeposit("dans-3", INVALID, Location.SWORD2),
      createDeposit("dans-4", UNKNOWN, Location.INGEST_FLOW),
      createDeposit("dans-5", null, Location.INGEST_FLOW),
    )
    val depositsNotInReport = Stream(
      createDeposit("dans-rejected", REJECTED, Location.INGEST_FLOW).copy(origin = "API", description = Some(Curation.requestChangesDescription)),
      createDeposit("dans-abandoned-draft", INVALID, Location.INGEST_FLOW).copy(origin = "SWORD2", description = Some("abandoned draft, data removed")),
    )
    outputReportManged(ps, depositsInReport #::: depositsNotInReport, ReportType.ERROR)

    val errorReport = baos.toString
    errorReport.linesIterator.toList should have length depositsInReport.size + 1 // 1x header + |depositsInReport|
    forEvery(depositsInReport)(deposit => errorReport should include(createCsvRow(deposit)))
    forEvery(depositsNotInReport)(deposit => errorReport should not include createCsvRow(deposit))
  }

  "outputFullReport" should "print all deposits" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val deposits = createDeposits
    outputReportManged(ps, deposits, ReportType.FULL)
    val fullReport = baos.toString
    forEvery(deposits)(deposit => fullReport should include(createCsvRow(deposit)))
  }

  "outputRawReport" should "not print anything on an empty table" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq.empty
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe empty
  }

  it should "only print the headers when only one row is provided" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq(
      Seq("a", "b", "c", "d"),
    )
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe
      """a,b,c,d
        |""".stripMargin
  }

  it should "only print the report when both headers and data are provided" in {
    val baos = new ByteArrayOutputStream()
    val ps: PrintStream = new PrintStream(baos, true)
    val table: Seq[Seq[String]] = Seq(
      Seq("a", "b", "c", "d"),
      Seq("1", "2", "3", "n/a"),
      Seq("4", "n/a", "5", "n/a"),
      Seq("n/a", "6", "7", "8"),
    )
    ReportGenerator.outputRawReport(table)(ps)
    val report = baos.toString()
    report shouldBe
      """a,b,c,d
        |1,2,3,n/a
        |4,n/a,5,n/a
        |n/a,6,7,8
        |""".stripMargin
  }

  private def outputReportManged(ps: PrintStream, deposits: Stream[DepositInformation], reportType: ReportType): Unit = {
    try {
      reportType match {
        case ReportType.ERROR => ReportGenerator.outputErrorReport(deposits)(ps)
        case ReportType.FULL => ReportGenerator.outputFullReport(deposits)(ps)
      }
    } finally {
      ps.close()
    }
  }

  private def createCsvRow(deposit: DepositInformation): String = {
    s"${ deposit.depositor }," +
      s"${ deposit.depositId }," +
      s"${ deposit.bagDirName }," +
      s"${ deposit.state.getOrElse(UNKNOWN) }," +
      s"${ deposit.origin }," +
      s"${ deposit.location }," +
      s"${ deposit.doiIdentifier.getOrElse("n/a") }," +
      s"${ deposit.registeredString }," +
      s"${ deposit.fedoraIdentifier.getOrElse("n/a") }," +
      s"${ deposit.datamanager.getOrElse("n/a") }," +
      s"${ deposit.creationTimestamp }," +
      s"${ deposit.lastModified }," +
      s"${ deposit.description.getOrElse("n/a") }"
  }

  private def createDeposit(depositorId: String, state: State, location: Location): DepositInformation = {
    DepositInformation(
      depositId = UUID.randomUUID().toString,
      depositor = depositorId,
      datamanager = Some("my-datamanager"),
      dansDoiRegistered = Some(true),
      doiIdentifier = Some("10.17026/dans-12345"),
      fedoraIdentifier = Some("FedoraId"),
      state = Option(state),
      description = Some("description"),
      creationTimestamp = DateTime.now().minusDays(3).toString(),
      lastModified = "",
      origin = "SWORD2",
      location = location,
      bagDirName = "baggy",
    )
  }
  
  private def getSummaryReportData(deposits: Stream[DepositInformation]): SummaryReportData = {
    SummaryReportData(
      deposits.length,
      deposits
        .map(_.state.getOrElse(State.UNKNOWN))
        .groupBy(identity)
        .map { case (state, dps) => state -> dps.length }
    )
  }

  private def createDeposits = Stream(
    createDeposit("dans-1", ARCHIVED, Location.INGEST_FLOW),
    createDeposit("dans-1", ARCHIVED, Location.INGEST_FLOW),
    createDeposit("dans-1", DRAFT, Location.SWORD2),
    createDeposit("dans-1", FINALIZING, Location.SWORD2),
    createDeposit("dans-1", INVALID, Location.INGEST_FLOW),
    createDeposit("dans-1", REJECTED, Location.INGEST_FLOW),
    createDeposit("dans-1", IN_REVIEW, Location.INGEST_FLOW),
    createDeposit("dans-1", FEDORA_ARCHIVED, Location.INGEST_FLOW),
    createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW),
    createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW),
    createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW),
    createDeposit("dans-1", SUBMITTED, Location.INGEST_FLOW), // duplicate deposits are allowed
    createDeposit("dans-1", UNKNOWN, Location.SWORD2),
    createDeposit("dans-1", UNKNOWN, Location.INGEST_FLOW),
    createDeposit("dans-1", null, Location.INGEST_FLOW), // mapped and added to unknown
    createDeposit("dans-1", null, Location.INGEST_FLOW),
  )
}
object ReportGeneratorSpec {
  object ReportType extends Enumeration {
    type ReportType = Value
    val FULL: ReportType = Value("FULL")
    val ERROR: ReportType = Value("EROOR")
  }
}
