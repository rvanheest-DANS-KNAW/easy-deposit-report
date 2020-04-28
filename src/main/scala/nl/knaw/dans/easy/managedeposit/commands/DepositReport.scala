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

import java.io.PrintStream

import better.files.File
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.{ DepositPropertiesRepository, FileDepositProperties }

import scala.util.Try

class DepositReport(depositPropertiesFactory: DepositPropertiesRepository)
                   (implicit printStream: PrintStream) {

  def summary(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = {
    for {
      depositInfos <- depositPropertiesFactory.listReportData(depositor, datamanager, age)
      _ <- ReportGenerator.outputSummary(depositInfos)
    } yield "End of summary report."
  }

  def createFullReport(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = {
    for {
      depositInfos <- depositPropertiesFactory.listReportData(depositor, datamanager, age)
      _ <- ReportGenerator.outputFullReport(depositInfos)
    } yield "End of full report."
  }

  def createErrorReport(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[String] = {
    for {
      depositInfos <- depositPropertiesFactory.listReportData(depositor, datamanager, age)
      _ <- ReportGenerator.outputErrorReport(depositInfos)
    } yield "End of error report."
  }

  def createRawReport(location: File): Try[String] = {
    ReportGenerator.outputRawReport {
      makeCompleteTable {
        location.list
          .toStream
          .collect { case file if file.isDirectory => new FileDepositProperties(file, "").properties }
      }
    } map (_ => "End of raw report.")
  }

  /**
   * Given a sequence of maps of key-value pairs, construct a table that has values for every key in every map.
   *
   * @example
   * {{{
   *    [
   *      { "a" -> "1", "b" -> "2", "c" -> "3" },
   *      { "a" -> "4", "c" -> "5" },
   *      { "b" -> "6", "c" -> "7", "d" -> "8" },
   *    ]
   *
   *    should result in
   *
   *    [
   *      [ "a",   "b",   "c", "d"   ],
   *      [ "1",   "2",   "3", "n/a" ],
   *      [ "4",   "n/a", "5", "n/a" ],
   *      [ "n/a", "6",   "7", "8"   ],
   *    ]
   * }}}
   * @param input        the sequence of maps to be made into a complete table
   * @param defaultValue the value (lazily evaluated) to be used for values that are not available (defaults to `"n/a"`)
   * @return the completed table
   */
  private def makeCompleteTable(input: Stream[Map[String, String]], defaultValue: => String = "n/a"): Stream[Seq[String]] = {
    val keys: List[String] = input.flatMap(_.keys.toSeq).toSet.toList

    keys +: input.map(m => keys.map(m.getOrElse(_, defaultValue)))
  }
}

object DepositReport {
  def apply(configuration: Configuration)(implicit printStream: PrintStream): DepositReport = {
    new DepositReport(configuration.depositPropertiesFactory)
  }
}
