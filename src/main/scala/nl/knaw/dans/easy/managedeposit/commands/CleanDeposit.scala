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

import nl.knaw.dans.easy.managedeposit.Command.FeedBackMessage
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.FileSystemDeposit.depositPropertiesFileName
import nl.knaw.dans.easy.managedeposit.properties.{ DepositProperties, DepositPropertiesRepository, FileSystemDeposit }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Success, Try }

class CleanDeposit(depositPropertiesFactory: DepositPropertiesRepository)
                  (implicit dansDoiPrefixes: List[String], printStream: PrintStream) extends DebugEnhancedLogging {

  def cleanDeposits(deleteParams: DeleteParameters): Try[FeedBackMessage] = {
    for {
      depositsToBeDeleted <- depositPropertiesFactory.listDepositsToBeCleaned(deleteParams.filterOnDepositor, deleteParams.age, deleteParams.state)
      deletedDepositInfos = depositsToBeDeleted.flatMap(deleteDeposit(_, deleteParams).unsafeGetOrThrow)
    } yield {
      if (deleteParams.output || !deleteParams.doUpdate)
        ReportGenerator.outputDeletedDeposits(deletedDepositInfos)
      "Execution of clean: success"
    }
  }

  private def deleteDeposit(deposit: DepositProperties with FileSystemDeposit, deleteParams: DeleteParameters): Try[Option[DepositInformation]] = {
    def deleteOnlyDataFromDeposit(depositorId: DepositorId, depositState: State): Try[Boolean] = {
      Try {
        var filesToDelete = false
        for (file <- deposit.listDepositFiles
             if file.name != depositPropertiesFileName) {
          filesToDelete = true
          deposit.validateFileIsReadable(file)
            .doIfSuccess(_ => {
              if (deleteParams.doUpdate) {
                logger.info(s"DELETE data from deposit for $depositorId from $depositState $deposit")
                deposit.deleteFile(file)
              }
            })
            .unsafeGetOrThrow
        }
        filesToDelete
      } flatMap {
        case true if deleteParams.doUpdate =>
          deleteParams.newState
            .map((deposit.setState _).tupled)
            .getOrElse(Success(()))
            .map(_ => true)
        case b => Success(b)
      }
    }

    def deleteDepositAndProperties(depositorId: DepositorId, depositState: State): Try[Boolean] = {
      for {
        _ <- deposit.validateDepositIsReadable()
        _ <- deposit.deleteDepositProperties()
        _ = if (deleteParams.doUpdate) {
          logger.info(s"DELETE deposit for $depositorId from $depositState $deposit")
          deposit.deleteDeposit()
        }
      } yield true
    }

    for {
      depositInfo <- deposit.getDepositInformation
      depositorId = depositInfo.depositor
      stateLabel = depositInfo.state.getOrElse(State.UNKNOWN)
      deletableFiles <- if (deleteParams.onlyData) deleteOnlyDataFromDeposit(depositorId, stateLabel)
                        else deleteDepositAndProperties(depositorId, stateLabel)
    } yield if (deletableFiles) Some(depositInfo)
            else None
  }
}
object CleanDeposit {
  def apply(configuration: Configuration)(implicit printStream: PrintStream): CleanDeposit = {
    implicit val dansDoiPrefixes: List[String] = configuration.dansDoiPrefixes
    new CleanDeposit(configuration.depositPropertiesFactory)
  }
}
