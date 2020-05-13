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
import nl.knaw.dans.easy.managedeposit.properties.FileSystemDeposit.depositPropertiesFileName
import nl.knaw.dans.easy.managedeposit.{ Deposit, NotReadableException }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.io.FileUtils

import scala.util.{ Failure, Success, Try }

trait FileSystemDeposit extends DebugEnhancedLogging {

  val depositPath: Deposit
  val location: String

  def depositPropertiesFilePath: File = depositPath / depositPropertiesFileName

  /**
   * List the files within the deposit directory (non-recursively)
   */
  def listDepositFiles: Iterator[File] = {
    depositPath.list
  }

  def getNumberOfContinuedDeposits: Try[Int] = Try {
    if (depositPath.exists)
      depositPath.list.count(_.name.matches("""^.*\.zip\.\d+$"""))
    else 0
  }

  def getDepositSize: Try[Long] = Try {
    FileUtils.sizeOfDirectory(depositPath.toJava)
  }

  def retrieveBagNameFromFilesystem: Option[String] = {
    depositPath.list.collect {
      case child if child.isDirectory => child.name
    }.toList match {
      case child :: Nil => Option(child)
      case _ => Option.empty
    }
  }

  def validateFilesInDepositDirectoryAreReadable(): Try[Unit] = {
    depositPath.list
      .map(file => validateFileIsReadable(file.path.toRealPath()))
      .collectFirst { case f @ Failure(_: Exception) => f }
      .getOrElse(Success(()))
  }

  def validateFileIsReadable(file: File): Try[Unit] = Try {
    if (!file.exists || !file.isReadable)
      throw NotReadableException(file)
  }
  
  def validateDepositIsReadable(): Try[Unit] = validateFileIsReadable(depositPath)

  /**
   * Returns whether the deposit is valid, also logs a warn if it is not.
   *
   * @return Boolean if the deposit is readable and contains the expected deposit.properties file
   */
  def depositIsReadable: Boolean = {
    validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      .doIfFailure { case t: Throwable => logger.warn(s"[${ depositPath.name }] was invalid: ${ t.getMessage }") }
      .isSuccess
  }

  def validateUserCanReadTheDepositDirectoryAndTheDepositProperties(): Try[Unit] = {
    for {
      _ <- validateDepositIsReadable()
      _ <- validateFileIsReadable(depositPropertiesFilePath)
    } yield ()
  }

  def deleteDeposit(): Unit = depositPath.delete()

  def deleteFile(file: File): Unit = {
    if (file.path.startsWith(depositPath.path))
      file.delete()
    else
      throw new IllegalArgumentException(s"path $file is not part of deposit ${ depositPath.name } at $depositPath")
  }
}

object FileSystemDeposit {
  val depositPropertiesFileName = "deposit.properties"
}
