package nl.knaw.dans.easy.managedeposit.properties

import better.files.File
import nl.knaw.dans.easy.managedeposit.properties.FileSystemDeposit.depositPropertiesFileName
import nl.knaw.dans.easy.managedeposit.{ Deposit, NotReadableException }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging

import scala.util.{ Failure, Success, Try }

trait FileSystemDeposit extends DebugEnhancedLogging {

  protected val depositPath: Deposit

  def depositPropertiesFilePath: File = depositPath / depositPropertiesFileName

  /**
   * List the files within the deposit directory (non-recursively)
   */
  def listDepositFiles: Iterator[File] = {
    depositPath.list
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
