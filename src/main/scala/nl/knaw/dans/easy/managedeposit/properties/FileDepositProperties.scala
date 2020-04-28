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

import better.files.File
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.FileDepositProperties._
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.{ BooleanUtils, StringUtils }
import org.joda.time.{ DateTime, DateTimeZone, Duration }

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

class FileDepositProperties(deposit: Deposit, val location: String) extends DepositProperties with DebugEnhancedLogging {
  private lazy val depositProperties: Try[PropertiesConfiguration] = findDepositProperties
  private lazy val lastModified: Option[DateTime] = getLastModifiedTimestamp.unsafeGetOrThrow

  private def depositPropertiesFilePath: File = deposit / depositPropertiesFileName

  private def findDepositProperties: Try[PropertiesConfiguration] = Try {
    if (depositPropertiesFilePath.exists) {
      debug(s"Getting info from $deposit")
      new PropertiesConfiguration() {
        setDelimiterParsingDisabled(true)
        setFile(depositPropertiesFilePath.toJava)
        load(depositPropertiesFilePath.toJava)
      }
    }
    else
      throw new FileNotFoundException(s"$depositPropertiesFileName does not exist for $deposit")
  }

  override def properties: Map[String, String] = {
    depositProperties
      .map(props => {
        props.getKeys.asScala
          .map(key => key -> props.getString(key))
          .toMap
      })
      .getOrElse(Map.empty) + ("depositId" -> deposit.name)
  }

  private def getProperty(key: String): Try[Option[String]] = {
    depositProperties.map(props => Option(props.getString(key)))
  }

  override def getDepositId: Try[String] = {
    getProperty(depositIdKey).map(_.getOrElse(deposit.name))
  }

  def hasDepositor(filterOnDepositor: Option[DepositorId]): Boolean = {
    getDepositorId.toOption.flatten.forall(depId => filterOnDepositor.forall(depId ==))
  }

  def getDepositorId: Try[Option[String]] = {
    getProperty("depositor.userId")
  }

  def hasDatamanager(filterOnDatamanager: Option[Datamanager]): Boolean = {
    getDatamanager.toOption.flatten.forall(dm => filterOnDatamanager.forall(dm ==))
  }

  def getDatamanager: Try[Option[String]] = {
    getProperty("curation.datamanager.userId")
  }

  def isOlderThan(filterOnAge: Option[Age]): Boolean = { // forall returns true for the empty set, see https://en.wikipedia.org/wiki/Vacuous_truth
    filterOnAge.forall(age => lastModified.forall(mod => Duration.millis(DateTime.now(mod.getZone).getMillis - mod.getMillis).getStandardDays <= age))
  }

  def depositAgeIsLargerThanRequiredAge(age: Age): Boolean = {
    getCreationTime
      .map {
        case Some(start) =>
          new Duration(start, end).getStandardDays > age
        case None =>
          logger.warn(s"deposit: $getDepositId does not have a creation time")
          false
      }
      .getOrElse(false)
  }

  private def end: DateTime = DateTime.now(DateTimeZone.UTC)

  def getCreationTime: Try[Option[DateTime]] = {
    getProperty("creation.timestamp").map(_.map(timeString => new DateTime(timeString)))
  }

  def getLastModifiedTimestamp: Try[Option[DateTime]] = {
    for {
      _ <- validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      _ <- validateFilesInDepositDirectoryAreReadable()
    } yield doGetLastModifiedStamp()
  }

  private def doGetLastModifiedStamp(): Option[DateTime] = {
    Try {
      deposit.list
        .map(_.lastModifiedTime.toEpochMilli)
        .max // if deposit.list is empty, max returns an UnsupportedOperationException
    }.fold(_ => Option.empty, Option(_))
      .map(new DateTime(_, DateTimeZone.UTC))
  }

  def getDansDoiRegistered: Try[Option[String]] = {
    getProperty(dansDoiRegisteredKey)
  }

  def getDoiIdentifier: Try[Option[String]] = {
    getProperty("identifier.doi")
  }

  def getFedoraIdentifier: Try[Option[String]] = {
    getProperty("identifier.fedora")
  }

  def getStateLabel: Try[Option[State]] = {
    getProperty(stateLabelKey).map(_.flatMap(State.toState))
  }

  def hasState(filterOnState: State): Boolean = {
    getStateLabel.toOption.flatten.exists(filterOnState ==)
  }

  def getStateDescription: Try[Option[String]] = {
    getProperty(stateDescriptionKey)
  }

  def setState(label: State, description: String): Try[Unit] = {
    for {
      dp <- depositProperties
      _ = dp.setProperty(stateLabelKey, label.toString)
      _ = dp.setProperty(stateDescriptionKey, description)
      _ = dp.save()
    } yield ()
  }

  def getDepositOrigin: Try[Option[String]] = {
    getProperty("deposit.origin")
  }

  /**
   * Returns whether the deposit is valid, also logs a warn if it is not.
   *
   * @return Boolean if the deposit is readable and contains the expected deposit.properties file
   */
  def isValidDeposit: Boolean = {
    validateUserCanReadTheDepositDirectoryAndTheDepositProperties()
      .doIfFailure { case t: Throwable => logger.warn(s"[${ deposit.name }] was invalid: ${ t.getMessage }") }
      .isSuccess
  }

  def validateUserCanReadTheDepositDirectoryAndTheDepositProperties(): Try[Unit] = {
    for {
      _ <- validateThatDepositDirectoryIsReadable()
      _ <- validateUserRightsForDepositDir()
      _ <- validateThatDepositPropertiesIsReadable()
      _ <- validateUserRightsForPropertiesFile()
    } yield ()
  }

  private def validateThatDepositDirectoryIsReadable(): Try[Unit] = {
    validateThatFileIsReadable(deposit)
  }

  private def validateThatDepositPropertiesIsReadable(): Try[Unit] = {
    validateThatFileIsReadable(depositPropertiesFilePath)
  }

  private def validateFilesInDepositDirectoryAreReadable(): Try[Unit] = {
    deposit.list
      .map(file => validateThatFileIsReadable(file.path.toRealPath()))
      .collectFirst { case f @ Failure(_: Exception) => f }
      .getOrElse(Success(()))
  }

  private def validateThatFileIsReadable(file: File): Try[Unit] = Try {
    if (!file.isReadable)
      throw NotReadableException(file)
  }

  private def validateUserRightsForDepositDir(): Try[Unit] = {
    validateUserRightsForFile(deposit)
  }

  private def validateUserRightsForPropertiesFile(): Try[Unit] = {
    validateUserRightsForFile(depositPropertiesFilePath)
  }

  private def validateUserRightsForFile(file: File): Try[Unit] = Try {
    if (file.exists && !file.isReadable)
      throw NotReadableException(file)
  }

  def getNumberOfContinuedDeposits: Try[Int] = Try {
    if (deposit.exists)
      deposit.list.count(_.name.toString.matches("""^.*\.zip\.\d+$"""))
    else 0
  }

  def getBagDirName: Try[Option[String]] = {
    getProperty("bag-store.bag-name").map(_.orElse(retrieveBagNameFromFilesystem))
  }

  private def retrieveBagNameFromFilesystem: Option[String] = {
    deposit.list.collect {
      case child if child.isDirectory => child.name
    }.toList match {
      case child :: Nil => Option(child)
      case _ => Option.empty
    }
  }

  def getDepositInformation(implicit dansDoiPrefixes: List[String]): Try[DepositInformation] = {
    for {
      depositId <- getDepositId
      depositor <- getDepositorId
      datamanager <- getDatamanager
      dansDoiRegistered <- getDansDoiRegistered
      doiIdentifier <- getDoiIdentifier
      fedoraId <- getFedoraIdentifier
      stateLabel <- getStateLabel
      stateDescription <- getStateDescription
      creationTimestamp <- getCreationTime
      lastModified <- getLastModifiedTimestamp
      continuedDeposits <- getNumberOfContinuedDeposits
      storageSpace = FileUtils.sizeOfDirectory(deposit.toJava)
      depositOrigin <- getDepositOrigin
      bagDirName <- getBagDirName
    } yield DepositInformation(
      depositId = depositId,
      depositor = depositor.getOrElse(notAvailable),
      datamanager = datamanager,
      dansDoiRegistered = dansDoiRegistered.map(BooleanUtils.toBoolean),
      doiIdentifier = doiIdentifier,
      fedoraIdentifier = fedoraId,
      state = stateLabel,
      description = stateDescription.map(StringUtils.abbreviate(_, 1000)),
      creationTimestamp = creationTimestamp.fold(notAvailable)(_.toString(dateTimeFormatter)),
      lastModified = lastModified.fold(notAvailable)(_.toString(dateTimeFormatter)),
      numberOfContinuedDeposits = continuedDeposits,
      storageSpace = storageSpace,
      origin = depositOrigin.getOrElse(notAvailable),
      location = location,
      bagDirName = bagDirName.getOrElse(notAvailable),
    )
  }

  override def setCurationParameters(dansDoiRegistered: Boolean, newState: State, newDescription: String): Try[Unit] = {
    for {
      dp <- depositProperties
      _ = dp.setProperty(dansDoiRegisteredKey, BooleanUtils.toStringYesNo(dansDoiRegistered))
      _ = dp.setProperty("curation.performed", BooleanUtils.toStringYesNo(true))
      _ = dp.setProperty(stateLabelKey, newState.toString)
      _ = dp.setProperty(stateDescriptionKey, newDescription)
      _ = dp.save()
    } yield ()
  }

  override def deleteDeposit(deleteParams: DeleteParameters)(implicit dansDoiPrefixes: List[String]): Try[Option[DepositInformation]] = {
    def doDelete(depositorId: DepositorId, stateLabel: State): Try[Boolean] = {
      if (deleteParams.onlyData)
        deleteOnlyDataFromDeposit(deleteParams.doUpdate, depositorId, stateLabel)
          .flatMap {
            case b @ true if deleteParams.doUpdate =>
              deleteParams.newState
                .map { case (newStateLabel, newStateDescription) => setState(newStateLabel, newStateDescription) }
                .getOrElse(Success(()))
                .map(_ => b)
            case b => Success(b)
          }
      else
        deleteDepositDirectory(deleteParams.doUpdate, depositorId, stateLabel)
    }

    for {
      depositInfo <- getDepositInformation
      depositorId <- getDepositorId.map(_.getOrElse("<unknown>"))
      stateLabel <- getStateLabel.map(_.getOrElse(State.UNKNOWN))
      deletableFiles <- doDelete(depositorId, stateLabel)
    } yield if (deletableFiles) Some(depositInfo)
            else None
  }

  private def deleteOnlyDataFromDeposit(doUpdate: Boolean, depositorId: DepositorId, depositState: State): Try[Boolean] = Try {
    var filesToDelete = false
    for (file <- deposit.list
         if file.name != depositPropertiesFileName) {
      filesToDelete = true
      validateThatFileIsReadable(file)
        .doIfSuccess(_ => doDeleteDataFromDeposit(doUpdate, depositorId, depositState, file)).unsafeGetOrThrow
    }
    filesToDelete
  }

  private def doDeleteDataFromDeposit(doUpdate: Boolean, depositorId: DepositorId, depositState: State, file: File): Unit = {
    if (doUpdate) {
      logger.info(s"DELETE data from deposit for $depositorId from $depositState $deposit")
      if (file.isDirectory) FileUtils.deleteDirectory(file.toJava)
      else file.delete()
    }
  }

  private def deleteDepositDirectory(doUpdate: Boolean, depositorId: DepositorId, depositState: State): Try[Boolean] = Try {
    if (doUpdate) {
      logger.info(s"DELETE deposit for $depositorId from $depositState $deposit")
      FileUtils.deleteDirectory(deposit.toJava)
    }
    true
  }
}

object FileDepositProperties {
  val depositPropertiesFileName = "deposit.properties"
  val depositIdKey = "bag-store.bag-id"
  val stateLabelKey = "state.label"
  val stateDescriptionKey = "state.description"
  val dansDoiRegisteredKey = "identifier.dans-doi.registered"
}
