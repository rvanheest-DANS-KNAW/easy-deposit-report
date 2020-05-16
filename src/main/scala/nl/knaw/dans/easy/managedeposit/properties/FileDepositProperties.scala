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

import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.FileDepositProperties._
import nl.knaw.dans.easy.managedeposit.properties.FileSystemDeposit.depositPropertiesFileName
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.commons.lang.{ BooleanUtils, StringUtils }
import org.joda.time.{ DateTime, DateTimeZone, Duration }

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.Try

class FileDepositProperties(override val depositPath: Deposit,
                            override val location: String,
                           ) extends DepositProperties with FileSystemDeposit with DebugEnhancedLogging {
  private lazy val depositProperties: Try[PropertiesConfiguration] = findDepositProperties
  private lazy val lastModified: Option[DateTime] = getLastModifiedTimestamp.unsafeGetOrThrow

  private def findDepositProperties: Try[PropertiesConfiguration] = Try {
    if (depositPropertiesFilePath.exists) {
      debug(s"Getting info from $depositPath")
      new PropertiesConfiguration() {
        setDelimiterParsingDisabled(true)
        setFile(depositPropertiesFilePath.toJava)
        load(depositPropertiesFilePath.toJava)
      }
    }
    else
      throw new FileNotFoundException(s"$depositPropertiesFileName does not exist for $depositPath")
  }

  override def properties: Map[String, String] = {
    depositProperties
      .map(props => {
        props.getKeys.asScala
          .map(key => key -> props.getString(key))
          .toMap
      })
      .getOrElse(Map.empty) + ("depositId" -> depositPath.name)
  }

  private def getProperty(key: String): Try[Option[String]] = {
    depositProperties.map(props => Option(props.getString(key)))
  }

  def getDepositId: Try[DepositId] = {
    getProperty(depositIdKey).map(_.getOrElse(depositPath.name))
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
      listDepositFiles
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

  override def setState(label: State, description: String): Try[Unit] = {
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

  def getBagDirName: Try[Option[String]] = {
    getProperty("bag-store.bag-name").map(_.orElse(retrieveBagNameFromFilesystem))
  }

  override def getDepositInformation(implicit dansDoiPrefixes: List[String]): Try[DepositInformation] = {
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

  override def deleteDepositProperties(): Try[Unit] = Try {
    deleteFile(depositPropertiesFilePath)
  }
}

object FileDepositProperties {
  val depositIdKey = "bag-store.bag-id"
  val stateLabelKey = "state.label"
  val stateDescriptionKey = "state.description"
  val dansDoiRegisteredKey = "identifier.dans-doi.registered"
}
