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

import nl.knaw.dans.easy.managedeposit.DepositInformation
import nl.knaw.dans.easy.managedeposit.State.State

import scala.util.Try

class CompoundDepositProperties(file: FileDepositProperties,
                                service: ServiceDepositProperties,
                               ) extends DepositProperties {

  override def properties: Map[String, String] = file.properties

  override def setState(label: State, description: String): Try[Unit] = {
    for {
      _ <- service.setState(label, description)
      _ <- file.setState(label, description)
    } yield ()
  }

  override def getDepositInformation(implicit dansDoiPrefixes: List[String]): Try[DepositInformation] = {
    for {
      _ <- service.getDepositInformation
      depositInfo <- file.getDepositInformation
    } yield depositInfo
  }

  override def setCurationParameters(dansDoiRegistered: Boolean, newState: State, newDescription: String): Try[Unit] = {
    for {
      _ <- service.setCurationParameters(dansDoiRegistered, newState, newDescription)
      _ <- file.setCurationParameters(dansDoiRegistered, newState, newDescription)
    } yield ()
  }

  override def deleteDepositProperties(): Try[Unit] = {
    for {
      _ <- service.deleteDepositProperties()
      _ <- file.deleteDepositProperties()
    } yield ()
  }
}
