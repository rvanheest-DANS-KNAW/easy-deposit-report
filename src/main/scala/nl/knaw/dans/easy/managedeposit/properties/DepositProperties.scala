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

trait DepositProperties {

  def properties: Map[String, String]

  def setState(label: State, description: String): Try[Unit]

  def getDepositInformation(implicit dansDoiPrefixes: List[String]): Try[DepositInformation]

  def setCurationParameters(dansDoiRegistered: Boolean, newState: State, newDescription: String): Try[Unit]

  def deleteDepositProperties(): Try[Unit]
}
