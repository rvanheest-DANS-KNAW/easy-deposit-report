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

import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit._

import scala.util.Try

trait DepositPropertiesRepository {

  def load(depositId: DepositId): Try[DepositProperties]

  def listReportData(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[Stream[DepositInformation]]

  def getCurationParametersByDatasetId(datasetId: DatasetId): Try[(DepositId, Option[State])]
  
  def listDepositsToBeCleaned(deleteParams: DeleteParameters): Try[Stream[DepositProperties]]
}
