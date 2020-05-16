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
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData

import scala.util.Try

class CompoundDepositPropertiesRepository(file: FileDepositPropertiesRepository,
                                          service: ServiceDepositPropertiesRepository,
                                         ) extends DepositPropertiesRepository {

  override def load(depositId: DepositId): Try[DepositProperties] = {
    for {
      _ <- service.load(depositId)
      props <- file.load(depositId)
    } yield props
  }

  override def getSummaryReportData(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[SummaryReportData] = {
    for {
      _ <- service.getSummaryReportData(depositor, datamanager, age)
      data <- file.getSummaryReportData(depositor, datamanager, age)
    } yield data
  }

  override def listReportData(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[Stream[DepositInformation]] = {
    for {
      _ <- service.listReportData(depositor, datamanager, age)
      data <- file.listReportData(depositor, datamanager, age)
    } yield data
  }

  override def getCurationParametersByDatasetId(datasetId: DatasetId): Try[(DepositId, Option[State])] = {
    for {
      _ <- service.getCurationParametersByDatasetId(datasetId)
      params <- file.getCurationParametersByDatasetId(datasetId)
    } yield params
  }

  override def listDepositsToBeCleaned(filterOnDepositor: Option[DepositorId], filterOnAge: Age, filterOnState: State): Try[Stream[DepositProperties with FileSystemDeposit]] = {
    for {
      _ <- service.listDepositsToBeCleaned(filterOnDepositor, filterOnAge, filterOnState)
      deposits <- file.listDepositsToBeCleaned(filterOnDepositor, filterOnAge, filterOnState)
    } yield deposits
  }
}
