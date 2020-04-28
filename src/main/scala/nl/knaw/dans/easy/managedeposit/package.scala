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
package nl.knaw.dans.easy

import better.files.File
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }

package object managedeposit {

  type DepositId = String
  type DepositorId = String
  type Deposit = File
  type Datamanager = String
  type Age = Int
  type DatasetId = String

  val dateTimeFormatter: DateTimeFormatter = ISODateTimeFormat.dateTime()
  val notAvailable = "n/a"

  case class NotReadableException(file: File, cause: Throwable = null)
    extends Exception(s"""cannot read $file""", cause)

  case class DeleteParameters(filterOnDepositor: Option[DepositorId],
                              age: Int,
                              state: State.State,
                              onlyData: Boolean,
                              doUpdate: Boolean,
                              newState: Option[(State.State, String)] = None,
                              output: Boolean = false,
                             )
}
