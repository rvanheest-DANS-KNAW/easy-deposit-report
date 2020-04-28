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
package nl.knaw.dans.easy.managedeposit

import nl.knaw.dans.easy.managedeposit.State.{ ARCHIVED, FAILED, State }
import org.apache.commons.lang.BooleanUtils

case class DepositInformation(depositId: DepositId,
                              depositor: DepositorId,
                              datamanager: Option[Datamanager],
                              dansDoiRegistered: Option[Boolean],
                              doiIdentifier: Option[String],
                              fedoraIdentifier: Option[String],
                              state: Option[State],
                              description: Option[String],
                              creationTimestamp: String,
                              lastModified: String,
                              numberOfContinuedDeposits: Int,
                              storageSpace: Long,
                              origin: String,
                              location: String,
                              bagDirName: String,
                             )(implicit dansDoiPrefixes: List[String]) {

  def isDansDoi: Boolean = doiIdentifier.exists(ident => dansDoiPrefixes.exists(ident.startsWith))

  def registeredString: String = {
    if (!isDansDoi) "yes"
    else dansDoiRegistered.fold(getDoiRegisteredFromState)(BooleanUtils.toStringYesNo)
  }

  /**
   * getDoiRegisteredFromState derives whether a deposit is registered with Datacite from the deposits state. For new deposits this
   * can be derived from the deposit.properties value 'identifier.dans-doi.registered=yes|no'. Since old deposits don't have this property we
   * use this function, making the reports backwards compatible.
   **/
  private def getDoiRegisteredFromState: String = state match {
    case Some(ARCHIVED) => "yes"
    case Some(FAILED) => "unknown"
    case _ => "no"
  }
}
