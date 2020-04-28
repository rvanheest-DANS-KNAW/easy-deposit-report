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
package nl.knaw.dans.easy.managedeposit.fixture

import better.files.File
import better.files.File.currentWorkingDirectory
import nl.knaw.dans.easy.managedeposit.Deposit
import org.scalatest.BeforeAndAfterEach

trait FileSystemTestDataFixture extends BeforeAndAfterEach {
  this: TestSupportFixture =>

  lazy val testDir: File = {
    val path = currentWorkingDirectory / s"target/test/${ getClass.getSimpleName }"
    if (path.exists) path.delete()
    path.createDirectories()
    path
  }

  lazy protected val depositDir: File = {
    val path = testDir / "inputForEasyManageDeposit/"
    if (path.exists) path.delete()
    path.createDirectories()
    path
  }

  protected val depositOne: Deposit = depositDir / "aba410b6-1a55-40b2-9ebe-6122aad00285"
  protected val depositWithoutDepositor: Deposit = depositDir / "deposit-no-depositor-id"
  protected val depositDirWithoutProperties: Deposit = depositDir / "deposit-no-properties"
  protected val ruimteReis01: Deposit = depositDir / "input-ruimtereis01"
  protected val ruimteReis02: Deposit = depositDir / "input-ruimtereis02"
  protected val ruimteReis03: Deposit = depositDir / "input-ruimtereis03"
  protected val ruimteReis04: Deposit = depositDir / "input-ruimtereis04"
  protected val ruimteReis05: Deposit = depositDir / "input-ruimtereis05"
  protected val nonExistingDeposit: Deposit = depositDir / "deposit-3"

  override def beforeEach(): Unit = {
    super.beforeEach()
    depositDir.clear()
    File(getClass.getResource("/inputForEasyManageDeposit/").toURI).copyTo(depositDir)
  }
}
