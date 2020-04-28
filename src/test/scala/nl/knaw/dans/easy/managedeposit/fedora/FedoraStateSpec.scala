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
package nl.knaw.dans.easy.managedeposit.fedora

import nl.knaw.dans.easy.managedeposit.fedora.FedoraState.{ DELETED, PUBLISHED, SUBMITTED, toState }
import nl.knaw.dans.easy.managedeposit.fixture.TestSupportFixture

class FedoraStateSpec extends TestSupportFixture {

  "toState" should "return a state object for a recognized state" in {
    toState("SUBMITTED").value shouldBe SUBMITTED
    toState("DELETED").value shouldBe DELETED
    toState("PUBLISHED").value shouldBe PUBLISHED
  }

  it should "return a None for an unrecognized state" in {
    toState("UNRECOGNIZED") shouldBe empty
  }

  it should "return a None for an empty string" in {
    toState("") shouldBe empty
  }

  it should "return a None for a null" in {
    toState(null) shouldBe empty
  }
}
