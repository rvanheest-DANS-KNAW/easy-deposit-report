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

object Location extends Enumeration {
  type Location = Value

  // @formatter:off
  val SWORD2              : Value = Value("SWORD2")
  val INGEST_FLOW         : Value = Value("INGEST_FLOW")
  val INGEST_FLOW_ARCHIVED: Value = Value("INGEST_FLOW_ARCHIVED")
  val UNKNOWN             : Value = Value("UNKNOWN")
  // @formatter:on
}
