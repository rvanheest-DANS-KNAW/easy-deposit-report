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
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositProperties.{ SetCurationParameters, SetState }
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit.{ DepositId, DepositInformation }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.JsonAST.{ JBool, JString }
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods
import org.json4s.{ Formats, JValue }

import scala.util.Try

class ServiceDepositProperties(depositId: DepositId, client: GraphQLClient)(implicit formats: Formats) extends DepositProperties with DebugEnhancedLogging {

  private def format(json: JValue): String = JsonMethods.compact(JsonMethods.render(json))

  private def logMutationOutput(operationName: String)(json: JValue): Unit = {
    logger.debug(s"Mutation $operationName returned ${ format(json) }")
  }

  override def properties: Map[String, String] = {
    throw new UnsupportedOperationException("You cannot list all properties from the GraphQL service. Please configure the application such that the filesystem deposit.properties are used.")
  }

  override def setState(label: State, description: String): Try[Unit] = {
    val setStateVariables = Map(
      "depositId" -> depositId,
      "label" -> label.toString,
      "description" -> description,
    )

    client.doQuery(SetState.query, SetState.operationName, setStateVariables)
      .toTry
      .doIfSuccess(logMutationOutput(SetState.operationName))
      .map(_ => ())
  }

  override def getDepositInformation(implicit dansDoiPrefixes: List[String]): Try[DepositInformation] = ???

  override def setCurationParameters(dansDoiRegistered: Boolean, newState: State, newDescription: String): Try[Unit] = {
    implicit val convertJson: Any => JValue = {
      case s: String => JString(s)
      case b: Boolean => JBool(b)
    }
    val setCurationVariables = Map(
      "depositId" -> depositId,
      "dansDoiRegistered" -> dansDoiRegistered,
      "stateLabel" -> newState.toString,
      "stateDescription" -> newDescription,
    )

    client.doQuery(SetCurationParameters.query, SetCurationParameters.operationName, setCurationVariables)
      .toTry
      .doIfSuccess(logMutationOutput(SetCurationParameters.operationName))
      .map(_ => ())
  }

  override def deleteDepositProperties(): Try[Unit] = ???
}

object ServiceDepositProperties {

  object SetState {
    val operationName = "SetState"
    val query: String =
      """mutation SetState($depositId: UUID!, $label: StateLabel!, $description: String!) {
        |  updateState(input: {depositId: $depositId, label: $label, description: $description}) {
        |    state {
        |      label
        |      description
        |    }
        |  }
        |}""".stripMargin
  }

  object SetCurationParameters {
    val operationName = "SetCurationParameters"
    val query: String =
      """mutation SetCurationParameters($depositId: UUID!, $dansDoiRegistered: Boolean!, $stateLabel: StateLabel!, $stateDescription: String!) {
        |  updateState(input: {depositId: $depositId, label: $stateLabel, description: $stateDescription}) {
        |    state {
        |      label
        |      description
        |    }
        |  }
        |  setDoiRegistered(input: {depositId: $depositId, value: $dansDoiRegistered}) {
        |    doiRegistered {
        |      value
        |    }
        |  }
        |  setIsCurationPerformed(input: {depositId: $depositId, isCurationPerformed: true}) {
        |    isCurationPerformed {
        |      value
        |    }
        |  }
        |}""".stripMargin
  }
}
