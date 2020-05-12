package nl.knaw.dans.easy.managedeposit.properties

import nl.knaw.dans.easy.managedeposit
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositProperties.SetCurationParameters
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit.{ DepositId, DepositInformation }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.JsonAST.{ JBool, JString }
import org.json4s.native.JsonMethods
import org.json4s.{ Formats, JValue }

import scala.util.Try

class ServiceDepositProperties(depositId: DepositId, client: GraphQLClient)(implicit formats: Formats) extends DepositProperties with DebugEnhancedLogging {

  private def format(json: JValue): String = JsonMethods.compact(JsonMethods.render(json))

  private def logMutationOutput(operationName: String)(json: JValue): Unit = {
    logger.debug(s"Mutation $operationName returned ${ format(json) }")
  }

  override def properties: Map[String, String] = ???

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

  override def deleteDeposit(deleteParams: managedeposit.DeleteParameters)(implicit dansDoiPrefixes: List[String]): Try[Option[DepositInformation]] = ???
}

object ServiceDepositProperties {

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
