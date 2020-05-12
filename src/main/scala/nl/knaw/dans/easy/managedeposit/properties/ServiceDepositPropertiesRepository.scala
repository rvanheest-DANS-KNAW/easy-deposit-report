package nl.knaw.dans.easy.managedeposit.properties

import nl.knaw.dans.easy.managedeposit
import nl.knaw.dans.easy.managedeposit.State.State
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit._
import nl.knaw.dans.easy.managedeposit.properties.ServiceDepositPropertiesRepository.FindByDatasetId
import org.json4s.Formats
import org.json4s.JsonDSL._

import scala.util.{ Failure, Success, Try }

class ServiceDepositPropertiesRepository(client: GraphQLClient)(implicit formats: Formats) extends DepositPropertiesRepository {

  override def load(depositId: DepositId): Try[DepositProperties] = Try {
    new ServiceDepositProperties(depositId, client)
  }

  override def listReportData(depositor: Option[DepositorId], datamanager: Option[Datamanager], age: Option[Age]): Try[Stream[DepositInformation]] = ???

  override def getCurationParametersByDatasetId(datasetId: DatasetId): Try[(DepositId, Option[State])] = {
    for {
      json <- client.doQuery(FindByDatasetId.query, FindByDatasetId.operationName, Map("datasetId" -> datasetId)).toTry
      identifier = json.extract[FindByDatasetId.Data].identifier
      result <- identifier
          .map(identifier => {
            val deposit = identifier.deposit
            Success(deposit.depositId -> deposit.state.map(_.label))
          })
          .getOrElse(Failure(DatasetDoesNotExist(datasetId)))
    } yield result
  }

  override def listDepositsToBeCleaned(deleteParams: managedeposit.DeleteParameters): Try[Stream[DepositProperties]] = ???
}

object ServiceDepositPropertiesRepository {

  object FindByDatasetId {
    case class Data(identifier: Option[Identifier])
    case class Identifier(deposit: Deposit)
    case class Deposit(depositId: DepositId, state: Option[StateObj])
    case class StateObj(label: State)

    val operationName = "FindByDatasetId"
    val query: String =
      """query FindByDatasetId($datasetId: String!) {
        |  identifier(type: FEDORA, value: $datasetId) {
        |    deposit {
        |      depositId
        |      state {
        |        label
        |      }
        |    }
        |  }
        |}""".stripMargin
  }
}
