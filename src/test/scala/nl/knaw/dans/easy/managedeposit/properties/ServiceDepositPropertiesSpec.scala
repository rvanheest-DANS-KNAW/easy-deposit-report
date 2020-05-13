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

import nl.knaw.dans.easy.managedeposit.fixture.{ FileSystemTestDataFixture, TestSupportFixture }
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import nl.knaw.dans.easy.managedeposit.{ DepositInformation, State }
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{ MockResponse, MockWebServer }
import org.json4s.JsonDSL._
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }
import org.scalatest.BeforeAndAfterAll
import scalaj.http.{ BaseHttp, Http }

import scala.util.Success

class ServiceDepositPropertiesSpec extends TestSupportFixture
  with BeforeAndAfterAll
  with FileSystemTestDataFixture {

  // configure the mock server
  private val server = new MockWebServer
  server.start()
  private val test_server = "/test_server/"
  private val baseUrl: HttpUrl = server.url(test_server)

  implicit val http: BaseHttp = Http
  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(State)
  private val client = new GraphQLClient(baseUrl.url())
  private val depositId = depositOne.name
  private implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")
  private val properties = new ServiceDepositProperties(depositId, depositOne, "SWORD2", client)

  override protected def afterAll(): Unit = {
    server.shutdown()
    super.afterAll()
  }

  "properties" should "throw an UnsupportedOperationException" in {
    an[UnsupportedOperationException] shouldBe thrownBy(properties.properties)
  }

  "setState" should "call the GraphQL service to update the state" in {
    val response =
      """{
        |  "data": {
        |    "updateState": {
        |      "state": {
        |        "label": "ARCHIVED",
        |        "description": "deposit is archived"
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    properties.setState(State.ARCHIVED, "deposit is archived") shouldBe a[Success[_]]

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositProperties.SetState.query) ~
        ("operationName" -> ServiceDepositProperties.SetState.operationName) ~
        ("variables" -> {
          ("depositId" -> depositId) ~
            ("label" -> State.ARCHIVED.toString) ~
            ("description" -> "deposit is archived")
        })
    }
  }

  "getDepositInformation" should "retrieve the required deposit information" in {
    val response =
      """{
        |  "data": {
        |    "deposit": {
        |      "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |      "depositor": {
        |        "depositorId": "user001"
        |      },
        |      "bagName": "bag1",
        |      "origin": "API",
        |      "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |      "lastModified": "2020-05-12T12:37:07.073Z",
        |      "state": {
        |        "label": "REJECTED",
        |        "description": "ERRRRRR"
        |      },
        |      "doi": {
        |        "value": "10.5072/dans-a1b-cde2"
        |      },
        |      "fedora": {
        |        "value": "easy-dataset:1"
        |      },
        |      "doiRegistered": false,
        |      "curator": {
        |        "userId": "archie001"
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    properties.getDepositInformation.success.value shouldBe DepositInformation(
      depositId = depositId,
      depositor = "user001",
      datamanager = Some("archie001"),
      dansDoiRegistered = Some(false),
      doiIdentifier = Some("10.5072/dans-a1b-cde2"),
      fedoraIdentifier = Some("easy-dataset:1"),
      state = Some(State.REJECTED),
      description = Some("ERRRRRR"),
      creationTimestamp = "2018-12-31T23:00:00.000Z",
      lastModified = "2020-05-12T12:37:07.073Z",
      numberOfContinuedDeposits = 2,
      storageSpace = 555L,
      origin = "API",
      location = "SWORD2",
      bagDirName = "bag1",
    )

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositProperties.GetDepositInformation.query) ~
        ("operationName" -> ServiceDepositProperties.GetDepositInformation.operationName) ~
        ("variables" -> ("depositId" -> depositId))
    }
  }

  "setCurationParameters" should "call the GraphQL service to update for curation" in {
    val response =
      """{
        |  "data": {
        |    "updateState": {
        |      "state": {
        |        "label": "ARCHIVED",
        |        "description": "deposit is archived"
        |      }
        |    },
        |    "setDoiRegistered": {
        |      "doiRegistered": {
        |        "value": true
        |      }
        |    },
        |    "setIsCurationPerformed": {
        |      "isCurationPerformed": {
        |        "value": true
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    properties.setCurationParameters(dansDoiRegistered = true, State.ARCHIVED, "deposit is archived") shouldBe a[Success[_]]

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositProperties.SetCurationParameters.query) ~
        ("operationName" -> ServiceDepositProperties.SetCurationParameters.operationName) ~
        ("variables" -> {
          ("depositId" -> depositId) ~
            ("dansDoiRegistered" -> true) ~
            ("stateLabel" -> State.ARCHIVED.toString) ~
            ("stateDescription" -> "deposit is archived")
        })
    }
  }
}
