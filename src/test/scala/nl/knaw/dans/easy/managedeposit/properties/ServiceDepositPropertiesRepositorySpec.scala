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

import better.files.File
import nl.knaw.dans.easy.managedeposit.State
import nl.knaw.dans.easy.managedeposit.fixture.{ FileSystemTestDataFixture, FixedDateTime, TestSupportFixture }
import nl.knaw.dans.easy.managedeposit.properties.graphql.GraphQLClient
import okhttp3.HttpUrl
import okhttp3.mockwebserver.{ MockResponse, MockWebServer }
import org.json4s.JsonDSL._
import org.json4s.ext.EnumNameSerializer
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }
import org.scalatest.BeforeAndAfterAll
import scalaj.http.{ BaseHttp, Http }

import scala.util.Success

class ServiceDepositPropertiesRepositorySpec extends TestSupportFixture
  with BeforeAndAfterAll
  with FileSystemTestDataFixture
  with FixedDateTime {

  // configure the mock server
  private val server = new MockWebServer
  server.start()
  private val test_server = "/test_server/"
  private val baseUrl: HttpUrl = server.url(test_server)

  private val sword2Inbox: File = depositDir / "sword2-inbox"
  private val ingestFlowInbox: File = depositDir / "ingest-flow-inbox"
  private val ingestFlowArchivedInbox: File = depositDir / "ingest-flow-archived-inbox"

  implicit val http: BaseHttp = Http
  implicit val formats: Formats = DefaultFormats + new EnumNameSerializer(State)
  private val client = new GraphQLClient(baseUrl.url())
  private val repo = new ServiceDepositPropertiesRepository(client, sword2Inbox, ingestFlowInbox, Option(ingestFlowArchivedInbox))

  override def beforeEach(): Unit = {
    super.beforeEach()

    sword2Inbox.createDirectoryIfNotExists()
    ingestFlowInbox.createDirectoryIfNotExists()
    ingestFlowArchivedInbox.createDirectoryIfNotExists()

    ruimteReis01.moveToDirectory(ingestFlowArchivedInbox)
    ruimteReis02.moveToDirectory(ingestFlowInbox)
    ruimteReis03.moveToDirectory(ingestFlowInbox)
    ruimteReis04.moveToDirectory(sword2Inbox)
    ruimteReis05.moveToDirectory(sword2Inbox)
  }

  override protected def afterAll(): Unit = {
    server.shutdown()
    super.afterAll()
  }

  "listReportData" should "???" ignore { // TODO test after implementation

  }

  "getCurationParametersByDatasetId" should "retrieve the curation parameters if the deposit exists" in {
    val response =
      """{
        |  "data": {
        |    "identifier": {
        |      "deposit": {
        |        "depositId": "my-deposit-id",
        |        "state": {
        |          "label": "REJECTED"
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getCurationParametersByDatasetId("my-dataset-id").success.value shouldBe ("my-deposit-id" -> Some(State.REJECTED))

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.FindByDatasetId.query) ~
        ("operationName" -> ServiceDepositPropertiesRepository.FindByDatasetId.operationName) ~
        ("variables" -> Map(
          "datasetId" -> "my-dataset-id",
        ))
    }
  }

  it should "fail if the datasetId cannot be found" in {
    val response =
      """{
        |  "data": {
        |    "identifier": null
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getCurationParametersByDatasetId("my-dataset-id").failure.exception shouldBe DatasetDoesNotExist("my-dataset-id")

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.FindByDatasetId.query) ~
        ("operationName" -> ServiceDepositPropertiesRepository.FindByDatasetId.operationName) ~
        ("variables" -> Map(
          "datasetId" -> "my-dataset-id",
        ))
    }
  }

  "listDepositsToBeCleaned" should "fetch all data at once if their aren't many records with depositor" in {
    val response =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": false,
        |          "endCursor": "abcdef"
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01"
        |            }
        |          },
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis02"
        |            }
        |          },
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis04"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listDepositsToBeCleaned(Option("depositor"), 14, State.FAILED).map(_.toList)) {
      case Success(prop1 :: prop2 :: prop3 :: Nil) =>
        prop1.depositPath shouldBe ingestFlowArchivedInbox / ruimteReis01.name
        prop2.depositPath shouldBe ingestFlowInbox / ruimteReis02.name
        prop3.depositPath shouldBe sword2Inbox / ruimteReis04.name
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100) ~
            ("depositorId" -> "depositor")
        })
    }
  }

  it should "fetch all data at once if their aren't many records without depositor" in {
    val response =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": false,
        |        "endCursor": "abcdef"
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01"
        |          }
        |        },
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis02"
        |          }
        |        },
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis04"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listDepositsToBeCleaned(Option.empty, 14, State.FAILED).map(_.toList)) {
      case Success(prop1 :: prop2 :: prop3 :: Nil) =>
        prop1.depositPath shouldBe ingestFlowArchivedInbox / ruimteReis01.name
        prop2.depositPath shouldBe ingestFlowInbox / ruimteReis02.name
        prop3.depositPath shouldBe sword2Inbox / ruimteReis04.name
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100)
        })
    }
  }

  it should "fetch data in a paginated way with depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": true,
        |          "endCursor": "abcdef"
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    val response2 =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": true,
        |          "endCursor": "ghijkl"
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis02"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    val response3 =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": false,
        |          "endCursor": "mnopqr"
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis04"
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))
    server.enqueue(new MockResponse().setBody(response3))

    inside(repo.listDepositsToBeCleaned(Option("depositor"), 14, State.FAILED).map(_.toList)) {
      case Success(prop1 :: prop2 :: prop3 :: Nil) =>
        prop1.depositPath shouldBe ingestFlowArchivedInbox / ruimteReis01.name
        prop2.depositPath shouldBe ingestFlowInbox / ruimteReis02.name
        prop3.depositPath shouldBe sword2Inbox / ruimteReis04.name
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100) ~
            ("depositorId" -> "depositor")
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("count" -> 100) ~
            ("state" -> "FAILED") ~
            ("depositorId" -> "depositor") ~
            ("after" -> "abcdef") ~
            ("earlierThan" -> "2018-03-08T20:43:01.000Z")
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("count" -> 100) ~
            ("state" -> "FAILED") ~
            ("depositorId" -> "depositor") ~
            ("after" -> "ghijkl") ~
            ("earlierThan" -> "2018-03-08T20:43:01.000Z")
        })
    }
  }

  it should "fetch data in a paginated way without depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": true,
        |        "endCursor": "abcdef"
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    val response2 =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": true,
        |        "endCursor": "ghijkl"
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis02"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    val response3 =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": false,
        |        "endCursor": "mnopqr"
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis04"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))
    server.enqueue(new MockResponse().setBody(response3))

    inside(repo.listDepositsToBeCleaned(Option.empty, 14, State.FAILED).map(_.toList)) {
      case Success(prop1 :: prop2 :: prop3 :: Nil) =>
        prop1.depositPath shouldBe ingestFlowArchivedInbox / ruimteReis01.name
        prop2.depositPath shouldBe ingestFlowInbox / ruimteReis02.name
        prop3.depositPath shouldBe sword2Inbox / ruimteReis04.name
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100)
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100) ~
            ("after" -> "abcdef")
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListDepositsToBeCleaned.operationName) ~
        ("variables" -> {
          ("earlierThan" -> "2018-03-08T20:43:01.000Z") ~
            ("state" -> "FAILED") ~
            ("count" -> 100) ~
            ("after" -> "ghijkl")
        })
    }
  }
}
