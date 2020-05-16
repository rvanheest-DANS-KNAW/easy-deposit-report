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
import nl.knaw.dans.easy.managedeposit.fixture.{ FileSystemTestDataFixture, FixedDateTime, TestSupportFixture }
import nl.knaw.dans.easy.managedeposit.properties.DepositPropertiesRepository.SummaryReportData
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

import scala.util.{ Failure, Success }

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
  implicit val dansDoiPrefixes: List[String] = List("10.17026/", "10.5072/")
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

  "getSummaryReportData" should "retrieve the counts necessary for a summary report (without depositor)" in {
    val response =
      """{
        |  "data": {
        |    "total": {
        |      "totalCount": 55
        |    },
        |    "draft": {
        |      "totalCount": 1
        |    },
        |    "uploaded": {
        |      "totalCount": 2
        |    },
        |    "finalizing": {
        |      "totalCount": 3
        |    },
        |    "invalid": {
        |      "totalCount": 4
        |    },
        |    "submitted": {
        |      "totalCount": 5
        |    },
        |    "rejected": {
        |      "totalCount": 6
        |    },
        |    "failed": {
        |      "totalCount": 7
        |    },
        |    "in_review": {
        |      "totalCount": 8
        |    },
        |    "archived": {
        |      "totalCount": 9
        |    },
        |    "fedora_archived": {
        |      "totalCount": 10
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getSummaryReportData(Option.empty, Option.empty, Option.empty).success.value shouldBe SummaryReportData(
      total = 55,
      totalPerState = Map(
        State.DRAFT -> 1,
        State.UPLOADED -> 2,
        State.FINALIZING -> 3,
        State.INVALID -> 4,
        State.SUBMITTED -> 5,
        State.REJECTED -> 6,
        State.FAILED -> 7,
        State.IN_REVIEW -> 8,
        State.ARCHIVED -> 9,
        State.FEDORA_ARCHIVED -> 10,
      ),
    )

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.GetSummaryReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.GetSummaryReportData.operationName)
    }
  }

  it should "only list totals for deposits that apply to the filters" in {
    val response =
      """{
        |  "data": {
        |    "total": {
        |      "totalCount": 55
        |    },
        |    "draft": {
        |      "totalCount": 1
        |    },
        |    "uploaded": {
        |      "totalCount": 2
        |    },
        |    "finalizing": {
        |      "totalCount": 3
        |    },
        |    "invalid": {
        |      "totalCount": 4
        |    },
        |    "submitted": {
        |      "totalCount": 5
        |    },
        |    "rejected": {
        |      "totalCount": 6
        |    },
        |    "failed": {
        |      "totalCount": 7
        |    },
        |    "in_review": {
        |      "totalCount": 8
        |    },
        |    "archived": {
        |      "totalCount": 9
        |    },
        |    "fedora_archived": {
        |      "totalCount": 10
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getSummaryReportData(Option.empty, Option("datamanager1"), Option(1)).success.value shouldBe SummaryReportData(
      total = 55,
      totalPerState = Map(
        State.DRAFT -> 1,
        State.UPLOADED -> 2,
        State.FINALIZING -> 3,
        State.INVALID -> 4,
        State.SUBMITTED -> 5,
        State.REJECTED -> 6,
        State.FAILED -> 7,
        State.IN_REVIEW -> 8,
        State.ARCHIVED -> 9,
        State.FEDORA_ARCHIVED -> 10,
      ),
    )

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.GetSummaryReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.GetSummaryReportData.operationName) ~
        ("variables" -> {
          ("curator" -> {
            ("userId" -> "datamanager1") ~ ("filter" -> "LATEST")
          }) ~
            ("laterThan" -> "2018-03-21T20:43:01.000Z")
        })
    }
  }

  it should "retrieve the counts necessary for a summary report (with depositor)" in {
    val response =
      """{
        |  "data": {
        |    "depositor": {
        |      "total": {
        |        "totalCount": 55
        |      },
        |      "draft": {
        |        "totalCount": 1
        |      },
        |      "uploaded": {
        |        "totalCount": 2
        |      },
        |      "finalizing": {
        |        "totalCount": 3
        |      },
        |      "invalid": {
        |        "totalCount": 4
        |      },
        |      "submitted": {
        |        "totalCount": 5
        |      },
        |      "rejected": {
        |        "totalCount": 6
        |      },
        |      "failed": {
        |        "totalCount": 7
        |      },
        |      "in_review": {
        |        "totalCount": 8
        |      },
        |      "archived": {
        |        "totalCount": 9
        |      },
        |      "fedora_archived": {
        |        "totalCount": 10
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getSummaryReportData(Option("user001"), Option.empty, Option.empty).success.value shouldBe SummaryReportData(
      total = 55,
      totalPerState = Map(
        State.DRAFT -> 1,
        State.UPLOADED -> 2,
        State.FINALIZING -> 3,
        State.INVALID -> 4,
        State.SUBMITTED -> 5,
        State.REJECTED -> 6,
        State.FAILED -> 7,
        State.IN_REVIEW -> 8,
        State.ARCHIVED -> 9,
        State.FEDORA_ARCHIVED -> 10,
      ),
    )

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.GetSummaryReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.GetSummaryReportData.operationName) ~
        ("variables" -> ("depositorId" -> "user001"))
    }
  }

  it should "only list totals for deposits that apply to the filters (with depositor)" in {
    val response =
      """{
        |  "data": {
        |    "depositor": {
        |      "total": {
        |        "totalCount": 55
        |      },
        |      "draft": {
        |        "totalCount": 1
        |      },
        |      "uploaded": {
        |        "totalCount": 2
        |      },
        |      "finalizing": {
        |        "totalCount": 3
        |      },
        |      "invalid": {
        |        "totalCount": 4
        |      },
        |      "submitted": {
        |        "totalCount": 5
        |      },
        |      "rejected": {
        |        "totalCount": 6
        |      },
        |      "failed": {
        |        "totalCount": 7
        |      },
        |      "in_review": {
        |        "totalCount": 8
        |      },
        |      "archived": {
        |        "totalCount": 9
        |      },
        |      "fedora_archived": {
        |        "totalCount": 10
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    repo.getSummaryReportData(Option("user001"), Option("datamanager1"), Option(0)).success.value shouldBe SummaryReportData(
      total = 55,
      totalPerState = Map(
        State.DRAFT -> 1,
        State.UPLOADED -> 2,
        State.FINALIZING -> 3,
        State.INVALID -> 4,
        State.SUBMITTED -> 5,
        State.REJECTED -> 6,
        State.FAILED -> 7,
        State.IN_REVIEW -> 8,
        State.ARCHIVED -> 9,
        State.FEDORA_ARCHIVED -> 10,
      ),
    )

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.GetSummaryReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.GetSummaryReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("curator" -> {
              ("userId" -> "datamanager1") ~ ("filter" -> "LATEST")
            }) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z")
        })
    }
  }

  "listReportData" should "fetch all report data at once without depositor" in {
    val response =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": false,
        |        "endCursor": "abcdef",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": "bag1",
        |            "origin": "API",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": {
        |              "label": "REJECTED",
        |              "description": "ERRRRRR"
        |            },
        |            "doi": {
        |              "value": "10.5072/dans-a1b-cde2"
        |            },
        |            "fedora": {
        |              "value": "easy-dataset:1"
        |            },
        |            "doiRegistered": false,
        |            "curator": {
        |              "userId": "archie001"
        |            }
        |          }
        |        },
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": null,
        |            "origin": "SMD",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": null,
        |            "doi": null,
        |            "fedora": null,
        |            "doiRegistered": null,
        |            "curator": null
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listReportData(Option.empty, Option.empty, Option.empty).map(_.toList)) {
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> ("count" -> 100))
    }
  }

  it should "fetch all report data paginated without depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": true,
        |        "endCursor": "abcdef",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": "bag1",
        |            "origin": "API",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": {
        |              "label": "REJECTED",
        |              "description": "ERRRRRR"
        |            },
        |            "doi": {
        |              "value": "10.5072/dans-a1b-cde2"
        |            },
        |            "fedora": {
        |              "value": "easy-dataset:1"
        |            },
        |            "doiRegistered": false,
        |            "curator": {
        |              "userId": "archie001"
        |            }
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
        |        "hasNextPage": false,
        |        "endCursor": "ghijkl",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": null,
        |            "origin": "SMD",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": null,
        |            "doi": null,
        |            "fedora": null,
        |            "doiRegistered": null,
        |            "curator": null
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))

    inside(repo.listReportData(Option.empty, Option.empty, Option.empty).map(_.toList)) {
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> ("count" -> 100))
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("count" -> 100) ~
            ("after" -> "abcdef")
        })
    }
  }

  it should "fetch only those records that apply to the given filters at once without depositor" in {
    val response =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": false,
        |        "endCursor": "abcdef",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": "bag1",
        |            "origin": "API",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": {
        |              "label": "REJECTED",
        |              "description": "ERRRRRR"
        |            },
        |            "doi": {
        |              "value": "10.5072/dans-a1b-cde2"
        |            },
        |            "fedora": {
        |              "value": "easy-dataset:1"
        |            },
        |            "doiRegistered": false,
        |            "curator": {
        |              "userId": "archie001"
        |            }
        |          }
        |        },
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": null,
        |            "origin": "SMD",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": null,
        |            "doi": null,
        |            "fedora": null,
        |            "doiRegistered": null,
        |            "curator": null
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listReportData(Option.empty, Option("archie001"), Option(0)).map(_.toList)) {
      case Failure(exception) => fail(exception)
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST")) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("count" -> 100)
        })
    }
  }

  it should "fetch only those records that apply to the given filters paginated without depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "deposits": {
        |      "pageInfo": {
        |        "hasNextPage": true,
        |        "endCursor": "abcdef",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": "bag1",
        |            "origin": "API",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": {
        |              "label": "REJECTED",
        |              "description": "ERRRRRR"
        |            },
        |            "doi": {
        |              "value": "10.5072/dans-a1b-cde2"
        |            },
        |            "fedora": {
        |              "value": "easy-dataset:1"
        |            },
        |            "doiRegistered": false,
        |            "curator": {
        |              "userId": "archie001"
        |            }
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
        |        "hasNextPage": false,
        |        "endCursor": "ghijkl",
        |      },
        |      "edges": [
        |        {
        |          "node": {
        |            "depositId": "input-ruimtereis01",
        |            "depositor": {
        |              "depositorId": "user001"
        |            },
        |            "bagName": null,
        |            "origin": "SMD",
        |            "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |            "lastModified": "2020-05-12T12:37:07.073Z",
        |            "state": null,
        |            "doi": null,
        |            "fedora": null,
        |            "doiRegistered": null,
        |            "curator": null
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))

    inside(repo.listReportData(Option.empty, Option("archie001"), Option(0)).map(_.toList)) {
      case Failure(exception) => fail(exception)
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST")) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("count" -> 100)
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithoutDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST")) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("count" -> 100) ~
            ("after" -> "abcdef")
        })
    }
  }

  it should "fetch all report data at once with depositor" in {
    val response =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": false,
        |          "endCursor": "abcdef",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": "bag1",
        |              "origin": "API",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": {
        |                "label": "REJECTED",
        |                "description": "ERRRRRR"
        |              },
        |              "doi": {
        |                "value": "10.5072/dans-a1b-cde2"
        |              },
        |              "fedora": {
        |                "value": "easy-dataset:1"
        |              },
        |              "doiRegistered": false,
        |              "curator": {
        |                "userId": "archie001"
        |              }
        |            }
        |          },
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": null,
        |              "origin": "SMD",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": null,
        |              "doi": null,
        |              "fedora": null,
        |              "doiRegistered": null,
        |              "curator": null
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listReportData(Option("user001"), Option.empty, Option.empty).map(_.toList)) {
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("count" -> 100)
        })
    }
  }

  it should "fetch all report data paginated with depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": true,
        |          "endCursor": "abcdef",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": "bag1",
        |              "origin": "API",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": {
        |                "label": "REJECTED",
        |                "description": "ERRRRRR"
        |              },
        |              "doi": {
        |                "value": "10.5072/dans-a1b-cde2"
        |              },
        |              "fedora": {
        |                "value": "easy-dataset:1"
        |              },
        |              "doiRegistered": false,
        |              "curator": {
        |                "userId": "archie001"
        |              }
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
        |          "hasNextPage": false,
        |          "endCursor": "ghijkl",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": null,
        |              "origin": "SMD",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": null,
        |              "doi": null,
        |              "fedora": null,
        |              "doiRegistered": null,
        |              "curator": null
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))

    inside(repo.listReportData(Option("user001"), Option.empty, Option.empty).map(_.toList)) {
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("count" -> 100)
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("count" -> 100) ~
            ("after" -> "abcdef")
        })
    }
  }

  it should "fetch only those records that apply to the given filters at once with depositor" in {
    val response =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": false,
        |          "endCursor": "abcdef",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": "bag1",
        |              "origin": "API",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": {
        |                "label": "REJECTED",
        |                "description": "ERRRRRR"
        |              },
        |              "doi": {
        |                "value": "10.5072/dans-a1b-cde2"
        |              },
        |              "fedora": {
        |                "value": "easy-dataset:1"
        |              },
        |              "doiRegistered": false,
        |              "curator": {
        |                "userId": "archie001"
        |              }
        |            }
        |          },
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": null,
        |              "origin": "SMD",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": null,
        |              "doi": null,
        |              "fedora": null,
        |              "doiRegistered": null,
        |              "curator": null
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response))

    inside(repo.listReportData(Option("user001"), Option("archie001"), Option(0)).map(_.toList)) {
      case Failure(exception) => fail(exception)
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST")) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("count" -> 100)
        })
    }
  }

  it should "fetch only those records that apply to the given filters paginated with depositor" in {
    val response1 =
      """{
        |  "data": {
        |    "depositor": {
        |      "deposits": {
        |        "pageInfo": {
        |          "hasNextPage": true,
        |          "endCursor": "abcdef",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "aba410b6-1a55-40b2-9ebe-6122aad00285",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": "bag1",
        |              "origin": "API",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": {
        |                "label": "REJECTED",
        |                "description": "ERRRRRR"
        |              },
        |              "doi": {
        |                "value": "10.5072/dans-a1b-cde2"
        |              },
        |              "fedora": {
        |                "value": "easy-dataset:1"
        |              },
        |              "doiRegistered": false,
        |              "curator": {
        |                "userId": "archie001"
        |              }
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
        |          "hasNextPage": false,
        |          "endCursor": "ghijkl",
        |        },
        |        "edges": [
        |          {
        |            "node": {
        |              "depositId": "input-ruimtereis01",
        |              "depositor": {
        |                "depositorId": "user001"
        |              },
        |              "bagName": null,
        |              "origin": "SMD",
        |              "creationTimestamp": "2018-12-31T23:00:00.000Z",
        |              "lastModified": "2020-05-12T12:37:07.073Z",
        |              "state": null,
        |              "doi": null,
        |              "fedora": null,
        |              "doiRegistered": null,
        |              "curator": null
        |            }
        |          }
        |        ]
        |      }
        |    }
        |  }
        |}""".stripMargin
    server.enqueue(new MockResponse().setBody(response1))
    server.enqueue(new MockResponse().setBody(response2))

    inside(repo.listReportData(Option("user001"), Option("archie001"), Option(0)).map(_.toList)) {
      case Failure(exception) => fail(exception)
      case Success(prop1 :: prop2 :: Nil) =>
        prop1 shouldBe DepositInformation(
          depositId = "aba410b6-1a55-40b2-9ebe-6122aad00285",
          depositor = "user001",
          datamanager = Option("archie001"),
          dansDoiRegistered = Option(false),
          doiIdentifier = Option("10.5072/dans-a1b-cde2"),
          fedoraIdentifier = Option("easy-dataset:1"),
          state = Option(State.REJECTED),
          description = Option("ERRRRRR"),
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "API",
          location = "UNKNOWN",
          bagDirName = "bag1",
        )
        prop2 shouldBe DepositInformation(
          depositId = "input-ruimtereis01",
          depositor = "user001",
          datamanager = Option.empty,
          dansDoiRegistered = Option.empty,
          doiIdentifier = Option.empty,
          fedoraIdentifier = Option.empty,
          state = Option.empty,
          description = Option.empty,
          creationTimestamp = "2018-12-31T23:00:00.000Z",
          lastModified = "2020-05-12T12:37:07.073Z",
          origin = "SMD",
          location = "INGEST_FLOW_ARCHIVED",
          bagDirName = "n/a",
        )
    }

    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("depositorId" -> "user001") ~
            ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST")) ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("count" -> 100)
        })
    }
    server.takeRequest().getBody.readUtf8() shouldBe Serialization.write {
      ("query" -> ServiceDepositPropertiesRepository.ListReportData.queryWithDepositor) ~
        ("operationName" -> ServiceDepositPropertiesRepository.ListReportData.operationName) ~
        ("variables" -> {
          ("count" -> 100) ~
            ("depositorId" -> "user001") ~
            ("laterThan" -> "2018-03-22T20:43:01.000Z") ~
            ("after" -> "abcdef") ~
            ("curator" -> ("userId" -> "archie001") ~ ("filter" -> "LATEST"))
        })
    }
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
