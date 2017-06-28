/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package utils

// java
import java.time.LocalDateTime

// scalatest
import org.scalatest._

// play
import play.api.libs.json._

// sauna
import Command._
import CommandTest._

object CommandTest {

  case class SampleCommandData(id: String, description: String, enabled: Boolean)

  implicit val sampleCommandDataReads: Reads[SampleCommandData] = Json.reads[SampleCommandData]

}

class CommandTest extends FunSuite with BeforeAndAfter with EitherValues with OptionValues {
  test("extractCommand invalid JSON") {
    val command = Json.parse(""""{}""""")
    val result = Command.extractCommand[SampleCommandData](command)
    assert(result.isLeft)
  }

  test("extractCommand invalid envelope") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {}
        |    },
        |    "command": {
        |      "schema": "iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-0-0",
        |      "data": {
        |        "id": "2a836e18-4d23-4ffb-b7b5-318a33e939eb",
        |        "description": "Donec nec porttitor mauris.",
        |        "enabled": true
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val result = Command.extractCommand[SampleCommandData](command)
    assert(result.isLeft)
  }

  test("extractCommand missing schema") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {
        |        "commandId": "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        |        "whenCreated": "2017-01-02T19:14:42Z",
        |        "execution": {
        |          "semantics": "AT_LEAST_ONCE",
        |          "timeToLive": 1200000
        |        },
        |        "tags": {}
        |      }
        |    },
        |    "command": {
        |      "schema": "iglu:com.not.a.schema",
        |      "data": {}
        |    }
        |  }
        |}
      """.stripMargin)
    val result = Command.extractCommand[SampleCommandData](command)
    assert(result.isLeft)
  }

  test("extractCommand invalid data") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {
        |        "commandId": "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        |        "whenCreated": "2017-01-02T19:14:42Z",
        |        "execution": {
        |          "semantics": "AT_LEAST_ONCE",
        |          "timeToLive": 1200000
        |        },
        |        "tags": {}
        |      }
        |    },
        |    "command": {
        |      "schema": "iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-0-0",
        |      "data": {}
        |    }
        |  }
        |}
      """.stripMargin)
    val result = Command.extractCommand[SampleCommandData](command)
    assert(result.isLeft)
  }

  test("extractCommand valid data but mismatched type") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {
        |        "commandId": "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        |        "whenCreated": "2017-01-02T19:14:42Z",
        |        "execution": {
        |          "semantics": "AT_LEAST_ONCE",
        |          "timeToLive": 1200000
        |        },
        |        "tags": {}
        |      }
        |    },
        |    "command": {
        |      "schema": "iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-0-0",
        |      "data": {
        |        "roomIdOrName": "Devops",
        |        "color": "YELLOW",
        |        "message": "HipChat is awesome!",
        |        "notify": true,
        |        "messageFormat": "TEXT"
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val result = Command.extractCommand[SampleCommandData](command)
    assert(result.isLeft)
  }

  test("processEnvelope expired") {
    val envelope = CommandEnvelope(
      null,
      LocalDateTime.now().minusYears(1),
      ExecutionParams(
        null,
        Some(100)
      ),
      null)
    val result = Command.validateEnvelope(envelope)
    assert(result.isLeft && result.left.get.contains("Command has expired"))
  }

  test("processEnvelope valid") {
    val envelope = CommandEnvelope(
      null,
      LocalDateTime.now(),
      ExecutionParams(
        null,
        Some(3600000)
      ),
      null)
    val result = Command.validateEnvelope(envelope)
    assert(result.isRight)
  }

  test("processCommand mismatch schema criterion in version") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {
        |        "commandId": "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        |        "whenCreated": "2017-01-02T19:14:42Z",
        |        "execution": {
        |          "semantics": "AT_LEAST_ONCE",
        |          "timeToLive": 1200000
        |        },
        |        "tags": {}
        |      }
        |    },
        |    "command": {
        |      "schema": "iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-0-0",
        |      "data": {
        |        "roomIdOrName": "Devops",
        |        "color": "YELLOW",
        |        "message": "HipChat is awesome!",
        |        "notify": true,
        |        "messageFormat": "TEXT"
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val commandCriterion = criterion("iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-2-0").toOption.get
    val result = Command.extractCommand[Hipchat.RoomNotification](command, commandCriterion)
    assert(result == Left(Command.ExtractionSkip))
  }

  test("processCommand mismatch schema criterion") {
    val command = Json.parse(
      """
        |{
        |  "schema": "iglu:com.snowplowanalytics.sauna.commands/command/jsonschema/1-0-0",
        |  "data": {
        |    "envelope": {
        |      "schema": "iglu:com.snowplowanalytics.sauna.commands/envelope/jsonschema/1-0-0",
        |      "data": {
        |        "commandId": "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        |        "whenCreated": "2017-01-02T19:14:42Z",
        |        "execution": {
        |          "semantics": "AT_LEAST_ONCE",
        |          "timeToLive": 1200000
        |        },
        |        "tags": {}
        |      }
        |    },
        |    "command": {
        |      "schema": "iglu:com.hipchat.sauna.commands/send_room_notification/jsonschema/1-0-0",
        |      "data": {
        |        "roomIdOrName": "Devops",
        |        "color": "YELLOW",
        |        "message": "HipChat is awesome!",
        |        "notify": true,
        |        "messageFormat": "TEXT"
        |      }
        |    }
        |  }
        |}
      """.stripMargin)
    val commandCriterion = criterion("iglu:com.hipchat.sauna.commands/unknown_command/jsonschema/1-0-0").toOption.get
    val result = Command.extractCommand[Hipchat.RoomNotification](command, commandCriterion)
    assert(result == Left(Command.ExtractionSkip))
  }
>>>>>>> e2da1da... In a command
}
