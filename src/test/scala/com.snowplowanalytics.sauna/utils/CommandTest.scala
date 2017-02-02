/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
import apis.Hipchat.RoomNotification

class CommandTest extends FunSuite with BeforeAndAfter with EitherValues with OptionValues {
  test("extractCommand invalid JSON") {
    val command = Json.parse(""""{}""""")
    val result = Command.extractCommand[RoomNotification](command)
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
    val result = Command.extractCommand[RoomNotification](command)
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
    val result = Command.extractCommand[RoomNotification](command)
    assert(result.isLeft)
  }

  test("extractCommand valid JSON") {
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
    val result = Command.extractCommand[RoomNotification](command)
    assert(result.right.value._1 ===
      CommandEnvelope(
        "9dadfc92-9311-43c7-9cee-61ab590a6e81",
        LocalDateTime.parse("2017-01-02T19:14:42"),
        ExecutionParams(
          AT_LEAST_ONCE,
          Some(1200000)
        ),
        Map()
      )
    )
    assert(result.right.value._2 ===
      RoomNotification(
        "Devops",
        "yellow",
        "HipChat is awesome!",
        true,
        "text"
      )
    )
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
    val result = Command.processEnvelope(envelope)
    assert(result.value.contains("Command has expired"))
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
    val result = Command.processEnvelope(envelope)
    assert(result.isEmpty)
  }
}
