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
import java.time.temporal.ChronoUnit

// scalaz
import scalaz.{Failure, Success}

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._

// jsonschema
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory

// iglu
import com.snowplowanalytics.iglu.client.repositories.{HttpRepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.iglu.client.{SchemaKey, _}

object Command {
  /**
   * Self-describing data.
   *
   * @param schema The schema the data should be validated against.
   * @param data   The data itself.
   */
  case class SelfDescribing(
    schema: SchemaKey,
    data: JsValue
  )

  /**
   * A Sauna command.
   *
   * @param envelope Identifies the command and provides instructions on how to execute it.
   * @param command  Specifies the exact action to execute.
   */
  case class SaunaCommand(
    envelope: SelfDescribing,
    command: SelfDescribing)

  implicit val schemaKeyReads: Reads[SchemaKey] = Reads {
    case JsString(s) => SchemaKey.parse(s) match {
      case Success(key) => JsSuccess(key)
      case Failure(error) => JsError(error.toString)
    }
    case _ => JsError("Non-string SchemaKey")
  }

  implicit val selfDescribingDataReads: Reads[SelfDescribing] = (
    (JsPath \ "schema").read[SchemaKey] and
      (JsPath \ "data").read[JsValue]
    ) (SelfDescribing.apply(_, _))

  implicit val saunaCommandReads: Reads[SaunaCommand] = (
    (JsPath \ "envelope").read[SelfDescribing] and
      (JsPath \ "command").read[SelfDescribing]
    ) (SaunaCommand.apply _)

  sealed trait Semantics
  case object AT_LEAST_ONCE extends Semantics
  object Semantics {
    val values = List(AT_LEAST_ONCE)
  }

  implicit val semanticsReads: Reads[Semantics] = Reads {
    case JsString(s) => Semantics.values.find(value => value.toString == s) match {
      case Some(value) => JsSuccess(value)
      case None => JsError(s"Invalid Semantics: $s")
    }
    case _ => JsError("Non-string Semantics")
  }

  /**
   * TODO: define what this is
   *
   * @param semantics  TODO: define what this is
   * @param timeToLive The command's lifetime (in ms): if a command is
   *                   received after `timeToLive` ms from its' creation have
   *                   passed, it will be discarded. (If None, a command won't be
   *                   discarded based on creation time.)
   */
  case class ExecutionParams(
    semantics: Semantics,
    timeToLive: Option[Int]
  )

  implicit val timeToLiveReads: Reads[Option[Int]] = JsPath.readNullable[Int]
  implicit val executionParamsReads: Reads[ExecutionParams] = Json.reads[ExecutionParams]

  /**
   * A Sauna command envelope.
   *
   * @param commandId   A unique identifier (uuid4) for the command.
   * @param whenCreated The command's creation date.
   * @param execution   TODO: define what this is
   * @param tags        TODO: define what this is
   */
  case class CommandEnvelope(
    commandId: String,
    whenCreated: LocalDateTime,
    execution: ExecutionParams,
    tags: Map[String, String]
  )
  implicit val commandEnvelopeReads: Reads[CommandEnvelope] = Json.reads[CommandEnvelope]

  /**
   * Attempts to extract a Sauna command from a [[JsValue]].
   *
   * @param json The [[JsValue]] to extract a command from.
   * @tparam T The type of the command's data.
   * @return Right containing a tuple of the command's envelope and data
   *         if the extraction was successful, Left containing an error message
   *         otherwise.
   */
  def extractCommand[T](json: JsValue)(implicit tReads: Reads[T]): Either[String, (CommandEnvelope, T)] = {
    json.validate[SelfDescribing] match {
      case JsSuccess(selfDescribing, _) =>
        val validateRoot = validateSelfDescribing(selfDescribing)
        if (validateRoot.isDefined)
          Left(s"Could not validate command JSON: ${validateRoot.get}")
        else
          selfDescribing.data.validate[SaunaCommand] match {
            case JsSuccess(command, _) =>
              val validateEnvelope = validateSelfDescribing(command.envelope)
              if (validateEnvelope.isDefined)
                Left(s"Could not validate command envelope JSON: ${validateEnvelope.get}")
              else
                command.envelope.data.validate[CommandEnvelope] match {
                  case JsSuccess(envelope, _) =>
                    val validateCommand = validateSelfDescribing(command.command)
                    if (validateCommand.isDefined)
                      Left(s"Could not validate command data JSON: ${validateCommand.get}")
                    else
                      command.command.data.validate[T] match {
                        case JsSuccess(data, _) =>
                          Right((envelope, data))
                        case JsError(error) =>
                          Left(s"Encountered an issue while parsing Sauna command data: $error")
                      }
                  case JsError(error) =>
                    Left(s"Encountered an issue while parsing Sauna command envelope: $error")
                }
            case JsError(error) =>
              Left(s"Encountered an issue while parsing Sauna command: $error")
          }
      case JsError(error) =>
        Left(s"Encountered an issue while parsing self-describing JSON: $error")
    }
  }

  /**
   * Validates self-describing data against its' schema.
   *
   * @param selfDescribing The self-describing data to validate.
   * @return None if the data was successfully validated against the schema,
   *         Some with an error message otherwise.
   */
  def validateSelfDescribing(selfDescribing: SelfDescribing): Option[String] = {
    val igluCentral = RepositoryRefConfig("Iglu central", 0, List("com.snowplowanalytics"))
    val httpRepository = HttpRepositoryRef(igluCentral, "http://iglucentral.com")
    val resolver = Resolver(500, List(httpRepository))
    resolver.lookupSchema(selfDescribing.schema) match {
      case Success(schema) =>
        Json.fromJson[JsonNode](selfDescribing.data) match {
          case JsSuccess(jsonNodeData, _) =>
            val jsonSchema = JsonSchemaFactory.byDefault().getJsonSchema(schema)
            val processingReport = jsonSchema.validate(jsonNodeData)
            if (processingReport.isSuccess) {
              None
            } else {
              Some(processingReport.toString)
            }
          case JsError(error) => Some(error.toString())
        }
      case Failure(error) => Some(error.toString())
    }
  }

  /**
   * Processes a Sauna command envelope.
   *
   * @param envelope A Sauna command envelope.
   * @return None if the envelope was successfully processed
   *         and the command's data can be executed,
   *         Some containing an error message otherwise.
   */
  def processEnvelope(envelope: CommandEnvelope): Option[String] = {
    for {
      ms <- envelope.execution.timeToLive
      commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
      if commandLife > ms
    } yield s"Command has expired: time to live is $ms but $commandLife has passed"
  }
}