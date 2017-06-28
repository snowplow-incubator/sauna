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
import java.time.temporal.ChronoUnit
import java.io.InputStream

// scala
import scala.io.Source
import scala.util.control.NonFatal


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

  lazy val igluCentralResolver: Resolver = Resolver(500, List(HttpRepositoryRef(RepositoryRefConfig("Iglu central", 0, List("com.snowplowanalytics")), "http://iglucentral.com")))

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
    ) (SelfDescribing.apply _)

  implicit val saunaCommandReads: Reads[SaunaCommand] = (
    (JsPath \ "envelope").read[SelfDescribing] and
      (JsPath \ "command").read[SelfDescribing]
    ) (SaunaCommand.apply _)

  sealed trait Semantics

  case object AT_LEAST_ONCE extends Semantics

  implicit val semanticsReads: Reads[Semantics] = Reads {
    case JsString(s) => s match {
      case "AT_LEAST_ONCE" => JsSuccess(AT_LEAST_ONCE)
      case _ => JsError(s"Invalid Semantics: $s")
    }
    case _ => JsError("Non-string Semantics")
  }

  /**
   * Parameters specifying a command's execution.
   *
   * @param semantics  Controls how the command should be executed; currently the
   *                   only option here is AT_LEAST_ONCE, meaning that the command
   *                   will be actioned at least one time (but potentially more)
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
   * @param execution   Parameters specifying a command's execution.
   * @param tags        A way of attaching additional freeform metadata to a given command.
   */
  case class CommandEnvelope(
    commandId: String,
    whenCreated: LocalDateTime,
    execution: ExecutionParams,
    tags: Map[String, String]
  )

  implicit val commandEnvelopeReads: Reads[CommandEnvelope] = Json.reads[CommandEnvelope]

  /**
   * Attempts to extract a Sauna command from a [[JsValue]]. If successful,
   * passes it on to the processing method.
   *
   * @param json         The [[JsValue]] to extract a command from.
   * @tparam T The type of the command's data.
   * @return Right containing a tuple of the command's envelope and data
   *         if the extraction and processing was successful, Left containing
   *         an error message otherwise.
   */
  def extractCommand[T](json: JsValue)(implicit tReads: Reads[T]): Either[String, (CommandEnvelope, T)] = {
    json.validate[SelfDescribing] match {
      case JsSuccess(selfDescribing, _) =>
        validateSelfDescribing(selfDescribing, igluCentralResolver) match {
          case Right(_) =>
            selfDescribing.data.validate[SaunaCommand] match {
              case JsSuccess(command, _) => processCommand[T](command)
              case JsError(error) => Left(s"Encountered an issue while parsing Sauna command: $error")
            }
          case Left(error) => Left(s"Could not validate command JSON: $error")
        }
      case JsError(error) => Left(s"Encountered an issue while parsing self-describing JSON: $error")
    }
  }

  /**
   * Processes a Sauna command, validating its' envelope and extracting the data.
   *
   * @param command      A Sauna command.
   * @tparam T The type of the command's data.
   * @return Right containing a tuple of the command's envelope and data
   *         if the processing was successful, Left containing
   *         an error message otherwise.
   */
  def processCommand[T](command: SaunaCommand)(implicit tReads: Reads[T]): Either[String, (CommandEnvelope, T)] = {
    validateSelfDescribing(command.envelope, igluCentralResolver) match {
      case Right(_) =>
        command.envelope.data.validate[CommandEnvelope] match {
          case JsSuccess(envelope, _) =>
            validateSelfDescribing(command.command, igluCentralResolver) match {
              case Right(_) =>
                command.command.data.validate[T] match {
                  case JsSuccess(data, _) => Right((envelope, data))
                  case JsError(error) => Left(s"Encountered an issue while parsing Sauna command data: $error")
                }
              case Left(error) => Left(s"Could not validate command data JSON: $error")
            }
          case JsError(error) => Left(s"Encountered an issue while parsing Sauna command envelope: $error")
        }
      case Left(error) => Left(s"Could not validate command envelope JSON: $error")
    }
  }

  /**
   * Validates self-describing data against its' schema.
   *
   * @param selfDescribing The self-describing data to validate.
   * @param igluResolver   [[Resolver]] containing info about Iglu repositories.
   * @return Right if the data was successfully validated against the schema,
   *         Left with an error message otherwise.
   */
  def validateSelfDescribing(selfDescribing: SelfDescribing, igluResolver: Resolver): Either[String, Unit] =
    igluResolver.lookupSchema(selfDescribing.schema) match {
      case Success(schema) =>
        Json.fromJson[JsonNode](selfDescribing.data) match {
          case JsSuccess(jsonNodeData, _) =>
            val jsonSchema = JsonSchemaFactory.byDefault().getJsonSchema(schema)
            val processingReport = jsonSchema.validate(jsonNodeData)
            if (processingReport.isSuccess) {
              Right(())
            } else {
              Left(processingReport.toString)
            }
          case JsError(error) => Left(error.toString())
        }
      case Failure(error) => Left(error.toString())
    }

  /**
   * Validates a Sauna command envelope.
   *
   * @param envelope A Sauna command envelope.
   * @return None if the envelope was successfully validated
   *         and the command's data can be executed,
   *         Some containing an error message otherwise.
   */
  def validateEnvelope(envelope: CommandEnvelope): Either[String, Unit] = {
    envelope.execution.timeToLive match {
      case Some(ms) =>
        val commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
        if (commandLife > ms)
          Left(s"Command has expired: time to live is $ms but $commandLife has passed")
        else
          Right()
      case None => Right()
    }
  }

  /** Parse InputStream as JSON, notifying Mediator on error */
  def parseJson(is: InputStream): Either[ExtractionResult, JsValue] =
    try {
      // Could be parsed in constant memory with Iteratee
      Right(Json.parse(Source.fromInputStream(is).mkString))
    } catch {
      case NonFatal(error) => Left(Command.ExtractionError(error.getMessage))
    }

  /**
    * Helper function to check data against optional schema criterion
    * Re-implementation due https://github.com/snowplow/iglu-scala-client/issues/68
    */
  private def matchCriterion(criterion: SchemaCriterion, key: SchemaKey): Either[ExtractionResult, Unit] =
    if (key.vendor == criterion.vendor && key.name == criterion.name && key.format == criterion.format) {
      key.getModelRevisionAddition match {
        case Some((keyModel, keyRevision, keyAddition)) if keyModel == criterion.model =>
          criterion.revision match {
            case None => criterion.addition match {
              case Some(a) if a == keyAddition => Right(())
              case None => Right(())
              case _ => Left(ExtractionSkip)
            }
            case Some(r) => criterion.addition match {
              case None if keyRevision == r => Right(())
              case Some(a) if a == keyAddition && r == keyRevision => Right(())
              case _ => Left(ExtractionSkip)
            }
          }
        case None => Left(ExtractionError(s"Invalid SchemaVer [${key.version}]"))
        case _ => Left(ExtractionSkip)
      }
    } else Left(ExtractionSkip)
>>>>>>> e2da1da... In a command
}
