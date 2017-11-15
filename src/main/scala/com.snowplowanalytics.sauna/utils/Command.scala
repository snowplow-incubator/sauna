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
import play.api.data.validation.ValidationError

// jsonschema
import com.fasterxml.jackson.databind.JsonNode
import com.github.fge.jsonschema.main.JsonSchemaFactory

// iglu
import com.snowplowanalytics.iglu.client.repositories.{HttpRepositoryRef, RepositoryRefConfig}
import com.snowplowanalytics.iglu.client.{SchemaKey, _}

object Command {

  /**
    * Sauna command
    * @see https://github.com/snowplow-incubator/sauna/wiki/Commands-for-analysts#anatomy-of-a-command
    */
  val rootCommand = SchemaCriterion("com.snowplowanalytics.sauna.commands", "command", "jsonschema", 1, 0)

  /** Sauna command envelope with aux data */
  val envelope = SchemaCriterion("com.snowplowanalytics.sauna.commands", "envelope", "jsonschema", 1, 0)

  lazy val igluCentralResolver: Resolver = Resolver(500, List(HttpRepositoryRef(RepositoryRefConfig("Iglu central", 0, List("com.snowplowanalytics")), "http://iglucentral.com")))

  /** Helper ADT supposed to make `Either` isomorphic to EitherT[Option, String, A],
    * e.g. `ExtractionSkip` is `None`
    * TODO: replace with actual `EitherT` when cats used
    */
  sealed trait ExtractionResult
  case class ExtractionError(message: String) extends ExtractionResult
  case object ExtractionSkip extends ExtractionResult

  object ExtractionResult {
    def error(message: String): ExtractionResult = ExtractionError(message)
    def error(messages: ProcessingMessageNel): ExtractionResult =
      ExtractionError(s"JSON Validation Errors: ${messages.list.mkString(", ")}")
  }

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
   * @see https://github.com/snowplow-incubator/sauna/wiki/Commands-for-analysts
   * @param json The [[JsValue]] to extract a command from.
   * @param criterion optional schema criterion that command must conform
   * @tparam T The type of the command's data.
   * @return Right containing a tuple of the command's envelope and data
   *         if the extraction and processing was successful, Left containing
   *         an error message otherwise.
   */
  def extractCommand[T: Reads](json: JsValue, criterion: SchemaCriterion): Either[ExtractionResult, (CommandEnvelope, T)] = {
    for {
      sdJson  <- json.validate[SelfDescribing].asEither.leftMap(jsonError(NotSelfDescribing))
      _       <- validateSelfDescribing(sdJson, igluCentralResolver, rootCommand)
      command <- sdJson.data.validate[SaunaCommand].asEither.leftMap(jsonError(NotCommand))
      result  <- processCommand[T](command, criterion)
    } yield result
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
  def processCommand[T: Reads](command: SaunaCommand, criterion: SchemaCriterion): Either[ExtractionResult, (CommandEnvelope, T)] = {
    for {
      _        <- validateSelfDescribing(command.envelope, igluCentralResolver, envelope)
      envelope <- command.envelope.data.validate[CommandEnvelope].asEither.leftMap(jsonError(NotEnvelope))
      _        <- validateSelfDescribing(command.command, igluCentralResolver, criterion)
      data     <- command.command.data.validate[T].asEither.leftMap(jsonError(NotData))
    } yield (envelope, data)
  }

  /**
   * Validates self-describing data against its' schema and check against criterion if necessary
   *
   * @param selfDescribing The self-describing data to validate.
   * @param igluResolver   [[Resolver]] containing info about Iglu repositories.
   * @return Right if the data was successfully validated against the schema,
   *         Left with an error message otherwise.
   */
  def validateSelfDescribing(selfDescribing: SelfDescribing, igluResolver: Resolver, criterion: SchemaCriterion): Either[ExtractionResult, Unit] = {
    for {
      _            <- matchCriterion(criterion, selfDescribing.schema)
      schema       <- igluResolver.lookupSchema(selfDescribing.schema).toEither.leftMap(ExtractionResult.error)
      jsonNodeData <- Json.fromJson[JsonNode](selfDescribing.data).asEither.leftMap(jsonError(NotJson))
      jsonSchema    = JsonSchemaFactory.byDefault().getJsonSchema(schema)
      processingReport = jsonSchema.validate(jsonNodeData)
      result       <- if (processingReport.isSuccess) Right(()) else Left(ExtractionError(processingReport.toString))
    } yield result
  }

  /**
   * Validates a Sauna command envelope.
   *
   * @param envelope A Sauna command envelope.
   * @return None if the envelope was successfully validated
   *         and the command's data can be executed,
   *         Some containing an error message otherwise.
   */
  def validateEnvelope(envelope: CommandEnvelope): Either[ExtractionResult, Unit] = {
    envelope.execution.timeToLive match {
      case Some(ms) =>
        val commandLife = envelope.whenCreated.until(LocalDateTime.now(), ChronoUnit.MILLIS)
        if (commandLife > ms)
          Left(ExtractionError("Command has expired: time to live is $ms but $commandLife has passed"))
        else
          Right(())
      case None => Right(())
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

  sealed trait ExtractionFailure
  case object NotData extends ExtractionFailure             // root.data does not conform T
  case object NotJson extends ExtractionFailure             // internal JSON error
  case object NotCommand extends ExtractionFailure
  case object NotEnvelope extends ExtractionFailure
  case object NotSelfDescribing extends ExtractionFailure

  /** Transform JSON-specific error into human-readable message */
  private def jsonError(failure: ExtractionFailure)(cause: Seq[(JsPath, Seq[ValidationError])]): ExtractionResult = {
    val error = Json.stringify(JsError.toJson(cause))
    val message = failure match {
      case NotData => s"Encountered an issue while parsing Sauna command data: $error"
      case NotJson => s"JSON-parsing problem: $error"
      case NotCommand => s"Encountered an issue while parsing Sauna command: $error"
      case NotEnvelope => s"Encountered an issue while parsing Sauna command envelope: $error"
      case NotSelfDescribing => s"Encountered an issue while parsing self-describing JSON: $error"
    }
    ExtractionError(message)
  }
}
