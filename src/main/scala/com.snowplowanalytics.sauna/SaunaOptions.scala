/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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

// java
import java.io.File

// scala
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.control.NonFatal

// iglu
import com.snowplowanalytics.iglu.core.SchemaKey

// avro4s
import com.sksamuel.avro4s._

// play-json
import play.api.libs.json._

// sauna
import loggers._
import observers._
import responders._

/**
 * Options parsed from command line
 *
 * @param configurationLocation root to directory with all configuration files
 */
case class SaunaOptions(configurationLocation: File) {

  import SaunaOptions._

  /**
   * Parse and extract global configuration object from `configurationLocation`
   */
  def extract: SaunaSettings =
    SaunaSettings(
      getConfig[AmazonDynamodbConfig],
      getConfig[loggers.HipchatConfig],
      getConfig[OptimizelyConfig],
      getConfig[SendgridConfig],
      getConfig[responders.HipchatConfig],
      getConfig[SlackConfig],
      getConfigs[LocalFilesystemConfig],
      getConfigs[AmazonS3Config],
      getConfigs[AmazonKinesisConfig])

  /**
   * Lazy enabledConfigs for all configurations parsed from `configurations` directory,
   * which was valid self-describing Avro
   * Key - class name, value - map of ids to content of `data` field
   * anything except observers is single-element/no-element list
   */
  private lazy val configMap: Map[String, List[Array[Byte]]] =
    buildConfigMap(getAllConfigs) match {
      case Left(error) => sys.error(error)
      case Right(enabledConfigs) =>
        enabledConfigs.mapValues { envelopes => envelopes.map(e => e.data.toString.getBytes) }
    }

  /**
   * Helper method for parsing Avro configuration
   * Gets one element (responders, loggers)
   *
   * @tparam S class of configuration with defined Avro schema
   * @return some configuration if it was parsed into configuration enabledConfigs
   */
  private[sauna] def getConfig[S: SchemaFor: FromRecord: ClassTag]: Option[S] = {
    val className = implicitly[ClassTag[S]].runtimeClass.getSimpleName
    for {
      configs <- configMap.get(className).map(_.headOption)
      config <- configs
      record <- parseConfig[S](config)
    } yield record
  }

  /**
   * Helper method for parsing Avro configuration
   * Gets multiple elements (observers)
   *
   * @tparam S class of configuration with defined Avro schema
   * @return some configuration if it was parsed into configuration enabledConfigs
   */
  private[sauna] def getConfigs[S: SchemaFor: FromRecord: ClassTag]: List[S] = {
    val className = implicitly[ClassTag[S]].runtimeClass.getSimpleName
    for {
      configs <- List(configMap.get(className))
      config <- configs.toList
      bytes <- config
      record <- parseConfig[S](bytes)
    } yield record
  }

  /**
   * Get all files from configuration directory with `json` and `avro` extensions
   */
  private def getAllConfigs: List[File] =
    configurationLocation.listFiles.toList.filter(filePredicate)

}

object SaunaOptions {

  private[sauna] val initial = SaunaOptions(new File("."))

  /**
   * Scopt CLI-parser
   */
  val parser = new scopt.OptionParser[SaunaOptions](generated.ProjectSettings.name) {
    head(generated.ProjectSettings.name, generated.ProjectSettings.version)

    opt[File]("configurations")
      .required()
      .valueName("<dir>")
      .action((x, c) => c.copy(configurationLocation = x))
      .text("path to directory with self-describing Avro JSON configurations")
      .validate(f => if (f.isDirectory && f.canRead) success else failure("responders must be a directory with read access"))
  }

  /**
   * Structure isomorphic to Iglu `SelfDescribingData`
   * JSON that contains `schema` and `data`
   *
   * @param schema string of SchemaKey
   * @param data   JSON instance with configuration
   */
  private[sauna] case class Envelope(schema: SchemaKey, data: JsValue)

  /**
   * Build configuration enabledConfigs (schema -> configuration) out of lift of files,
   * supposed to be self-describing AVRO instances
   *
   * @param files list of JSON Avro files
   * @return either valid configuration enabledConfigs or error if any encountered
   */
  def buildConfigMap(files: List[File]): Either[String, Map[String, List[Envelope]]] = {
    val enabledConfigs = sequence(files.map(parseSelfDescribing)).right.map { configs =>
      configs.filter(filterEnabled).groupBy(_.schema)
    }.left.map(list => list.mkString(", "))

    enabledConfigs.right.flatMap { map =>
      getUnique(map)
    }
  }

  /**
   * Parse file into structure representing pair of SchemaKey and whole
   * configuration
   *
   * @param file existing file, supposed to be JSON Avro instance
   * @return pair of json and data
   */
  private[sauna] def parseSelfDescribing(file: File): Either[String, Envelope] = {
    val content = Source.fromFile(file).getLines.mkString
    try {
      val json = Json.parse(content)
      val envelope = for {
        schema <- (json \ "schema").asOpt[String].flatMap(SchemaKey.fromUri)
        data <- (json \ "data").asOpt[JsObject]
      } yield Envelope(schema, data)
      envelope match {
        case Some(e) => Right(e)
        case None => Left(s"File [${file.getAbsolutePath}] doesn't contain self-describing JSON Avro")
      }
    } catch {
      case NonFatal(e) => Left(e.getMessage)
    }
  }

  /**
   * Predicate allowing to filter only files with json and avro extensions
   */
  private[sauna] def filePredicate(file: File): Boolean = {
    val fileName = file.getName
    file.isFile && (fileName.endsWith("json") || fileName.endsWith("avro"))
  }

  /**
   * Accumulate disjunctions. If at least one `Left` encountered - return `Left`,
   * if many `Left`s encountered - return accumulated
   *
   * @param eithers list of disjunctions
   * @return disjucntion of errors or successes
   */
  private[sauna] def sequence[A, B](eithers: List[Either[A, B]]): Either[List[A], List[B]] =
    eithers.foldLeft(Right(Nil): Either[List[A], List[B]]) { (acc, cur) =>
      acc match {
        case Left(errors) => cur match {
          case Right(succ) => Left(errors)
          case Left(error) => Left(error :: errors)
        }
        case Right(successes) => cur match {
          case Left(error) => Left(List(error))
          case Right(succ) => Right(succ :: successes)
        }
      }
    }

  /**
   * Filter only configurations with `enabled` flag set to true
   *
   * @param envelope self-describing configuration
   * @return true if configuration is enabled
   */
  private[sauna] def filterEnabled(envelope: Envelope): Boolean = {
    (envelope.data \ "enabled").asOpt[Boolean] match {
      case Some(enabled) => enabled
      case None => false
    }
  }

  /**
   * Ensure that only observer configurations can appear more that once
   *
   * @param enabledConfigs configuration enabledConfigs with list of possible enabled configurations
   * @return validated configuration enabledConfigs with
   */
  private[sauna] def getUnique(enabledConfigs: Map[SchemaKey, List[Envelope]]): Either[String, Map[String, List[Envelope]]] = {
    val (valid, invalid) = enabledConfigs.partition {
      case (schema, envelopes) => schema.vendor.contains(".observers") || envelopes.size == 1
    }

    val ids = valid.flatMap {
      case (schema, envelopes) => envelopes.map(e => (e.data \ "id").asOpt[String])
    }.flatten.toList

    if (ids.distinct.size != ids.size) {
      Left(s"All ids must be unique: [${ids.mkString(", ")}]")
    } else if (invalid.nonEmpty) {
      Left(s"Multiple configurations enabled: [${invalid.keys.mkString(",")}]")
    } else {
      Right(valid.map {
        case (schema, envelopes) => (schema.name, envelopes)
      })
    }
  }

  /**
   * Parse array of bytes, supposed to be Avro JSON instance configuration
   *
   * @param content Avro JSON with configuration
   * @tparam S one of configuration types with defined Avro schemas
   * @return some configuration if `content` conforms class
   */
  private[sauna] def parseConfig[S: SchemaFor: FromRecord](content: Array[Byte]): Option[S] =
    try {
      val is = AvroInputStream.json[S](content)
      val instances = is.singleEntity.toOption
      is.close()
      instances
    } catch {
      case NonFatal(e) => sys.error(s"Cannot parse configuration file [${content.toString}]. Make sure its valid JSON version of Avro instance\n${e.getMessage}")
    }
}
