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

// play
import play.api.libs.json.JsValue
import play.api.libs.ws.{WSClient, WSResponse}
import play.api.libs.ws.ning.NingWSClient

// scala-csv
import com.github.tototoshi.csv.TSVFormat

// scala
import util.{ Try, Success, Failure }

/**
 * Shares common variables in single instance.
 */
package object utils {
  object tsvFormat extends TSVFormat  // forces scala-csv to use tsv

  val wsClient: WSClient = NingWSClient()

  /**
   * Extract `JsValue` from HTTP response without throwing exceptions
   */
  def safeJsonExtract(response: WSResponse): Either[Throwable, JsValue] = {
    Try(response.json) match {
      case Success(json) => Right(json)
      case Failure(error) => Left(error)
    }
  }

  implicit class EitherFlatMap[L, R](val either: Either[L, R]) extends AnyVal {
    def flatMap[RR](f: R => Either[L, RR]): Either[L, RR] = either match {
      case Right(r) => f(r)
      case Left(l) => Left(l)
    }

    def map[RR](f: R => RR): Either[L, RR] = either match {
      case Right(r) => Right(f(r))
      case Left(l) => Left(l)
    }

    def leftMap[LL](f: L => LL): Either[LL, R] = either match {
      case Left(l) => Left(f(l))
      case Right(r) => Right(r)
    }
  }
}
