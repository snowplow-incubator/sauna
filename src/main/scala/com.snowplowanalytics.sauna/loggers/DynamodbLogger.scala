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
package loggers

// akka
import akka.actor.{ Actor, Props }

// scala
import scala.concurrent.Future

// awscala
import awscala.{ Region, Credentials }
import awscala.dynamodbv2.{ DynamoDB, Table }

// sauna
import Logger._

/**
 * Sublogger putting messages to DynamoDB table
 */
class DynamodbLogger(ddbTable: Table, ddb: DynamoDB) extends Actor {

  import context.dispatcher

  /**
   * Writes the message to DynamoDb table
   */
  def receive = {
    case message: Manifestation =>
      import message._

      // make non-blocking call
      val f = Future {
        ddbTable.put(uid, name, "status" -> status, "description" -> description, "lastModified" -> lastModified)(ddb)
      }
      f.onFailure { case e => println(s"Unable to send manifestation to DynamoDB table: [${e.toString}") }
  }
}

object DynamodbLogger {
  def props(parameters: AmazonDynamodbConfigParameters_1_0_0): Props = {
    // AWS credentials
    implicit val region = Region(parameters.awsRegion)
    val credentials = new Credentials(parameters.awsAccessKeyId, parameters.awsSecretAccessKey)

    // DynamoDB
    val ddb = DynamoDB(credentials)
    val ddbTable = ddb.table(parameters.dynamodbTableName)
      .getOrElse(throw new RuntimeException(s"No table [${parameters.dynamodbTableName}] was found"))

    Props(new DynamodbLogger(ddbTable, ddb))
  }
}