/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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

// sauna
import loggers._
import observers._
import responders._

/**
 * Global settings-object, required to run whole application,
 * aggregating Avro-generated configuration classes for each entity
 *
 * @param amazonDynamodbConfig   optional DynamoDB logger configuration
 * @param hipchatLoggerConfig    optional Hipchat logger configuration
 * @param optimizelyConfig       optional Optimizely responder configuration
 * @param sendgridConfig         optional Sendgrid responder configuration
 * @param hipchatResponderConfig optional Hipchat responder configuration
 * @param localFilesystemConfigs list of local roots to observe
 * @param amazonS3Configs        list of S3 buckets to observe
 * @param amazonKinesisConfigs   list of Kinesis streams to observe
 */
case class SaunaSettings(
  // Loggers
  amazonDynamodbConfig: Option[AmazonDynamodbConfig],
  hipchatLoggerConfig: Option[loggers.HipchatConfig],

  // Responders
  optimizelyConfig: Option[OptimizelyConfig],
  sendgridConfig: Option[SendgridConfig],
  hipchatResponderConfig: Option[responders.HipchatConfig],

  // Observers
  localFilesystemConfigs: List[LocalFilesystemConfig],
  amazonS3Configs: List[AmazonS3Config],
  amazonKinesisConfigs: List[AmazonKinesisConfig])
