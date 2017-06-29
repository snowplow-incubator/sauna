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

/**
 * Global settings-object, required to run whole application,
 * aggregating Avro-generated configuration classes for each entity
 *
 * @param amazonDynamodbConfig   optional DynamoDB logger configuration
 * @param hipchatLoggerConfig    optional Hipchat logger configuration
 * @param optimizelyConfig       optional Optimizely responders configuration
 * @param sendgridConfig_1_0_0   optional Sendgrid responder configuration (schema ver. 1-0-0)
 * @param sendgridConfig_1_0_1   optional Sendgrid responder configuration (schema ver. 1-0-1)
 * @param hipchatResponderConfig optional Hipchat responder configuration
 * @param slackConfig            optional Slack responder configuration
 * @param pagerDutyConfig        optional PagerDuty responder configuration
 * @param localFilesystemConfigs list of local roots to observe
 * @param amazonS3Configs        list of S3 buckets to observe
 * @param amazonKinesisConfigs   list of Kinesis streams to observe
 */
case class SaunaSettings(
  // Loggers
  amazonDynamodbConfig: Option[loggers.AmazonDynamodbConfig_1_0_0],
  hipchatLoggerConfig: Option[loggers.HipchatConfig_1_0_0],

  // Responders
  optimizelyConfig: Option[responders.OptimizelyConfig_1_0_0],
  sendgridConfig_1_0_0: Option[responders.SendgridConfig_1_0_0],
  sendgridConfig_1_0_1: Option[responders.SendgridConfig_1_0_1],
  hipchatResponderConfig: Option[responders.HipchatConfig_1_0_0],
  slackConfig: Option[responders.SlackConfig_1_0_0],
  pagerDutyConfig: Option[responders.PagerDutyConfig_1_0_0],

  // Observers
  localFilesystemConfigs: List[observers.LocalFilesystemConfig_1_0_0],
  amazonS3Configs: List[observers.AmazonS3Config_1_0_0],
  amazonKinesisConfigs: List[observers.AmazonKinesisConfig_1_0_0])

object SaunaSettings {
  def apply(): SaunaSettings = SaunaSettings(None, None, None, None, None, None, None, None, Nil, Nil, Nil)
}
