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
package observers

// scalatest
import org.scalatest._

// play
import play.api.libs.json.Json

class S3MonitorTest extends FunSuite {
  test("getBucketAndFile invalid json") {
    val json = Json.parse(""" {"qwerty":"123"} """)

    assert(AmazonS3Observer.extractBucketAndFile(json) === None)
  }

  test("getBucketAndFile valid json") {
    val json = Json.parse("""
      |{
      |  "Records": [
      |    {
      |      "eventVersion": "2.0",
      |      "eventSource": "aws:s3",
      |      "awsRegion": "us-west-2",
      |      "eventTime": "2015-12-04T11:58:37.704Z",
      |      "eventName": "ObjectCreated:Put",
      |      "userIdentity": {
      |        "principalId": "AWS:AIDAIABCCBSEGVKJMPHLK"
      |      },
      |      "requestParameters": {
      |        "sourceIPAddress": "93.72.40.18"
      |      },
      |      "responseElements": {
      |        "x-amz-request-id": "5B0B2CD00ECB7103",
      |        "x-amz-id-2": "Yx2QvTmc\/Qs\/ohRrZJB4h8KjieK0E\/bBrZ4o\/PSW2OJT01mzg5qwbZZyH\/7CGQ0d"
      |      },
      |      "s3": {
      |        "s3SchemaVersion": "1.0",
      |        "configurationId": "sqsQueueConfig",
      |        "bucket": {
      |          "name": "snowplow-sauna-sandbox3",
      |          "ownerIdentity": {
      |            "principalId": "A1N2UL99VJQH3T"
      |          },
      |          "arn": "arn:aws:s3:::snowplow-sauna-sandbox3"
      |        },
      |        "object": {
      |          "key": "test2",
      |          "size": 13,
      |          "eTag": "cd76763d5aebcbc53a8a1f3211b356e7",
      |          "sequencer": "0056617FEDAC3F1C7F"
      |        }
      |      }
      |    }
      |  ]
      |}
    """.stripMargin)

    assert(AmazonS3Observer.extractBucketAndFile(json) === Some(("snowplow-sauna-sandbox3", "test2")))
  }

  test("getBucketAndFile partially valid json") {
    val json = Json.parse("""
      |{
      |  "Records": [
      |    {
      |      "eventVersion": "2.0",
      |      "eventSource": "aws:s3",
      |      "awsRegion": "us-west-2",
      |      "eventTime": "2015-12-04T11:58:37.704Z",
      |      "eventName": "ObjectCreated:Put",
      |      "userIdentity": {
      |        "principalId": "AWS:AIDAIABCCBSEGVKJMPHLK"
      |      },
      |      "requestParameters": {
      |        "sourceIPAddress": "93.72.40.18"
      |      },
      |      "responseElements": {
      |        "x-amz-request-id": "5B0B2CD00ECB7103",
      |        "x-amz-id-2": "Yx2QvTmc\/Qs\/ohRrZJB4h8KjieK0E\/bBrZ4o\/PSW2OJT01mzg5qwbZZyH\/7CGQ0d"
      |      },
      |      "s3": {
      |        "s3SchemaVersion": "1.0",
      |        "configurationId": "sqsQueueConfig",
      |        "bucket": {
      |          "name": "snowplow-sauna-sandbox3",
      |          "ownerIdentity": {
      |            "principalId": "A1N2UL99VJQH3T"
      |          },
      |          "arn": "arn:aws:s3:::snowplow-sauna-sandbox3"
      |        },
      |        "object": {
      |          "size": 13,
      |          "eTag": "cd76763d5aebcbc53a8a1f3211b356e7",
      |          "sequencer": "0056617FEDAC3F1C7F"
      |        }
      |      }
      |    }
      |  ]
      |}
    """.stripMargin)

    assert(AmazonS3Observer.extractBucketAndFile(json) === None)
  }
}