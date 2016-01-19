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
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source.fromInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor.{Actor, ActorSystem}
import akka.actor.Status.Success
import akka.testkit.TestActorRef

// amazonaws
import com.amazonaws.services.s3.model.AmazonS3Exception

// awscala
import awscala.{Credentials, Region}
import awscala.s3.S3
import awscala.sqs.SQS

// sauna
import apis.Optimizely
import loggers.Logger.{Manifestation, Notification}
import loggers._
import observers._
import processors.Processor
import processors.Processor._
import processors.optimizely._

class IntegrationTests extends FunSuite with BeforeAndAfter {
  implicit var system: ActorSystem = _
  implicit var logger: TestActorRef[Actor] = _
  var awsAccessKeyId: String = _
  var secretAccessKey: String = _
  var awsBucketName: String = _
  var awsQueueName: String = _
  var awsRegion: String = _
  var optimizelyToken: String = _
  var optimizelyImportRegion: String = _

  before {
    system = ActorSystem.create()
    logger = TestActorRef(new MutedLogger)

    // credentials
    awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
    secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
    awsBucketName = System.getenv("AWS_BUCKET_NAME")
    awsQueueName = System.getenv("AWS_QUEUE_NAME")
    awsRegion = System.getenv("AWS_REGION")
    optimizelyToken = System.getenv("OPTIMIZELY_PROJECT_TOKEN")
    optimizelyImportRegion = System.getenv("OPTIMIZELY_IMPORT_REGION")
    assert(awsAccessKeyId != null, "not found system variable 'AWS_ACCESS_KEY_ID'")
    assert(secretAccessKey != null, "not found system variable 'AWS_SECRET_ACCESS_KEY'")
    assert(awsBucketName != null, "not found system variable 'AWS_BUCKET_NAME'")
    assert(awsQueueName != null, "not found system variable 'AWS_BUCKET_NAME'")
    assert(awsRegion != null, "not found system variable 'AWS_REGION'")
    assert(optimizelyToken != null, "not found system variable 'OPTIMIZELY_PROJECT_TOKEN'")
    assert(optimizelyImportRegion != null, "not found system variable 'OPTIMIZELY_IMPORT_REGION'")
  }

  after {
    val _ = system.terminate()
  }

  test("local no processor") {
    // prepare for start, define some variables
    val saunaRoot = "/opt/sauna/"
    val fileName = UUID.randomUUID().toString
    val testFile = new File(saunaRoot + fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = null
    val processors = Seq(
      TestActorRef(new Processor {
        override def processed(fileAppeared: FileAppeared): Boolean = {
          expectedLines = fromInputStream(fileAppeared.is).getLines().toSeq
          true
        }
      })
    )
    val lo = new LocalObserver(saunaRoot, processors)

    // start observer
    new Thread(lo).start()

    // wait
    Thread.sleep(300)

    // do an action that should trigger observer
    new PrintWriter(testFile) {
      write(s"$line1\n$line2")

      close()
    }

    // wait
    Thread.sleep(300)

    // make sure everything went as expected
    assert(!testFile.exists(), "test file was not deleted")
    assert(expectedLines === Seq(line1, line2))
  }

  test("local targeting lists") {
    // prepare for start, define some variables
    val saunaRoot = "/opt/sauna"
    val source = Paths.get("src/test/resources/targeting_list.tsv")
    val destinationPath = s"$saunaRoot/com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary"
    val destinationName = "new-lists.tsv"
    val destination = Paths.get(s"$destinationPath/$destinationName")

    // cleanup
    val _ = new File(destinationPath).mkdirs()
    Files.deleteIfExists(destination)

    // this should be changed if TargetingList works properly
    var id: String = null

    // actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var error = Option.empty[String]

    // define mocked logger
    logger = TestActorRef(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new local file"
          if (!message.text.contains(expectedText)) error = Some(s"in step1, [${message.text}] does not contain [$expectedText]")
          context.become(step2)

        case message =>
          error = Some(s"in step1, got unexpected message $message")
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name [dec_ab_group]."
          if (message.text != expectedText) error = Some(s"in step2, [${message.text}] is not equal to [$expectedText]]")

        case message: Manifestation =>
          id = message.uid

        case Success => // do nothing

        case message =>
          error = Some(s"in step2, got unexpected message [$message]")
      }

      override def receive = step1
    })

    // define Optimizely, processor and observer
    val optimizely = new Optimizely(optimizelyToken)
    val processorActors = Seq(TargetingList(optimizely))
    val observers = Seq(new LocalObserver(saunaRoot, processorActors))
    observers.foreach(new Thread(_).start())

    // wait
    Thread.sleep(500)

    // do an action that should trigger observer
    Files.copy(source, destination)

    // wait, assuming 5 seconds is enough to get to Optimizely and back
    Thread.sleep(5000)

    // cleanup // todo check if really appeared
    optimizely.deleteTargetingList(id)

    // make sure everything went as expected
    assert(id != null, "id was not updated")
    assert(!destination.toFile.exists(), "processed file should have been deleted")
    assert(error.isEmpty)
  }

  test("s3 targeting lists") {
    // prepare for start, define some variables
    val source = new File("src/test/resources/targeting_list.tsv")
    val destination = "com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary/warehouse.tsv"
    implicit val region = Region(awsRegion)
    implicit val credentials = new Credentials(awsAccessKeyId, secretAccessKey)
    val s3 = S3(credentials)
    val sqs = SQS(credentials)
    val queue = sqs.queue(awsQueueName)
                   .getOrElse(throw new Exception("No queue with that name found"))

    // clean up, if object does not exist, Amazon S3 returns a success message instead of an error message
    s3.deleteObject(awsBucketName, destination)

    // this should be changed if TargetingList works properly
    var id: String = null

    // actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var error = Option.empty[String]

    // define mocked logger
    logger = TestActorRef(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new S3 file"
          if (!message.text.contains(expectedText)) error = Some(s"in step1, [${message.text}] does not contain [$expectedText]")
          context.become(step2)

        case message =>
          error = Some(s"in step1, got unexpected message $message")
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name [dec_ab_group]."
          if (!message.text.contains(expectedText)) error = Some(s"in step2, [${message.text}] does not contain [$expectedText]")

        case message: Manifestation =>
          id = message.uid

        case Success => // do nothing

        case message =>
          error = Some(s"in step2, got unexpected message [$message]")
      }

      override def receive = step1
    })

    // define Optimizely, processor and observer
    val optimizely = new Optimizely(optimizelyToken)
    val processorActors = Seq(TargetingList(optimizely))
    val observers = Seq(new S3Observer(s3, sqs, queue, processorActors))
    observers.foreach(new Thread(_).start())

    // wait
    Thread.sleep(500)

    // do an action that should trigger observer
    s3.putObject(awsBucketName, destination, source)

    // wait, assuming 5 seconds is enough to get file from AWS, and travel to Optimizely and back
    Thread.sleep(5000)

    // cleanup
    optimizely.deleteTargetingList(id)

    // make sure everything went as expected
    assert(id != null, "id was not updated")
    try {
      val _ = s3.getObject(awsBucketName, destination)
      assert(false, "processed file should have been deleted")
    } catch { case e: AmazonS3Exception if e.getMessage.contains("The specified key does not exist") =>
        // do nothing, it's 'happy path'
    }
    assert(error.isEmpty)
  }

  // todo after datasource restored
  ignore("local dynamic client profiles") {
    // prepare for start, define some variables
    val saunaRoot = "/opt/sauna"
    val source = Paths.get("src/test/resources/dynamic_client_profiles.tsv")
    val serviceId = "4034482827"
    val datasourceId = "4560617625"
    val destinationPath = s"$saunaRoot/com.optimizely.dcp/datasource/v1/$serviceId/$datasourceId/tsv:isVip,customerId,spendSegment,whenCreated/ua-team/joe"
    val destinationName = "warehouse.tsv"
    val destination = Paths.get(s"$destinationPath/$destinationName")

    // cleanup
    val _ = new File(destinationPath).mkdirs()
    Files.deleteIfExists(destination)

    // actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var error = Option.empty[String]

    // define mocked logger
    logger = TestActorRef(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new local file"
          if (!message.text.contains(expectedText)) error = Some(s"in step1, [${message.text}] does not contain [$expectedText]]")
          context.become(step2)

        case message =>
          error = Some(s"in step1, got unexpected message [$message]")
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"Successfully uploaded file to S3 bucket 'optimizely-import/dcp/$serviceId/$datasourceId"
          if (message.text != expectedText) error = Some(s"in step2, [${message.text}] is not equal to [$expectedText]]")

        case Success => // do nothing

        case message =>
          error = Some(s"in step2, got unexpected message [$message]")
      }

      override def receive = step1
    })

    // define Optimizely, processor and observer
    val optimizely = new Optimizely(optimizelyToken)
    val processorActors = Seq(DCPDatasource(optimizely, saunaRoot, optimizelyImportRegion))
    val observers = Seq(new LocalObserver(saunaRoot, processorActors))
    observers.foreach(new Thread(_).start())

    // wait
    Thread.sleep(500)

    // do an action that should trigger observer
    Files.copy(source, destination)

    // wait, assuming 5 seconds is enough to get to S3, Optimizely and back
    Thread.sleep(5000)

    // cleanup
    optimizely.deleteDcpService(datasourceId)

    // make sure everything went as expected
    assert(!destination.toFile.exists(), "processed file should have been deleted")
    assert(error.isEmpty)
  }
}