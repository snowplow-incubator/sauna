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
import java.io.ByteArrayInputStream
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.UUID

// scala
import scala.io.Source.fromInputStream

// scalatest
import org.scalatest._

// akka
import akka.actor._

// amazonaws
import com.amazonaws.services.s3.model.AmazonS3Exception

// awscala
import awscala.{Credentials, Region}
import awscala.s3.S3
import awscala.sqs.SQS

// sauna
import actors.Mediator
import apis.{ Optimizely, Sendgrid }
import loggers.Logger
import loggers.Logger.{Manifestation, Notification}
import observers._
import observers.Observer.{ ObserverBatchEvent, LocalFilePublished }
import responders.Responder
import responders.Responder.{ResponderEvent, ResponderResult}
import responders.sendgrid._
import responders.optimizely._
import IntegrationTests.AnyEvent


object IntegrationTests {
  /**
   * Supervisor actor
   */
  class RootActor(respondersProps: List[Props], observerProps: Props, override val logger: ActorRef) extends Mediator(SaunaSettings(None, None, None, None, Nil, Nil)) {
    override val observers = List(context.actorOf(observerProps))
    override val responderActors = respondersProps.map(p => context.actorOf(p))
  }

  case class AnyEvent(source: ObserverBatchEvent) extends Responder.ResponderEvent[ObserverBatchEvent]

  val filePath = "some-non-existing-file-123/opt/sauna/com.sendgrid.contactdb/recipients/v1/tsv:email,birthday,middle_name,favorite_number,when_promoted/ua-team/joe/warehouse.tsv"
  class MockLocalFilePublished(data: String, observer: ActorRef) extends LocalFilePublished(java.nio.file.Paths.get(filePath), observer) {
    override def streamContent = Some(new ByteArrayInputStream(data.getBytes("UTF-8")))
  }

  /**
   * Dummy responder ignoring all observer event
   */
  class DummyResponder(val logger: ActorRef) extends Responder[AnyEvent] {
    def extractEvent(observerEvent: ObserverBatchEvent): Option[AnyEvent] = None
    def process(observerEvent: AnyEvent) = ???
  }
}

/**
 * Test whole system using real-world resources
 * WARNING: if some test failed it could probably leave resources not cleaned,
 * which means you may probably need to do cleanup (S3 bucket, Optimizely lists) manually
 */
class IntegrationTests extends FunSuite with BeforeAndAfter {

  implicit var system: ActorSystem = _

  val optimizelyToken: Option[String] = sys.env.get("OPTIMIZELY_TOKEN")
  val optimizelyProjectId = sys.env.getOrElse("OPTIMIZELY_PROJECT_ID", "4034532101")
  val optimizelyImportRegion = sys.env.getOrElse("OPTIMIZELY_IMPORT_REGION", "us-east-1")
  val optimizelyServiceId = sys.env.getOrElse("OPTIMIZELY_SERVICE_ID", "4034482827")
  val optimizelyDatasourceId = sys.env.getOrElse("OPTIMIZELY_DATASOURCE_ID", "7082200340")

  val awsAccessKeyId: Option[String] = sys.env.get("AWS_ACCESS_KEY_ID")
  val secretAccessKey: Option[String] = sys.env.get("AWS_SECRET_ACCESS_KEY")
  val awsBucketName = sys.env.getOrElse("AWS_BUCKET_NAME", "sauna-integration-test-bucket-staging")
  val awsQueueName = sys.env.getOrElse("AWS_QUEUE_NAME", "sauna-integration-test-queue-staging")
  val awsRegion = sys.env.getOrElse("AWS_REGION", "us-east-1")

  val sendgridToken: Option[String] = sys.env.get("SENDGRID_API_KEY_ID")

  val saunaRoot = System.getProperty("java.io.tmpdir", "/tmp") + "/saunaRoot"

  before {
    val root = new File(saunaRoot)
    root.delete()
    system = ActorSystem("IntegrationTest")
    root.mkdir()
  }

  after {
    system.terminate()
  }

  test("local no responder") {
    // prepare for start, define some variables
    val fileName = UUID.randomUUID().toString
    val testFile = new File(saunaRoot, fileName)
    val line1 = "aaaaaa"
    val line2 = "bbbbbb"
    var expectedLines: Seq[String] = Seq()

    val responders =
      Props(new Responder[AnyEvent] {
        val logger: ActorRef = null

        def extractEvent(event: ObserverBatchEvent): Option[AnyEvent] = {
          Some(AnyEvent(event))
        }

        val pathPattern: String = ".*"

        def process(event: AnyEvent): Unit = {
          expectedLines = fromInputStream(event.source.streamContent.get).getLines().toSeq
          self ! new ResponderResult {
            override def source: ResponderEvent[ObserverBatchEvent] = event
            override def message: String = "OK!"
          }
        }
      })

    val dummyLogger = system.actorOf(Props(new Actor {
      def receive = {
        case _ => ()
      }
    }))

    val localObserver = Props(new LocalObserver(Paths.get(saunaRoot)))
    val _ = system.actorOf(Props(new IntegrationTests.RootActor(List(responders), localObserver, dummyLogger)))

    // wait
    Thread.sleep(2000)

    // do an action that should trigger observer
    new PrintWriter(testFile) {
      write(s"$line1\n$line2")
      close()
    }

    // wait
    Thread.sleep(15000)

    // make sure everything went as expected
    assert(!testFile.exists(), "test file was not deleted")
    assert(expectedLines === Seq(line1, line2))
  }

  test("local targeting lists") {
    assume(optimizelyToken.isDefined)

    // Prepare for start
    val source = Paths.get("src/test/resources/targeting_list.tsv")
    val destinationPath = s"$saunaRoot/com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary"
    val destinationName = "new-lists.tsv"
    val destination = Paths.get(s"$destinationPath/$destinationName")

    // Cleanup
    new File(destinationPath).mkdirs()
    Files.deleteIfExists(destination)

    // This should be changed if TargetingListResponder works properly
    var id: String = null

    // Actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var error: String = null

    // Mocked logger
    val loggerProps = Props(new Actor {

      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new local file"
          if (!message.text.contains(expectedText))
            error = s"In step1, [${message.text}] does not contain [$expectedText]"
          context.become(step2)

        case message =>
          error = s"In step1, got unexpected message $message"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name"
          if (!message.text.startsWith(expectedText))
            error = s"In step2, [${message.text}] does not start with [$expectedText]"
          context.become(step3)

        case message: Manifestation =>
          id = message.uid

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      def step3: Receive = {
        case message: Notification =>
          val expectedText = "new-lists.tsv has been successfully published"
          if (!message.text.endsWith(expectedText))
            error = s"In step3, [${message.text}] does not end with [$expectedText]"
          context.become(step4)

        case message =>
          error = s"in step3, got unexpected message [$message]"
      }

      def step4: Receive = {
        case message: Notification =>
          val expectedText = "All actors finished processing message [/tmp/saunaRoot/com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary/new-lists.tsv]. Deleting"
          if (message.text != expectedText)
            error = s"In step4, [${message.text}] isn't equal to [$expectedText]"

        case message =>
          error = s"in step3, got unexpected message [$message]"
      }

      override def receive = step1
    })

    val logger = system.actorOf(loggerProps)
    val apiWrapper = new Optimizely(optimizelyToken.get, logger)
    val responderProps = TargetingListResponder.props(apiWrapper, logger)
    val localObserver = Props(new LocalObserver(Paths.get(saunaRoot)))

    // Cleanup
    apiWrapper.deleteTargetingList(optimizelyProjectId, "dec_ab_group")

    system.actorOf(Props(new IntegrationTests.RootActor(List(responderProps), localObserver, logger)))  // TODO: try different names

    // wait
    Thread.sleep(2000)

    // do an action that should trigger observer
    Files.copy(source, destination)

    // wait, assuming 7 seconds is enough to get to Optimizely and back
    Thread.sleep(7000)

    // Cleanup
    apiWrapper.deleteTargetingList(id)

    // make sure everything went as expected
    assert(id != null, "id was not updated")
    assert(error == null)
    assert(!destination.toFile.exists(), "processed file should have been deleted")
  }

  test("s3 targeting lists") {
    assume(optimizelyToken.isDefined)
    assume(awsAccessKeyId.isDefined)
    assume(secretAccessKey.isDefined)

    // prepare for start, define some variables
    val source = new File("src/test/resources/targeting_list.tsv")
    val destination = "com.optimizely/targeting_lists/v1/tsv:*/marketing-team/mary/warehouse.tsv"

    implicit val region = Region(awsRegion)
    implicit val credentials = new Credentials(awsAccessKeyId.get, secretAccessKey.get)
    val s3 = S3(credentials)
    val sqs = SQS(credentials)
    val queue = sqs.queue(awsQueueName)
                   .getOrElse(throw new Exception("No queue with that name found"))

    // clean up, if object does not exist, Amazon S3 returns a success message instead of an error message
    s3.deleteObject(awsBucketName, destination)

    // this should be changed if TargetingListResponder works properly
    var id: String = null

    // Actors (if executed in another thread) silences error
    // approach with testing 'receive' is also impossible,
    // because this test should go as close to real one as possible
    // so, let's introduce a variable that will be assigned if something goes wrong
    var error: String = null

    // define mocked logger
    val loggerProps = Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new S3 file"
          if (!message.text.contains(expectedText)) {
            error = s"In step1, [${message.text}] does not contain [$expectedText]"
          } else { context.become(step2) }

        case message =>
          error = s"In step1, got unexpected message $message"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step2, [${message.text}] does not start with [$expectedText]"
          } else { context.become(step3) }

        case message: Manifestation =>
          id = message.uid

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      def step3: Receive = {
        case message: Notification =>
          val expectedText = "warehouse.tsv has been successfully published"
          if (!message.text.endsWith(expectedText)) {
            error = s"In step3, [${message.text}] does not end with [$expectedText]"
          } else { context.become(step4) }

        case message =>
          error = s"in step3, got unexpected message [$message]"
      }

      def step4: Receive = {
        case message: Notification =>
          val expectedText = "All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step4, [${message.text}] does not start with [$expectedText]"
          }

        case message =>
          error = s"in step4, got unexpected message [$message]"
      }

      override def receive = step1
    })

    // define Optimizely, responder and observer
    val logger = system.actorOf(loggerProps)

    val apiWrapper = new Optimizely(optimizelyToken.get, logger)
    val responderProps = TargetingListResponder.props(apiWrapper, logger)
    val s3Observer = AmazonS3Observer.props(s3, sqs, queue)

    // Cleanup
    apiWrapper.deleteTargetingList(optimizelyProjectId, "dec_ab_group")

    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responderProps), s3Observer, logger)))

    // wait
    Thread.sleep(500)

    // do an action that should trigger observer
    s3.putObject(awsBucketName, destination, source)

    // wait, assuming 10 seconds is enough to get file from AWS, and travel to Optimizely and back
    Thread.sleep(10000)

    // Cleanup
    apiWrapper.deleteTargetingList(optimizelyProjectId, "dec_ab_group")
    root ! PoisonPill
    Thread.sleep(1000)

    // make sure everything went as expected
    assert(id != null, "id was not updated")
    assert(error == null)
    try {
      s3.getObject(awsBucketName, destination)
      assert(false, "processed file should have been deleted")
    } catch {
      case e: AmazonS3Exception if e.getMessage.contains("The specified key does not exist") =>
        // do nothing, it was expected exception
    }
  }

  test("local dynamic client profiles") {
    assume(optimizelyToken.isDefined)

    // prepare for start, define some variables
    val source = Paths.get("src/test/resources/dynamic_client_profiles.tsv")
    val destinationPath = s"$saunaRoot/com.optimizely.dcp/datasource/v1/$optimizelyServiceId/$optimizelyDatasourceId/tsv:isVip,customerId,spendSegment,birth/ua-team/joe"
    val destinationName = "warehouse"   // It will be uploaded with `.csv` extension
    val destination = Paths.get(s"$destinationPath/$destinationName")

    new File(destinationPath).mkdirs()
    Files.deleteIfExists(destination)

    var error: String = null

    // define mocked logger
    val loggerProps = Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new local file"
          if (!message.text.contains(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"Successfully uploaded file to S3"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else context.become(step3)

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      def step3: Receive = {
        case message: Notification =>
          val expectedText = "All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step4, [${message.text}] does not start with [$expectedText]"
          }

        case message =>
          error = s"in step4, got unexpected message [$message]"
      }

      override def receive = step1
    })

    // define Optimizely, responder and observer
    val loggerActor = system.actorOf(loggerProps)
    val apiWrapper = new Optimizely(optimizelyToken.get, loggerActor)
    val responderProps = DcpResponder.props(apiWrapper, optimizelyImportRegion, loggerActor)
    val observers = Props(new LocalObserver(Paths.get(saunaRoot)))
    system.actorOf(Props(new IntegrationTests.RootActor(List(responderProps, Props(new IntegrationTests.DummyResponder(loggerActor))), observers, loggerActor)))

    Thread.sleep(500)

    // Do an action that should trigger observer
    Files.copy(source, destination)

    // Long period used because local WatchService can be extremely slow sometimes
    Thread.sleep(10000)

    // make sure everything went as expected
    assert(!destination.toFile.exists(), "processed file should have been deleted")
    assert(error == null)
  }

  test("local recipients") {
    assume(sendgridToken.isDefined)   // Or this should be moved into integration tests

    val data = List(
      "\"bob@foo.com1980-06-21\"\t\"Al\"\t\"13\"\t\"a2013-12-15 14:05:06.789\"",
      "\"karl@bar.de\"\t\"1975-07-02\"\t\"\"\t\"12\"\t\"b2014-06-10 21:48:32.712\""
    ).mkString("\n")
    val expected = List(
      "RecipientsResponder: line length unmatch [\"bob@foo.com1980-06-21\"\t\"Al\"\t\"13\"\t\"a2013-12-15 14:05:06.789\"]",
      "Error 0 caused due to [date type conversion error]"
    )

    // Custom logger to check if all messages really appeared
    var received = List.empty[String]
    val logger = system.actorOf(Props(new Logger(None, None) {
      override def receive = {
        case message: Notification => received :+= message.text
      }
    }))

    val sendgrid = new Sendgrid(sendgridToken.get)
    val recipients = system.actorOf(RecipientsResponder.props(logger, sendgrid))
    val noopRef = system.actorOf(Props(new Actor { def receive = { case _ => () }}))

    // send a message, get a Future notification that it was processed
    recipients ! new IntegrationTests.MockLocalFilePublished(data, noopRef)

    // wait for communication with Sendgrid
    Thread.sleep(5000)

    assert(received == expected)
  }
}