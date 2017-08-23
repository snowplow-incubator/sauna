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

// java
import java.io.{ByteArrayInputStream, File, PrintWriter}
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source.fromInputStream
import scala.util.{Failure, Success}

// scalatest
import org.scalatest._

// play
import play.api.libs.json._
import play.api.libs.ws.WSResponse
import play.api.libs.ws.ning.NingWSResponse

// akka
import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

// amazonaws
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.{GetRecordsRequest, PutRecordRequest}
import com.amazonaws.services.s3.model.AmazonS3Exception

// awscala
import awscala.s3.S3
import awscala.sqs.SQS
import awscala.{Credentials, Region}

// eventhubs
import com.microsoft.azure.eventhubs.EventHubClient
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventprocessorhost._
import com.microsoft.azure.servicebus.ConnectionStringBuilder

// sauna
import IntegrationTests._
import actors.Mediator
import apis._
import loggers.Logger
import loggers.Logger.{Manifestation, Notification}
import observers.Observer._
import observers._
import Observer.EventHubRecordReceived
import responders.Responder
import responders.Responder.{ResponderEvent, ResponderResult}
import responders.hipchat._
import responders.optimizely._
import responders.pagerduty._
import responders.sendgrid._
import responders.slack._
import responders.opsgenie._
import responders.pusher._
import responders.facebook._

object IntegrationTests {

  case class ObserverTrigger(data: Any)

  /**
   * Supervisor actor
   */
  class RootActor(respondersProps: List[Props], observerProps: Props, override val logger: ActorRef) extends Mediator(SaunaSettings()) {
    override val observers = List(context.actorOf(observerProps))
    override val responderActors = respondersProps.map(p => context.actorOf(p))

    override def receive: Receive = super.receive orElse {
      case ot: ObserverTrigger =>
        observers.foreach {
          _ ! ot.data
        }
    }
  }

  case class AnyEvent(source: ObserverEvent) extends Responder.ResponderEvent

  val filePath = "some-non-existing-file-123/opt/sauna/com.sendgrid.contactdb/recipients/v1/tsv:email,birthday,middle_name,favorite_number,when_promoted/ua-team/joe/warehouse.tsv"

  class MockLocalFilePublished(data: String, observer: ActorRef) extends LocalFilePublished(java.nio.file.Paths.get(filePath), observer) {
    override def streamContent = Some(new ByteArrayInputStream(data.getBytes("UTF-8")))
  }

  /**
   * Dummy responder ignoring all observer event
   */
  class DummyResponder(val logger: ActorRef) extends Responder[ObserverEvent, AnyEvent] {
    def extractEvent(observerEvent: ObserverEvent): Option[AnyEvent] = None

    def process(observerEvent: AnyEvent) = ???
  }

  case object WhenFinished

  object MockResponse extends NingWSResponse(null) {
    override lazy val body = """{}"""
  }

  /**
   * Supervisor actor tracking execution time
   */
  class RootActorAwait(respondersProps: List[Props]) extends Mediator(SaunaSettings()) {
    override val logger = context.actorOf(Props(new NoopActor))
    override val observers = List(context.actorOf(Props(new NoopActor)))
    override val responderActors = respondersProps.map(p => context.actorOf(p))
    var mocksSent = 0
    var results = 0
    var finished: Long = 0L

    def readyToAnswer: Receive = {
      case WhenFinished => sender() ! finished
    }

    def receiveResults: Receive = {
      case result: ResponderResult =>
        results = results + 1
        if (results == 2) {
          finished = System.currentTimeMillis()
          context.become(readyToAnswer)
        }
    }

    override def receive = {
      case mock: MockLocalFilePublished =>
        mocksSent = mocksSent + 1
        responderActors.foreach {
          _ ! mock
        }
        if (mocksSent == 2) context.become(receiveResults)
    }
  }

  class NoopActor extends Actor {
    def receive = {
      case _ => ()
    }
  }

  class MockRealTimeObserver extends Actor with Observer {
    override def receive: Receive = {
      case data: String => context.parent ! new KinesisRecordReceived("", "", ByteBuffer.wrap(data.getBytes()), self)
    }
  }

}

/**
 * Test whole system using real-world resources
 * WARNING: if some test failed it could probably leave resources not cleaned,
 * which means you may probably need to do cleanup (S3 bucket, Optimizely lists) manually
 */
class IntegrationTests extends FunSuite with BeforeAndAfter {

  implicit var system: ActorSystem = _
  var noopRef: ActorRef = _

  val optimizelyToken: Option[String] = sys.env.get("OPTIMIZELY_TOKEN")
  val optimizelyProjectId = sys.env.getOrElse("OPTIMIZELY_PROJECT_ID", "4034532101")
  val optimizelyImportRegion = sys.env.getOrElse("OPTIMIZELY_IMPORT_REGION", "us-east-1")
  val optimizelyServiceId = sys.env.getOrElse("OPTIMIZELY_SERVICE_ID", "4034482827")
  val optimizelyDatasourceId = sys.env.getOrElse("OPTIMIZELY_DATASOURCE_ID", "7082200340")

  val awsAccessKeyId: Option[String] = sys.env.get("AWS_ACCESS_KEY_ID")
  val secretAccessKey: Option[String] = sys.env.get("AWS_SECRET_ACCESS_KEY")
  val awsBucketName = sys.env.getOrElse("AWS_BUCKET_NAME", "sauna-integration-test-bucket-staging") // "sauna-integration-test" for eng-sandbox
  val awsQueueName = sys.env.getOrElse("AWS_QUEUE_NAME", "sauna-integration-test-queue-staging") // "sauna-integration-test-queue" for eng-sandbox
  val awsRegion = sys.env.getOrElse("AWS_REGION", "us-east-1")

  val kinesisApplicationName = sys.env.getOrElse("KINESIS_APPLICATION_NAME", "sauna-integration-test")
  val kinesisStreamName = sys.env.getOrElse("KINESIS_STREAM_NAME", "sauna-integration-test")
  val kinesisRegion = sys.env.getOrElse("KINESIS_REGION", "us-east-1")

  val eventHubName: Option[String] = sys.env.get("AZURE_EVENTHUB_EVENTHUBNAME")
  val consumerGroupName = sys.env.getOrElse("AZURE_EVENTHUB_CONSUMER_GROUP_NAME", "$Default")
  val serviceBusNamespaceName: Option[String] = sys.env.get("AZURE_EVENTHUB_SB_NAMESPACE_NAME")
  val storageConnectionString: Option[String] = sys.env.get("AZURE_EVENTHUB_STORAGE_CONNECTION_STRING")
  val eventHubConnectionString = sys.env.get("AZURE_EVENTHUB_CONNECTION_STRING")
  val checkPointFrequency: Int = sys.env.getOrElse("AZURE_EVENTHUB_CHECKPOINT_FREQ", "5").toInt

  val sendgridToken: Option[String] = sys.env.get("SENDGRID_API_KEY_ID")
  val hipchatToken: Option[String] = sys.env.get("HIPCHAT_TOKEN")
  val opsgenieToken: Option[String] = sys.env.get("OPSGENIE_API_KEY")
  val slackWebhookUrl: Option[String] = sys.env.get("SLACK_WEBHOOK_URL")
  val pagerDutyServiceKey: Option[String] = sys.env.get("PAGERDUTY_SERVICE_KEY")
  val Seq(pusherAppId, pusherKey, pusherSecret, pusherCluster) =
    Seq("APPID", "SECRET", "KEY", "CLUSTER").map{k=>sys.env.get(s"PUSHER_$k")}

  val facebookCAaccessToken: Option[String] = sys.env.get("FACEBOOK_CA_ACCESS_TOKEN")
  val facebookCAaccountId: Option[String] = sys.env.get("FACEBOOK_CA_ACCOUNT_ID")
  val facebookCAappSecret: Option[String] = sys.env.get("FACEBOOK_CA_APP_SECRET")

  val postmarkApiToken: Option[String] = sys.env.get("POSTMARK_API_TOKEN")
  val postmarkInboundEmail: Option[String] = sys.env.get("POSTMARK_INBOUND_EMAIL")

  val saunaRoot = System.getProperty("java.io.tmpdir", "/tmp") + "/saunaRoot"

  before {
    val root = new File(saunaRoot)
    root.delete()
    system = ActorSystem("IntegrationTest")
    root.mkdir()
    noopRef = system.actorOf(Props(new IntegrationTests.NoopActor))
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
      Props(new Responder[ObserverEvent, AnyEvent] {
        val logger: ActorRef = null

        def extractEvent(event: ObserverEvent): Option[AnyEvent] = {
          Some(AnyEvent(event))
        }

        val pathPattern: String = ".*"

        def process(event: AnyEvent): Unit = {
          event.source match {
            case e: ObserverFileEvent =>
              expectedLines = fromInputStream(e.streamContent.get).getLines().toSeq
              self ! new ResponderResult {
                override def source: ResponderEvent = event

                override def message: String = "OK!"
              }
            case e: ObserverCommandEvent =>
              expectedLines = fromInputStream(e.streamContent).getLines().toSeq
              self ! new ResponderResult {
                override def source: ResponderEvent = event

                override def message: String = "OK!"
              }
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
          val observerText = "Active observers:"
          val responderText = "No active responders (or running in a test environment)"

          if (message.text.contains(observerText) || message.text.contains(responderText)) {
            // Ignore startup entity info
          } else if (!message.text.contains(expectedText)) {
            error = s"In step1, [${message.text}] does not contain [$expectedText]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"In step1, got unexpected message $message"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step2, [${message.text}] does not start with [$expectedText]"
          } else {
            context.become(step3)
          }

        case message: Manifestation =>
          id = message.uid

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      def step3: Receive = {
        case message: Notification =>
          val expectedText = "new-lists.tsv has been successfully published"
          if (!message.text.endsWith(expectedText)) {
            error = s"In step3, [${message.text}] does not end with [$expectedText]"
          } else {
            context.become(step4)
          }

        case message =>
          error = s"in step3, got unexpected message [$message]"
      }

      def step4: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message [$destination]. Deleting"
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

    system.actorOf(Props(new IntegrationTests.RootActor(List(responderProps), localObserver, logger))) // TODO: try different names

    // wait
    Thread.sleep(3000)

    // do an action that should trigger observer
    Files.copy(source, destination)

    // extremely long time is required for WatchService on Mac OS X
    Thread.sleep(15000)

    // Cleanup
    apiWrapper.deleteTargetingList(id)

    // make sure everything went as expected
    assert(error == null)
    assert(id != null, "id was not updated")
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
          } else {
            error = "Remain on step1"
            context.become(step2)
          }

        case message =>
          error = s"In step1, got unexpected message $message"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = "Successfully uploaded targeting lists with name"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step2, [${message.text}] does not start with [$expectedText]"
          } else {
            error = "Remain on step2"
            context.become(step3)
          }

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
          } else {
            error = "Remain on step3"
            context.become(step4)
          }

        case message =>
          error = s"in step3, got unexpected message [$message]"
      }

      def step4: Receive = {
        case message: Notification =>
          val expectedText = "All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"In step4, [${message.text}] does not start with [$expectedText]"
          } else {
            error = null // Success
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
    Thread.sleep(2000)

    // do an action that should trigger observer
    s3.putObject(awsBucketName, destination, source)

    // wait, assuming 11 seconds is enough to get file from AWS, and travel to Optimizely and back
    Thread.sleep(11000)

    // Cleanup
    apiWrapper.deleteTargetingList(optimizelyProjectId, "dec_ab_group")
    root ! PoisonPill
    Thread.sleep(1000)

    // make sure everything went as expected
    assert(error == null)
    assert(id != null, "id was not updated")
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
    val destinationName = "warehouse" // It will be uploaded with `.csv` extension
    val destination = Paths.get(s"$destinationPath/$destinationName")

    new File(destinationPath).mkdirs()
    Files.deleteIfExists(destination)

    var error: String = null

    // define mocked logger
    val loggerProps = Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Detected new local file"
          val observerText = "Active observers:"
          val responderText = "No active responders (or running in a test environment)"

          if (message.text.contains(observerText) || message.text.contains(responderText)) {
            // Ignore startup entity info
          } else if (!message.text.contains(expectedText)) {
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
    Thread.sleep(15000)

    // make sure everything went as expected
    assert(!destination.toFile.exists(), "processed file should have been deleted")
    assert(error == null)
  }

  test("local recipients") {
    assume(sendgridToken.isDefined) // Or this should be moved into integration tests

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
    val noopRef = system.actorOf(Props(new Actor {
      def receive = {
        case _ => ()
      }
    }))

    // send a message, get a Future notification that it was processed
    recipients ! new IntegrationTests.MockLocalFilePublished(data, noopRef)

    // wait for communication with Sendgrid
    Thread.sleep(5000)

    assert(received == expected)
  }

  test("respect Sendgrid limitations: LINE_LIMIT and WAIT_TIME") {
    assume(sendgridToken.isDefined)
    implicit val timeout = Timeout(11.seconds)

    var respectLineLimit = true
    val data = (for (i <- 1 to 3000) yield s""""bob$i@foo.com"\t"1980-06-21"\t"Al"\t"13"\t"2013-12-15 14:05:06.789"""")
      .mkString("\n")
    val logger = system.actorOf(Props(new IntegrationTests.NoopActor))
    val mockedSendgrid = new Sendgrid(sendgridToken.get) {
      override def postRecipients(json: JsValue): Future[WSResponse] = {
        json.asOpt[JsArray].foreach { array =>
          if (array.value.length > RecipientsResponder.LINE_LIMIT) {
            respectLineLimit = false
          }
        }
        Future.successful(IntegrationTests.MockResponse)
      }
    }

    val root = system.actorOf(Props(
      new IntegrationTests.RootActorAwait(List(RecipientsResponder.props(logger, mockedSendgrid))))
    )

    // preparing is done, start timing
    val time = System.currentTimeMillis()

    // simulate two simultaneous messages
    root ! new IntegrationTests.MockLocalFilePublished(data, noopRef)
    root ! new IntegrationTests.MockLocalFilePublished(data, noopRef)

    Thread.sleep(8000)

    val end = Await.result((root ? IntegrationTests.WhenFinished).mapTo[Long], 2.second)

    assert(end - time > 4000, "file was processed too fast") // 2 x 3000 lines == 4 seconds
    assert(respectLineLimit, "too many lines in a single chunk")
  }

  // Likely to fail during simultaneous builds (push/pull/deploy)
  ignore("Kinesis (mock responder)") {
    assume(awsAccessKeyId.isDefined)
    assume(secretAccessKey.isDefined)

    var observerRecord: KinesisRecordReceived = null

    val dummyLogger = system.actorOf(Props(new Actor {
      override def receive: PartialFunction[Any, Unit] = {
        case _ =>
      }
    }))


    val responder = Props(new Responder[ObserverEvent, AnyEvent] {
      override def logger: ActorRef = dummyLogger

      override def extractEvent(observerEvent: ObserverEvent): Option[AnyEvent] =
        Some(AnyEvent(observerEvent))

      override def process(event: AnyEvent): Unit = {
        event.source match {
          case r@KinesisRecordReceived(_, _, _, _) =>
            observerRecord = r
        }
      }
    })

    val kinesisObserver = AmazonKinesisObserver.props(AmazonKinesisConfig_1_0_0(
      true,
      kinesisApplicationName,
      AmazonKinesisObserverParameters_1_0_0(
        AwsConfigParameters_1_0_0(
          awsAccessKeyId.get,
          secretAccessKey.get
        ),
        AmazonKinesisConfigParameters_1_0_0(
          kinesisRegion,
          kinesisStreamName,
          5000,
          ShardIteratorType_1_0_0.LATEST,
          None
        )
      )
    ))

    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), kinesisObserver, dummyLogger)))

    // KCL consumers can take a while to get started - give the observer plenty of time.
    Thread.sleep(120000)

    val amazonKinesisClient = new AmazonKinesisClient(
      new BasicAWSCredentials(awsAccessKeyId.get, secretAccessKey.get))
    amazonKinesisClient.configureRegion(Regions.fromName(kinesisRegion))

    // Obtain a shard iterator using the API (the test stream uses a single shard, so this works fine).
    val shardId = amazonKinesisClient
      .describeStream(kinesisStreamName).getStreamDescription.getShards.get(0).getShardId
    val iterator = amazonKinesisClient
      .getShardIterator(kinesisStreamName, shardId, "LATEST").getShardIterator

    // Push some random data to the stream.
    val charset = "UTF-8"
    val data = UUID.randomUUID().toString
    amazonKinesisClient.putRecord(new PutRecordRequest()
      .withStreamName(kinesisStreamName)
      .withPartitionKey("partitionKey")
      .withData(ByteBuffer.wrap(data.getBytes(charset))))

    Thread.sleep(10000)

    // Retrieve the data that was just pushed using a raw client call.
    val apiRecord = amazonKinesisClient.getRecords(new GetRecordsRequest()
      .withShardIterator(iterator))

    // Make sure the data was successfully retrieved by the client...
    assert(apiRecord !== null, "Could not consume the produced record using a raw client call")
    assert(apiRecord.getRecords.size() === 1)
    assert(new String(apiRecord.getRecords.get(0).getData.array(), Charset.forName(charset)) === data)

    // ...as well as the observer.
    assert(observerRecord !== null, "Could not consume the produced record using the observer")
    assert(new String(observerRecord.data.array(), Charset.forName(charset)) === data)
  }

  test("Azure Eventhub (mock responder)") {
    assume(eventHubName.isDefined)
    assume(serviceBusNamespaceName.isDefined)
    assume(eventHubConnectionString.isDefined)
    assume(serviceBusNamespaceName.isDefined)
    var observerRecord: EventHubRecordReceived = null

    val dummyLogger = system.actorOf(Props(new Actor {
      override def receive: PartialFunction[Any, Unit] = {
        case _ =>
      }
    }))


    val responder = Props(new Responder[ObserverEvent, AnyEvent] {
      override def logger: ActorRef = dummyLogger

      override def extractEvent(observerEvent: ObserverEvent): Option[AnyEvent] =
        Some(AnyEvent(observerEvent))

      override def process(event: AnyEvent): Unit = {
        event.source match {
          case r@ EventHubRecordReceived(_, _, _) =>
            observerRecord = r
        }
      }
    })

    val eventhubsObserver = AzureEventHubsObserver.props(
      eventHubName.get,
      serviceBusNamespaceName.get,
      eventHubConnectionString.get,
      storageConnectionString.get,
      Some(consumerGroupName),
      checkPointFrequency
    )

    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), eventhubsObserver, dummyLogger)))

    Thread.sleep(75000)

    val host: EventProcessorHost  = new EventProcessorHost(eventHubName.get, consumerGroupName,
    eventHubConnectionString.get, storageConnectionString.get)

    val charset = "UTF-8"
    val data = UUID.randomUUID().toString
    val sendEvent: EventData = new EventData(data.getBytes(charset))

    val ehClient: EventHubClient = EventHubClient.createFromConnectionStringSync(eventHubConnectionString.get)
    (1 to 5).foreach{i => ehClient.sendSync(sendEvent)}

    Thread.sleep(5000)

    // ...as well as the observer.
    assert(observerRecord !== null, "Could not consume the produced record using the observer")
    assert(new String(observerRecord.data.getBytes, Charset.forName(charset)) === data)
  }

  test("HipChat responder") {
    assume(hipchatToken.isDefined)

    // Get some data from a resource file.
    val command: String = fromInputStream(getClass.getResourceAsStream("/commands/hipchat.json")).getLines().mkString

    // Define a mock logger.
    var error: String = "Did not send HipChat notification"
    val dummyLogger = system.actorOf(Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Successfully sent HipChat notification"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else {
            error = null
          }

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      override def receive = step1
    }))

    // Define other actors.
    val apiWrapper = new Hipchat(hipchatToken.get, dummyLogger)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = SendRoomNotificationResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(10000)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("OpsGenie responder") {
    assume(opsgenieToken.isDefined)

    // Get some data from a resource file.
    val command: String = fromInputStream(getClass.getResourceAsStream("/commands/opsgenie.json")).getLines().mkString

    // Define a mock logger.
    var error: String = "Failed to create new OpsGenie alert"
    val dummyLogger = system.actorOf(Props(new Actor {
      override def receive: Receive = {
        case status: OpsGenie.CreateAlertSuccess => error = null
        case alertError: OpsGenie.CreateAlertError => error = alertError.message
      }
    }))

    // Define other actors.
    val apiWrapper = new OpsGenie(opsgenieToken.get, dummyLogger)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = CreateAlertResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(10000)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("Pusher responder") {
    assume(pusherAppId.isDefined)
    assume(pusherKey.isDefined)
    assume(pusherSecret.isDefined)

    // Get some data from a resource file.
    val command: String = fromInputStream(getClass.getResourceAsStream("/commands/pusher.json")).getLines().mkString

    // Define a mock logger.
    var error: String = "Did not publish Pusher event"
    val dummyLogger = system.actorOf(Props(new Actor {
      import PublishEventResponder._
      override def receive: Receive = {
        case e: EventSent => error = null
        case message: Notification => error = message.text
      }

    }))

    // Define other actors.
    val apiWrapper = new Pusher(pusherAppId.get, pusherKey.get, pusherSecret.get, pusherCluster, dummyLogger)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = PublishEventResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(10000)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("Facebook Custom Audience responder") {
    assume(facebookCAaccessToken.isDefined)
    assume(facebookCAaccountId.isDefined)
    assume(facebookCAappSecret.isDefined)

    // Get some data from a resource file.
    val command: String = fromInputStream(getClass.getResourceAsStream("/commands/facebook.json")).getLines().mkString

    // Define a mock logger.
    var error: String = "Did not upload Facebook custom audience"
    val dummyLogger = system.actorOf(Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Error while uploading audience:"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else {
            error = null
          }

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      override def receive = step1
    }))

    // Define other actors.
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = CustomAudienceResponder.props(facebookCAaccessToken.get, facebookCAappSecret.get, facebookCAaccountId.get, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(10000)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("Slack responder") {
    assume(slackWebhookUrl.isDefined)

    // Get some data from a resource file.
    val command: String = fromInputStream(getClass.getResourceAsStream("/commands/slack.json")).getLines().mkString

    // Define a mock logger.
    var error: String = "Did not send Slack message"
    val dummyLogger = system.actorOf(Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Successfully sent Slack message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else {
            error = null
          }

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      override def receive = step1
    }))

    // Define other actors.
    val apiWrapper = new Slack(slackWebhookUrl.get, dummyLogger)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = SendMessageResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(5000)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("PagerDuty responder") {
    assume(pagerDutyServiceKey.isDefined)

    // Get some data from a resource file - replace the service key with an environment variable.
    var command: String = fromInputStream(getClass.getResourceAsStream("/commands/pagerDuty.json")).getLines().mkString
    command = command.replaceAll("PAGERDUTY_SERVICE_KEY", pagerDutyServiceKey.get)

    // Define a mock logger.
    var error: String = "Did not create PagerDuty event"
    val dummyLogger = system.actorOf(Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Successfully created PagerDuty event"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else {
            error = null
          }

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      override def receive = step1
    }))

    // Define other actors.
    val apiWrapper = new PagerDuty(dummyLogger)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = CreateEventResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    Thread.sleep(7500)

    // Assert that nothing went wrong.
    assert(error == null)
  }

  test("SendGrid command responder") {
    assume(sendgridToken.isDefined)
    assume(postmarkApiToken.isDefined)
    assume(postmarkInboundEmail.isDefined)

    // Get some data from a resource file.
    var command: String = fromInputStream(getClass.getResourceAsStream("/commands/sendgrid.json")).getLines().mkString
    command = command.replaceAll("POSTMARK_INBOUND_EMAIL", postmarkInboundEmail.get)
    val subject: String = UUID.randomUUID().toString
    command = command.replaceAll("RANDOM_UUID", subject)

    // Define a mock logger.
    var error: String = "Did not send SendGrid email"
    val dummyLogger = system.actorOf(Props(new Actor {
      def step1: Receive = {
        case message: Notification =>
          val expectedText = "Successfully sent Sendgrid email"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step1, [${message.text}] does not contain [$expectedText]]"
          } else {
            context.become(step2)
          }

        case message =>
          error = s"in step1, got unexpected message [$message]"
      }

      def step2: Receive = {
        case message: Notification =>
          val expectedText = s"All actors finished processing message"
          if (!message.text.startsWith(expectedText)) {
            error = s"in step2, [${message.text}] is not equal to [$expectedText]]"
          } else {
            error = null
          }

        case message =>
          error = s"in step2, got unexpected message [$message]"
      }

      override def receive = step1
    }))

    // Define other actors.
    val apiWrapper = new Sendgrid(sendgridToken.get)
    val dummyObserver = Props(new MockRealTimeObserver())
    val responder = SendEmailResponder.props(apiWrapper, dummyLogger)
    val root = system.actorOf(Props(new IntegrationTests.RootActor(List(responder), dummyObserver, dummyLogger)))
    val postmarkWrapper = new Postmark(postmarkApiToken.get)

    Thread.sleep(3000)

    // Manually trigger the observer.
    root ! ObserverTrigger(command)

    // Time for Sendgrid to send the email, and for Postmark to process it & expose to inbound API.
    Thread.sleep(60000)

    // Assert that nothing went wrong with the command.
    assert(error == null)

    // Verify that the email was successfully sent.
    postmarkWrapper.getInboundMessage(subject).onComplete {
      case Success(message) =>
        if (message.status == 200) {
          val json: JsValue = Json.parse(message.body)
          assert((json \ "TotalCount").as[Int] == 1)
          val inbound: JsLookupResult = (json \ "InboundMessages") (0)
          assert((inbound \ "From").as[String].equals("staging@saunatests.com"))
          assert((inbound \ "To").as[String].equals(postmarkInboundEmail.get))
          assert((inbound \ "Subject").as[String].equals(subject))
        }
        else
          fail(message.body)
      case Failure(error) =>
        fail(error)
    }

    Thread.sleep(5000)
  }
}
