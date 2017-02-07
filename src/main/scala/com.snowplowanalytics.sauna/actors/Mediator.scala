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
package actors

// scala
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, TimeoutException}

// akka
import akka.actor._

// sauna
import apis._
import loggers.Logger._
import loggers._
import observers.Observer._
import observers._
import responders._
import responders.hipchat._
import responders.optimizely._
import responders.sendgrid._

/**
 * Root user-level (supervisor) actor
 * Collects all common entities (apis, configs, logger, observers)
 * and perform central role in coordinating work
 */
class Mediator(saunaSettings: SaunaSettings) extends Actor {

  import Mediator._

  // For tick
  import context.dispatcher

  /**
   * Scheduled message to check internal state and warn about inconsistencies
   */
  val tick = context.system.scheduler.schedule(1.minute, 1.minute, self, Tick)

  /**
   * Observer actor, responsible for watching for new events
   */
  val observers: List[ActorRef] =
    localObserversCreator(saunaSettings).map { case (name, props) => context.actorOf(props, name) } ++
      s3ObserverCreator(saunaSettings).map { case (name, props) => context.actorOf(props, name) } ++
      kinesisObserverCreator(saunaSettings).map { case (name, props) => context.actorOf(props, name) }

  // Terminate application if none observer were configured
  // null is valid value when overriding observers in tests
  if (observers != null && observers.isEmpty) {
    stop(Some("At least one observer must be configured"))
  }

  /**
   * Single system logger, accepting all Notifications and Manifestations
   * from all observers, responders, etc
   */
  val logger = context.actorOf(loggerCreator(saunaSettings))

  /**
   * Map of currently processing events from all observers
   * Primary mediator's state
   */
  val processedEvents = new mutable.HashMap[ObserverBatchEvent, MessageState]()

  /**
   * List of responder actors, communicating with 3rd-party APIs
   */
  val responderActors = respondersProps(saunaSettings)
    .map(creator => creator(logger))
    .map { case (name, props) => context.actorOf(props, name) }

  /**
   * Check current state for orphan messages and notify user about them
   *
   * @return list of notifications
   */
  def getWarnings: List[String] = {
    val currentTime = System.currentTimeMillis()
    processedEvents.flatMap { case (event, state) =>
      state.check match {
        case InProcess(timings) =>
          val delayed = timings.filter(currentTime - _._2 > 60000)
          delayed.map { case (actor, time) =>
            s"Delay warning: message from [${event.path}] was sent to responder [$actor] and no respond was received for ${(currentTime - time) / 1000} seconds"
          }
        case AllFinished(finishers) =>
          val timestamps = finishers.map(_._2)
          if (timestamps.nonEmpty) {
            val last = timestamps.max
            List(s"Delay warning: message from [${event.path}] was processed by all [${finishers.size}] actors [${(currentTime - last) / 1000} seconds ago] and still wasn't deleted from internal state")
          } else Nil
      }
    }.toList
  }

  def receive = {

    // Broadcast observer event to all responders
    case observerEvent: Observer.ObserverBatchEvent =>
      responderActors.map { responder => responder ! observerEvent }

    // Track what responders accepted broadcast
    case Responder.Accepted(message, responder) =>
      processedEvents.get(message) match {
        case Some(state) =>
          processedEvents.put(message, state.addReceiver(responder))
        case None =>
          processedEvents.put(message, MessageState.empty.addReceiver(responder))
      }

    // Track rejected messages
    case Responder.Rejected(event, rejecter) =>
      processedEvents.get(event) match {
        case Some(state) =>
          val updatedState = state.addRejecter(rejecter)
          processedEvents.put(event, updatedState)
          if (updatedState.rejecters.size == responderActors.size) {
            notify(s"Warning: observer event from [${event.path}] was rejected by all responders")
          }
        case None =>
          processedEvents.put(event, MessageState.empty.addRejecter(rejecter))
          if (responderActors.size == 1) {
            notify(s"Warning: observer event from [${event.path}] was rejected by single running responder")
          }
      }

    // Mutate `processedEvents` primary state and clean-up resources
    case result: Responder.ResponderResult =>
      notify(result.message)
      val original = result.source.source
      processedEvents.get(original) match {
        case Some(state) =>
          val updatedState = state.addFinisher(sender())
          updatedState.check match {
            case AllFinished(actorStamps) =>
              notify(s"All actors finished processing message [${original.path}]. Deleting")
              original match {
                case l: LocalFilePublished => original.observer ! Observer.DeleteLocalFile(l.file)
                case s: S3FilePublished => original.observer ! Observer.DeleteS3Object(s.path, s.s3Source)
                case r: KinesisRecordReceived => ()
              }
              processedEvents.remove(original)
            case InProcess(stillWorking) =>
              notify(s"Some actors still processing message [${original.path}]")
              processedEvents.put(original, updatedState)
          }
        case None =>
          notify(s"Mediator received unknown (not-accepted) ResponderResult [$result]")
      }

    // Forward notification
    case notification: Notification =>
      logger ! notification

    // Check state
    case Tick =>
      getWarnings.foreach(notify)
  }

  override def postStop(): Unit = {
    tick.cancel()
  }

  /**
   * Stop whole application along with mediator actor
   *
   * @param error optional error message
   */
  def stop(error: Option[String]): Unit = {
    try {
      Await.result(context.system.terminate(), 5.seconds)
    } catch {
      case e: TimeoutException => ()
    } finally {
      error match {
        case Some(e) => sys.error("At least one observer must be configured")
        case None =>
          println("Mediator actor stopped")
          sys.exit()
      }
    }
  }

  def notify(message: String): Unit =
    logger ! Notification(message)
}

object Mediator {

  /**
   * Self-awaking message to check current state for consistency
   */
  case object Tick

  /**
   * Pair representing actor name and timestamp when it finished to process
   */
  type ActorStamp = (ActorRef, Long)

  /**
   * Map of actor (responder) and timestamp (in milliseconds) when it received
   * OR finished to process some observer event
   */
  type Timings = Map[ActorRef, Long]

  /**
   * State of some message, denoting which responders received message (when)
   * and which responders finishers processing message
   *
   * @param receivers list of actor and timestamp, denoting when each
   *                  received message
   * @param finishers list of actor and timestamp, denoting when each
   *                  finishers processing message
   */
  // This internal class was introduced to avoid extremely long `ask` on responders,
  // handling big files. Using it, we can be sure what files have been processed,
  // yet we don't assume any timeouts, because biggest timeout can be not big enough
  private[actors] case class MessageState(receivers: Timings, finishers: Timings, rejecters: List[ActorRef]) {

    /**
     * Add responder accepted observer event for further processing
     *
     * @param receiver responder
     * @return updated state
     */
    def addReceiver(receiver: ActorRef): MessageState = {
      val timestamp = System.currentTimeMillis()
      this.copy(receivers = Map(receiver -> timestamp) ++ receivers)
    }

    /**
     * Add responder finished processing an observer event
     *
     * @param finisher responder
     * @return updated state
     */
    def addFinisher(finisher: ActorRef): MessageState = {
      val timestamp = System.currentTimeMillis()
      this.copy(finishers = Map(finisher -> timestamp) ++ finishers)
    }

    /**
     * Add responder rejected to process an observer event
     *
     * @param rejecter responder
     * @return updated state
     */
    def addRejecter(rejecter: ActorRef): MessageState = {
      this.copy(rejecters = rejecter :: rejecters)
    }

    /**
     * Get current state of message processing
     */
    def check: ProcessingState = {
      val stillWorking = receivers.keySet diff finishers.keySet
      if (stillWorking.isEmpty) {
        AllFinished(finishers.toList)
      } else {
        InProcess(timings)
      }
    }

    /**
     * Get time in milliseconds which every still-working actor processing a
     * message so far
     */
    def timings: Timings = {
      val pairs = finishers.map { case (actor, finishedTime) =>
        receivers.get(actor).map(receivedTime => (actor, finishedTime - receivedTime))
      }
      pairs.flatten.toMap
    }
  }

  object MessageState {
    /**
     * Initial state for mediator actor, no messages accepted, no messages sent
     */
    val empty = MessageState(Map.empty, Map.empty, Nil)
  }

  /**
   * Shorthand current state, denoting if message processing is complete by
   * all responders
   */
  sealed trait ProcessingState extends Product with Serializable
  case class AllFinished(timings: List[ActorStamp]) extends ProcessingState
  case class InProcess(stillWorking: Timings) extends ProcessingState

  // Below is primitive version of Reader monad (called Creator for simplicity)
  // It gives some amount of flexibility for dependency injection, allowing
  // to factor out responders' props construction from supervisor constructor,
  // which in turn allows us to test supvervisor with injected actors and
  // test creation of responders

  /**
   * Actor names can be configured
   */
  type ActorName = String

  /**
   * Actor props dependent on logger
   */
  type ActorConstructor = SaunaLogger => (ActorName, Props)

  /**
   * Function that given sauna settings can produce several actors
   */
  type ResponderCreator = SaunaSettings => List[ActorConstructor]

  /**
   * Function that given sauna settings can produce actor props
   */
  type ActorCreator = SaunaSettings => Props

  /**
   * Function that given sauna settings can produce multiple actors props
   */
  type MultipleActorCreator = SaunaSettings => List[(ActorName, Props)]

  /**
   * Deepest dependency (every responder has to be constructed with logger)
   */
  type SaunaLogger = ActorRef

  /**
   * List of functions able to consctruct particular responders
   */
  val responderCreators = List(sendgridCreator _, optimizelyCreator _, hipchatCreator _)

  def respondersProps(saunaSettings: SaunaSettings): List[ActorConstructor] = {
    responderCreators.flatMap { constructor => constructor(saunaSettings) }
  }

  /**
   * Function producing `Props` (still requiring logger) for Optimizely responders
   *
   * @param saunaSettings global settings object
   * @return list of functions that accept logger and produce optimizely responders
   */
  def optimizelyCreator(saunaSettings: SaunaSettings): List[ActorConstructor] = {
    saunaSettings.optimizelyConfig match {
      case Some(OptimizelyConfig(true, id, params)) =>

        val apiWrapper: SaunaLogger => Optimizely = (logger) => new Optimizely(params.token, logger)

        val targetingProps: List[ActorConstructor] =
          if (params.targetingListEnabled)
            ((logger: SaunaLogger) => (id + "-TargetingList", TargetingListResponder.props(apiWrapper(logger), logger))) :: Nil
          else
            Nil

        val dcpProps: List[ActorConstructor] =
          if (params.dynamicClientProfilesEnabled)
            ((logger: SaunaLogger) => (id + "-Dcp", DcpResponder.props(apiWrapper(logger), params.awsRegion, logger))) :: Nil
          else
            Nil

        targetingProps ++ dcpProps

      case _ => Nil
    }
  }

  /**
   * Function producing `Props` (still requiring logger) for Sendgrid responders
   * (only `RecipientsResponder` so far)
   *
   * @param saunaSettings global settings object
   * @return list of functions that accept logger and produce sendgrid responders
   */
  def sendgridCreator(saunaSettings: SaunaSettings): List[ActorConstructor] = {
    saunaSettings.sendgridConfig match {
      case Some(SendgridConfig(true, id, params)) =>
        val apiWrapper: Sendgrid = new Sendgrid(params.apiKeyId)
        if (params.recipientsEnabled) {
          ((logger: SaunaLogger) => (id, RecipientsResponder.props(logger, apiWrapper))) :: Nil
        } else Nil

      case _ => Nil
    }
  }

  /**
   * A function producing `Props` based on loggers for the Hipchat responder.
   *
   * @param saunaSettings A global settings object.
   * @return A list of functions that accept loggers and produce Hipchat responders.
   */
  def hipchatCreator(saunaSettings: SaunaSettings): List[ActorConstructor] = {
    saunaSettings.hipchatResponderConfig match {
      case Some(responders.HipchatConfig(true, id, params)) =>
        val apiWrapper: SaunaLogger => Hipchat = (logger) => new Hipchat(params.authToken, logger)
        if (params.sendRoomNotificationEnabled) {
          ((logger: SaunaLogger) => (id, SendRoomNotificationResponder.props(apiWrapper(logger), logger))) :: Nil
        } else Nil

      case _ => Nil
    }
  }

  /**
   * Function producing `props` for local observer
   *
   * @param saunaSettings global settings object
   * @return immutable `Props` object ready to be used for creating several
   *         local observers
   */
  def localObserversCreator(saunaSettings: SaunaSettings): List[(ActorName, Props)] = {
    saunaSettings.localFilesystemConfigs.flatMap { config =>
      if (config.enabled) {
        List((config.id, LocalObserver.props(config.parameters.saunaRoot)))
      } else Nil
    }
  }

  /**
   * Function producing `props` for S3 observer
   *
   * @param saunaSettings global settings object
   * @return immutable `Props` object ready to be used for creating several
   *         S3 observers
   */
  def s3ObserverCreator(saunaSettings: SaunaSettings): List[(ActorName, Props)] = {
    saunaSettings.amazonS3Configs.flatMap { config =>
      if (config.enabled) {
        List((config.id, AmazonS3Observer.props(config.parameters)))
      } else Nil
    }
  }

  /**
   * Function producing `props` for Kinesis observer
   *
   * @param saunaSettings global settings object
   * @return immutable `Props` object ready to be used for creating several
   *         Kinesis observers
   */
  def kinesisObserverCreator(saunaSettings: SaunaSettings): List[(ActorName, Props)] = {
    saunaSettings.amazonKinesisConfigs.flatMap { config =>
      if (config.enabled) {
        List((config.id, AmazonKinesisObserver.props(config)))
      } else Nil
    }
  }

  /**
   * Function producing `Props` for `Logger`
   *
   * @param saunaSettings global settings object
   * @return immutable `Props` object ready to be used for creating logger
   */
  def loggerCreator(saunaSettings: SaunaSettings): Props = {

    val dynamodb = saunaSettings.amazonDynamodbConfig match {
      case Some(AmazonDynamodbConfig(true, _, dynamodbParams)) =>
        val dynamodbProps = DynamodbLogger.props(dynamodbParams)
        Some(DynamodbProps(dynamodbProps))
      case _ => None
    }

    val hipchat = saunaSettings.hipchatLoggerConfig match {
      case Some(loggers.HipchatConfig(true, _, hipchatParams)) =>
        val hipchatProps = HipchatLogger.props(hipchatParams)
        Some(HipchatProps(hipchatProps))
      case _ => None
    }

    Logger.props(dynamodb, hipchat)
  }
}
