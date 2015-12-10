package com.snowplowanalytics.sauna
package actors

// akka
import akka.actor._
import com.snowplowanalytics.sauna.loggers.Logger.{Notification, Manifestation}

// sauna
import apis._
import config._
import loggers._
import responders.optimizely._
import responders.sendgrid._

/**
 * Implementations of this actor run on cluster nodes. They launch different observers during lifetime.
 * This class collects common stuff (apis, configs, loggers).
 */
abstract class CommonActor(respondersConfig: RespondersConfig,
                           observersConfig: ObserversConfig,
                           loggersConfig: LoggersConfig) extends Actor {
  // logger
  // bunch of lazy loggers, to reduce boilerplate code
  private[this] lazy val dynamodbLogger = context.actorOf(Props(new DynamodbLogger(loggersConfig) {
    override def log(message: Notification): Unit = ??? // should never happen
  }))
  private[this] lazy val hipchatLogger = context.actorOf(Props(new HipchatLogger(loggersConfig) {
    override def log(message: Manifestation): Unit = ??? // should never happen
  }))
  private[this] lazy val stdoutLogger = context.actorOf(Props(new StdoutLogger {}))
  private[this] lazy val mutedLogger = context.actorOf(Props(new MutedLogger))

  implicit val logger = (loggersConfig.hipchatEnabled, loggersConfig.dynamodbEnabled, loggersConfig.stdoutEnabled) match {
    // hipchat + dynamodb (stdoutEnabled doesn't matter here) 1, 2
    case (true, true, _) =>
      context.actorOf(Props(new Logger {
        override def log(message: Manifestation): Unit = dynamodbLogger ! message

        override def log(message: Notification): Unit = hipchatLogger ! message
      }))

    // hipchat + stdout 3
    case (true, false, true) =>
      context.actorOf(Props(new Logger {
        override def log(message: Manifestation): Unit = stdoutLogger ! message

        override def log(message: Notification): Unit = hipchatLogger ! message
      }))

    // hipchat + muted 4
    case (true, false, false) =>
      context.actorOf(Props(new Logger {
        override def log(message: Manifestation): Unit = mutedLogger ! message

        override def log(message: Notification): Unit = hipchatLogger ! message
      }))

    // dynamodb + stdout 5
    case (false, true, true) =>
      context.actorOf(Props(new Logger {
        override def log(message: Manifestation): Unit = dynamodbLogger ! message

        override def log(message: Notification): Unit = stdoutLogger ! message
      }))

    // dynamodb + muted 6
    case (false, true, false) =>
      context.actorOf(Props(new Logger {
        override def log(message: Manifestation): Unit = dynamodbLogger ! message

        override def log(message: Notification): Unit = mutedLogger ! message
      }))

    // only stdout 7
    case (false, false, true) =>
      stdoutLogger

    // only muted 8
    case (false, false, false) =>
      mutedLogger
  }

  // apis
  // note that even if api was disabled in config, no error will happen, because
  // default sentinel value "" is applied as token, and first real use happens inside responders,
  // and it wont happen if appropriate responder was not activated
  val optimizely = new Optimizely(respondersConfig.optimizelyToken)
  val sendgrid = new Sendgrid(respondersConfig.sendgridToken)

  // responders
  var responderActors = List.empty[ActorRef]
  if (respondersConfig.targetingListEnabled) {
    responderActors +:= context.actorOf(TargetingList(optimizely), "TargetingList")
  }
  if (respondersConfig.dynamicClientProfilesEnabled) {
    responderActors +:= context.actorOf(DCPDatasource(optimizely, observersConfig.saunaRoot, respondersConfig.optimizelyImportRegion), "DCPDatasource")
  }
  if (respondersConfig.recipientsEnabled) {
    responderActors +:= context.actorOf(Recipients(sendgrid), "Recipients")
  }

  def receive: Receive = {
    case _ =>
  }
}