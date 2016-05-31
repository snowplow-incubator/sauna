package com.snowplowanalytics.sauna
package actors

// akka
import akka.actor._
import com.snowplowanalytics.sauna.loggers.Logger.Manifestation

// sauna
import apis._
import config._
import loggers._
import responders.optimizely._
import responders.sendgrid._
import responders.urbanairship._

/**
 * Implementations of this actor run on cluster nodes. They launch different observers during lifetime.
 * This class collects common stuff (apis, configs, loggers).
 */
abstract class CommonActor(respondersConfig: RespondersConfig,
                           observersConfig: ObserversConfig,
                           loggersConfig: LoggersConfig) extends Actor {
  // logger
  implicit val logger = (loggersConfig.hipchatEnabled, loggersConfig.dynamodbEnabled) match {
    case (true, true) =>
      context.actorOf(Props(new HipchatLogger(loggersConfig) {
        val dynamodbLogger = context.actorOf(Props(new DynamodbLogger(observersConfig, loggersConfig) with StdoutLogger))
        override def log(message: Manifestation): Unit = dynamodbLogger ! message
      }))

    case (true, false) =>
      context.actorOf(Props(new HipchatLogger(loggersConfig) with StdoutLogger))

    case (false, true) =>
      context.actorOf(Props(new DynamodbLogger(observersConfig, loggersConfig) with StdoutLogger))

    case _ =>
      context.actorOf(Props(new StdoutLogger {}))
  }

  // apis
  // note that even if api was disabled in config, no error will happen, because
  // default sentinel value "" is applied as token, and first real use happens inside responders,
  // and it wont happen if appropriate responder was not activated
  val optimizely = new Optimizely(respondersConfig.optimizelyToken)
  val sendgrid = new Sendgrid(respondersConfig.sendgridToken)
  val urbanairship=new UrbanAirship()
  // responders
  //val uaresp=UAResponder(urbanairship)
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
    responderActors +:= context.actorOf(UAResponder(urbanairship), "UAResponder")

  def receive: Receive = {
    case _ =>
  }
}