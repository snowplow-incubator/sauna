package com.snowplowanalytics.sauna
package actors

// sauna
import config._
import observers._

/**
 * This actor supposed to run on all nodes.
 */
class UbiquitousActor(respondersConfig: RespondersConfig,
                      observersConfig: ObserversConfig,
                      loggersConfig: LoggersConfig) extends CommonActor(respondersConfig, observersConfig, loggersConfig) {
  val localObserver = new LocalObserver(observersConfig.saunaRoot, responderActors, logger)(self)
  localObserver.start()

  override def postStop(): Unit = {
    localObserver.interrupt()
  }
}