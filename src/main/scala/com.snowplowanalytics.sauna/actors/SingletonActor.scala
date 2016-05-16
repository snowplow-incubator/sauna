package com.snowplowanalytics.sauna
package actors

// awscala
import awscala._
import awscala.s3.S3
import awscala.sqs.SQS

// sauna
import config._
import observers._

/**
 * This actor supposed to run on exactly one node.
 */
class SingletonActor(respondersConfig: RespondersConfig,
                     observersConfig: ObserversConfig,
                     loggersConfig: LoggersConfig) extends CommonActor(respondersConfig, observersConfig, loggersConfig) {
  // aws configuration
  implicit val region = Region(observersConfig.awsRegion)
  implicit val credentials = new Credentials(observersConfig.awsAccessKeyId, observersConfig.awsSecretAccessKey)

  // S3
  val s3 = S3(credentials)

  // SQS
  val sqs = SQS(credentials)
  val queue = sqs.queue(observersConfig.sqsName)
                 .getOrElse(throw new Exception("No queue with that name found"))

  // observers
  val s3Observer = new S3Observer(s3, sqs, queue, responderActors, logger)(self)
  s3Observer.start()

  override def postStop(): Unit = {
    s3Observer.interrupt()
  }
}