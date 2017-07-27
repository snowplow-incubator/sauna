package com.snowplowanalytics.sauna
package observers

// java
import java.util.Date
import java.util.function.Consumer
import java.util.concurrent.ExecutionException


// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.collection.JavaConverters._
import scala.util.{Try, Failure, Success}

// akka
import akka.actor._
//import system.dispatcher

// eventhubs
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventprocessorhost._
import com.microsoft.azure.servicebus.ConnectionStringBuilder


// sauna
import Observer.EventHubRecordReceived

class AzureEventHubsObserver(
  val eventHubName: String,
  val namespaceName: String,
  val eventHubConnectionString: String,
  val storageConnectionString: String,
  val consumerGroupName: Option[String],
  val checkPointFrequency: Int) extends Actor with Observer {
  import AzureEventHubsObserver._

  val host: EventProcessorHost  = new EventProcessorHost(eventHubName, consumerGroupName.getOrElse("$Default"), 
    eventHubConnectionString, storageConnectionString)
  val notifier = (s: String) => notify(s)
  notify("Registering host named " + host.getHostName())
  val options: EventProcessorOptions = new EventProcessorOptions()
  options.setExceptionNotification(new ErrorNotificationHandler(notifier))
  
  override def preStart: Unit = {
    try{    
      val evpf = new EventProcessorFactory(context, checkPointFrequency, notifier)
      host.registerEventProcessorFactory(evpf, options).get()
    }catch {
      case e: ExecutionException => 
        notify(e.getCause().toString())
      case e: Exception =>
        notify(s"Failure while registering: ${e.toString()}")
    }
    super.preStart()
  }
  
  def receive: Receive = {
    case _ =>
  }

  override def postStop: Unit = {
    host.unregisterEventProcessor()
    notify("Calling forceExecutorShutdown")
    EventProcessorHost.forceExecutorShutdown(120)
  }
}

object AzureEventHubsObserver{
  def props(eventHubName: String, namespaceName: String, eventHubConnStr: String, storageConnStr: String,
    consumerGroupName: Option[String], checkPointFrequency: Int): Props = {
    Props(new AzureEventHubsObserver(eventHubName, namespaceName, eventHubConnStr, storageConnStr,
    consumerGroupName, checkPointFrequency))
  }

  def props(config: AzureEventHubsConfig_1_0_0): Props = {
    val params = config.parameters
    props(params.eventHubName, params.serviceBusNamespaceName, params.eventHubConnectionString,
    params.storageConnectionString, Some(params.consumerGroupName), params.checkPointFrequency)
  }

  class ErrorNotificationHandler(notify: (String => Unit)) extends Consumer[ExceptionReceivedEventArgs]{
    override def accept(t: ExceptionReceivedEventArgs): Unit = {
      val m = t.getException().toString()
      val action = t.getAction()
      val hostname = t.getHostname()
      notify(s"SAMPLE: Host ${hostname} received general error notification during ${action} : ${m}")
    }
  }

  class EventProcessor(
    actor: ActorContext,
    checkpointingFreq: Int,
    notify: (String => Unit)) extends IEventProcessor{
    private var checkpointBatchingCount = 0

    def onOpen(context: PartitionContext) = {
      notify("SAMPLE: Partition " + context.getPartitionId() + " is opening")
    }

    def onClose(context: PartitionContext, reason: CloseReason) = {
      notify("SAMPLE: Partition " + context.getPartitionId() + " is closing for reason " + reason.toString())
    }
    
    def onError(context: PartitionContext, error: Throwable) = {
      notify("SAMPLE: Partition " + context.getPartitionId() + " onError: " + error.toString())
    }

    override def onEvents(context: PartitionContext, messages: java.lang.Iterable[EventData]): Unit = {
      notify("SAMPLE: Partition " + context.getPartitionId() + " got message batch")
      var messageCount = 0
      val partId = context.getPartitionId()
      for (data <- messages.asScala)
      {
        val offset = data.getSystemProperties().getOffset()
        val seqNum  = data.getSystemProperties().getSequenceNumber()
        notify(s"SAMPLE (${partId} ,${offset}, ${seqNum} ): ${data.getBody()}")
        actor.parent ! EventHubRecordReceived(context, data, actor.parent)
        messageCount += 1
        checkpointBatchingCount += 1
        if((checkpointBatchingCount % checkpointingFreq) == 0)
        {
          checkpoint(context, data)
        }
      }
      notify(s"SAMPLE: Partition $partId batch size was $messageCount for host ${context.getOwner()}")
    }

    def checkpoint(context: PartitionContext, data: EventData): Unit = {
      val partId = context.getPartitionId()
      val offset = data.getSystemProperties().getOffset()
      val seqNum = data.getSystemProperties().getSequenceNumber()
      notify(s"SAMPLE: Partition ${partId} checkpointing at ${offset}, ${seqNum}")
      context.checkpoint(data)
    }
  }

  class EventProcessorFactory(
    actor: ActorContext,
    checkpointingFreq: Int,
    notifier: (String => Unit)) extends IEventProcessorFactory[EventProcessor]{
    override def createEventProcessor(context: PartitionContext): EventProcessor = {
      new EventProcessor(actor, checkpointingFreq, notifier)
    }
  }
}
