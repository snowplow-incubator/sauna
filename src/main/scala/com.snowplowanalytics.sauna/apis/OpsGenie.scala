/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package apis

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// akka
import akka.actor.ActorRef

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._

// jackson
import com.fasterxml.jackson.core.JsonParseException

// sauna
import loggers.Logger.Notification
import utils._

/**
 * Opsgenie API wrapper. Encapsulates all communications with Opsgenie
 *
 * @param apiKey OpsGenie token.
 * @param logger A logger actor.
 */
class OpsGenie(apiKey: String, logger: ActorRef) {
  import OpsGenie._

  val client = makeClient(apiKey)

  /**
   * Create alert request is used to create alerts in OpsGenie
   *
   * @param  alert  OpsGenie Alert object
   *
   * @return Future[CreateAlertResponse]
   */
  def createAlert(alert: Alert): Future[CreateAlertResponse] = Future{
    CreateAlertRequest(alert)(client).toResponse
  }

}

object OpsGenie {
  import com.ifountain.opsgenie.client.OpsGenieClient
  import com.ifountain.opsgenie.client.swagger.model.{CreateAlertRequest => POJOCreateAlertRequest, TeamRecipient}
  import com.ifountain.opsgenie.client.swagger.model.{SuccessResponse => POJOSuccessResponse}
  import scala.util.{Try, Success, Failure}
  
  type Alias = String
  type Description = String
  type Tag = String
  type Recipient = String
  type User = String
  type Note = String
  type Action = String
  type Message = String
  type Entity = String
  type Source = String
  type Team = String

  def makeClient(apiKey: String) = {
    val client = new OpsGenieClient()
    client.setApiKey(apiKey)
    client
  }

  case class Alert(
    val message:     Message,
    val teams:       Option[Seq[Team]] = None,
    val alias:       Option[Alias] = None,
    val description: Option[Description] = None,
    val recipients:  Option[Seq[Recipient]] = None,
    val actions:     Option[Seq[Action]] = None,
    val source:      Option[Source] = None,
    val tags:        Option[Seq[Tag]] = None,
    val details:     Option[Map[String, String]] = None,
    val entity:      Option[Entity] = None,
    val user:        Option[User] = None,
    val note:        Option[Note] = None){

    lazy val request: POJOCreateAlertRequest = {
      val request: POJOCreateAlertRequest =  new POJOCreateAlertRequest();
      request.setMessage(message)
      alias.foreach {a => request.setAlias(a)}
      description.foreach {descr => request.setDescription(descr)}
      entity.foreach {e => request.setEntity(e)}
      user.foreach {u => request.setUser(u)}
      note.foreach {n => request.setNote(note.get)}

      if(teams.isDefined) teams.get.foreach{t => request.addTeamsItem(new TeamRecipient().name(t))}
      if(recipients.isDefined) recipients.get.foreach{r => request.addTeamsItem(new TeamRecipient().name(r))}
      if(actions.isDefined) actions.get.foreach{a => request.addActionsItem(a)}
      if(tags.isDefined) tags.get.foreach{t => request.addTagsItem(t)}
      
      request  
    }
  }

  /**
   * Custom reader for an Alert instance.
   */
  implicit val alertReads: Reads[Alert] = (
    (JsPath \ "message").read[Message] and
    (JsPath \ "teams").readNullable[Seq[Team]] and
    (JsPath \ "alias").readNullable[Alias] and
    (JsPath \ "description").readNullable[Description] and
    (JsPath \ "recipients").readNullable[Seq[Recipient]] and
    (JsPath \ "actions").readNullable[Seq[Action]] and
    (JsPath \ "source").readNullable[Source] and
    (JsPath \ "tags").readNullable[Seq[Tag]] and
    (JsPath \ "details").readNullable[Map[String, String]] and
    (JsPath \ "entity").readNullable[Entity] and
    (JsPath \ "user").readNullable[User] and
    (JsPath \ "note").readNullable[Note]
  )(Alert.apply _)

  implicit val alertWrites: Writes[Alert] = (
    (JsPath \ "message").write[Message] and
    (JsPath \ "teams").writeNullable[Seq[Team]] and
    (JsPath \ "alias").writeNullable[Alias] and
    (JsPath \ "description").writeNullable[Description] and
    (JsPath \ "recipients").writeNullable[Seq[Recipient]] and
    (JsPath \ "actions").writeNullable[Seq[Action]] and
    (JsPath \ "source").writeNullable[Source] and
    (JsPath \ "tags").writeNullable[Seq[Tag]] and
    (JsPath \ "details").writeNullable[Map[String, String]] and
    (JsPath \ "entity").writeNullable[Entity] and
    (JsPath \ "user").writeNullable[User] and
    (JsPath \ "note").writeNullable[Note]
  )(unlift(Alert.unapply))

  trait CreateAlertResponse
  case class CreateAlertSuccess(val response: POJOSuccessResponse) extends CreateAlertResponse{
    def id = response.getRequestId()
    def data = response.getData()
    def took = response.getTook()
  }
  case class CreateAlertError(val message: String) extends CreateAlertResponse

  object CreateAlertResponse{
    def apply(response: Try[POJOSuccessResponse]) = response match{
      case Success(pojoResponse) => new CreateAlertSuccess(pojoResponse)
      case Failure(excp) => CreateAlertError(excp.getMessage())
    }
  }

  case class CreateAlertRequest(val alert: Alert)(implicit val client: OpsGenieClient){
    def toResponse: CreateAlertResponse = CreateAlertResponse.apply(Try(client.alertV2().createAlert(alert.request)))
  }
}