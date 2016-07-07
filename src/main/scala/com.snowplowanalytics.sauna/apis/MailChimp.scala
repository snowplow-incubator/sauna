package com.snowplowanalytics.sauna
package apis

import play.api.libs.json.Json
import play.api.libs.json.JsArray

import play.api.libs.ws._
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder

import akka.actor.ActorRef

import org.apache.commons.io.IOUtils

import java.io.File
import java.util.zip.GZIPInputStream
import java.net.URL
import javax.net.ssl.HttpsURLConnection

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.language.postfixOps
import scala.io.Source.fromFile
import scala.concurrent.ExecutionContext.Implicits.global

import loggers.Logger.Notification

class mailChimp(implicit logger: ActorRef) {
  
  /**
   * 
   * sends the request to Mailchimp using batch operations API and processes to reponse to log any failures
   * @param bodyJson the json to be sent as the part of the request body
   * 
   **/
  
  def uploadToMailChimpRequest(bodyJson: String): Unit = {
    val url = "https://us9.api.mailchimp.com/3.0/batches/"
    val urbanairshipFile = new File(Sauna.respondersLocation + "/mailchimp_config.json")
    val urbanairshipJson = Json.parse(fromFile(urbanairshipFile).mkString)
    val apiKey = (urbanairshipJson \ "data" \ "parameters" \ "apiKey").as[String]
    val futureResponse: Future[WSResponse] = utils.wsClient.url(url).withHeaders("content-type" -> "application/json").withAuth("anystring", apiKey, WSAuthScheme.BASIC).withBody(bodyJson).execute("POST")
    val response = Await.result(futureResponse, 5000 milliseconds)
    val id = (response.json \ "id").as[String]
    var status2 = ""
    var url2 = ""
    while (status2 != "finished") {
      val (status, url1) = getStatus(id, apiKey)
      Thread.sleep(1000)
      status2 = status
      url2 = url1
    }
    getErrorStatus(url2)
  }

  /**
   * 
   * gets the individual status from the response body url for each of the operations and logs them if there is a failure
   * @param responsebody url 
   * 
  * */
  
  def getErrorStatus(url: String): Unit = {
    val obj = new URL(url)
    val con = obj.openConnection().asInstanceOf[HttpsURLConnection]
    con.setRequestMethod("GET")
    val in: GZIPInputStream = new GZIPInputStream(con.getInputStream())
    val encoding = "UTF-8"
    val body1 = IOUtils.toString(in, encoding)

    val jsonBody = body1.substring(body1.indexOf('['))
    val jsonFormattedString = jsonBody.trim()
    val json = Json.parse(jsonFormattedString)
    val count = json.as[JsArray].value.size

    for (i <- 0 to count - 1) {
      val jsonElement = json(i)

      val statusCode = (jsonElement \ "status_code").as[Int]
      if (statusCode != 200) {
        val opId = (jsonElement \ "operation_id").as[String]
        val Array(listId, emailId) = opId.split("_")
        logger ! Notification("Upload for emailId " + emailId + " to listid " + listId + " failed with statuscode " + statusCode)
      }

    }

  }
  
/**
 * gets the status of the list and the response body url once its ready
 * @ param listId for which status is needed
 * @ param apiKey needed to make the request
 * @ return tuple of status and responseBody url   
 **/
  
  def getStatus(listId: String, apiKey: String): (String, String) = {
    val url = "https://us9.api.mailchimp.com/3.0/batches/" + listId

    val futureResult: Future[String] = utils.wsClient.url(url).withAuth("anyString", apiKey, WSAuthScheme.BASIC).get().map {
      response =>
        (response.json).toString
    }

    val responseString = Await.result(futureResult, 5000 milliseconds)
    val responseJson = Json.parse(responseString)
    val result = (responseJson \ "status").as[String]
    val responseUrl = (responseJson \ "response_body_url").as[String]
    (result, responseUrl)
  }

}

object MailChimp {
  val urlPrefix = "https://go.urbanairship.com/api/lists/"
}
