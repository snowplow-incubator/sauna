package com.snowplowanalytics.sauna
package apis

import java.io.DataOutputStream
import java.io.File
import java.io.InputStream
import java.net.URL
import javax.net.ssl.HttpsURLConnection
import java.util.zip.GZIPOutputStream

import scala.io.Source.fromInputStream
import scala.io.Source.fromFile
import scala.util.{ Failure, Success }
import scala.collection.mutable.{ Map => MutableMap }
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

import akka.actor.ActorRef
 
import play.api.Play.current
import play.api.libs.ws._
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder

import responders.urbanairship.UAResponder.Airship

import play.api.libs.json.Json

//import play.api.mvc._
//import play.api.Play.current
//import play.api.libs.ws._

import loggers.Logger.Notification

class urbanAirship(implicit logger: ActorRef) {
  import UrbanAirship._

  /**
   * Converts the gouped data in the form of a map into upload requests to UrbanAirship and checks the status of the operation
   *
   * @param is the map of data grouped by appKeys and then by listNames
   */

  def maptoRequest(appMap: Map[String, Map[String, List[Airship]]]): Unit =
    {

      var listNamesToKeyMap = Map[String, String]()
      for ((appKey, value) <- appMap) {

        for ((listName, valueList) <- value) {

          val userPass = makeRequest(appKey, listName, valueList, listNamesToKeyMap)
          listNamesToKeyMap += (listName -> userPass)

        }

      }

      val timeout = System.currentTimeMillis() + 15 * 60 * 1000

      while (listNamesToKeyMap.size > 0 && timeout >= System.currentTimeMillis()) {

        for ((listName, userpass) <- listNamesToKeyMap) {
          val f = Future {

            val status = checkStatus(listName, userpass)
            if (status == "ready") {
              listNamesToKeyMap -= listName
            }
          }
          f.onComplete {
            case Success(value) =>
            case Failure(e) => e.printStackTrace
          }
          if (listNamesToKeyMap.size > 0)
            Thread.sleep(30000)
        }

      }

      if (timeout >= System.currentTimeMillis()) {

        for ((listName, userpass) <- listNamesToKeyMap) {
          logger ! Notification("listName " + listName + " upload failed")
        }

      }
      logger ! Notification("Upload Complete !")
    }

  /**
   * Converts the gouped data in the form of a map into upload requests to UrbanAirship and checks the status of the operation
   *
   * @param appKey is the Application Key of the App in UrbanAirship
   * @param listName is the name for the list to which the uploads are directed
   * @param valueMap is the List of Identifier and IdentifierTypes for the upload
   * @return userpass is the authentication key which is the combination of Appkey and MasterKey needed to make the request
   */
  def makeRequest(appKey: String, listName: String, values: List[Airship], listNamesToKeyMap: Map[String, String]): String =
    {
  
      val client = {
        val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
        new play.api.libs.ws.ning.NingWSClient(builder.build())
      }
      
      val url = urlPrefix + listName + "/csv/"
      val urbanairshipFile = new File(Sauna.respondersLocation + "/urban_airship_config.json")

      val urbanairshipJson = Json.parse(fromFile(urbanairshipFile).mkString)
      val master = (urbanairshipJson \ "data" \ "parameters" \ "credentials" \ appKey).as[String]
      val userPass = appKey + ":" + master
      val urlParameters = new StringBuffer()

      values.foreach((listElem: Airship) => urlParameters.append(listElem.identifierType + "," + listElem.identifier).append("\n"))

      
      val  futureResponse:Future[WSResponse]  = client.url(url).withHeaders("Accept" -> "application/vnd.urbanairship+json; version=3")
        .withHeaders("Content-Type" -> "text/csv").withAuth(appKey, master, WSAuthScheme.BASIC).withBody(urlParameters.toString).execute("PUT")
     
      val response = Await.result(futureResponse, 5000 milliseconds)
        
      val responseFlag = (response.json \ "ok").as[Boolean]
  
      client.close()
      
     if (responseFlag)
        logger ! Notification("Sending 'POST' request to URL : " + url + "was sucessful" )
      else
        logger ! Notification("upload to list" + listName + "failed")

      userPass
    }

  
  
  
  /**
   * Checks to see if the upload request to a list is done with processing
   *
   * @param listName the name of the list for which we check the status
   * @param userpass the key required to make the request
   * @return status of the list wheather it has completed processing or is still processing
   */

  def checkStatus(listName: String, userpass: String): String =
    {

      val Array(appKey, master) = userpass.split(":")

      val client = {
        val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
        new play.api.libs.ws.ning.NingWSClient(builder.build())
      }

      val futureResult: Future[String] = client.url(urlPrefix + listName).withHeaders("Accept" -> "application/vnd.urbanairship+json; version=3")
        .withAuth(appKey, master, WSAuthScheme.BASIC).get().map {
          response =>
            (response.json \ "status").as[String]
        }

      val result = Await.result(futureResult, 5000 milliseconds)
      client.close()
      result
    }

}

object UrbanAirship {
  val urlPrefix = "https://go.urbanairship.com/api/lists/"
}