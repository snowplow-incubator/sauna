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

import akka.actor.ActorRef

import responders.urbanairship.UAResponder.Airship

import play.api.libs.json.Json

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
          val userPass = makeRequest(appKey, listName, valueList)
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
  
  def makeRequest(appKey: String, listName: String, values: List[Airship]): String =
    {

      val url = urlPrefix + listName + "/csv/"
      val obj: URL = new URL(url)
      val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

      con.setRequestMethod("PUT")

      val urbanairshipFile = new File(Sauna.respondersLocation + "/urban_airship_config.json")

      val urbanairshipJson = Json.parse(fromFile(urbanairshipFile).mkString)
      val master = (urbanairshipJson \ "data" \ "parameters" \ "credentials" \ appKey).as[String]
      val userpass = appKey + ":" + master

      con.setRequestProperty("Authorization", "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes()))
      con.setRequestProperty("Accept", "application/vnd.urbanairship+json; version=3")
      con.setRequestProperty("Content-Type", "text/csv")
      con.setRequestProperty("Content-Encoding", "gzip")

      val urlParameters = new StringBuffer()

      values.foreach((listElem: Airship) => urlParameters.append(listElem.identifierType + "," + listElem.identifier).append("\n"))

      con.setDoOutput(true)
      val wr = new DataOutputStream(new GZIPOutputStream(con.getOutputStream))
      wr.writeBytes(urlParameters.toString)
      wr.flush
      wr.close

      val responseCode = con.getResponseCode
      val response = con.getResponseMessage

      if (responseCode != 202)
        logger ! Notification("upload failed for list" + listName)
      else
        logger ! Notification("Sending 'POST' request to URL : " + url + ">>" + responseCode + "<<" + response)
        
      userpass
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
      val url = urlPrefix + listName
      val obj: URL = new URL(url)
      val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

      //add request header
      con.setRequestMethod("GET")
      con.setRequestProperty("Authorization", "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes()))
      con.setRequestProperty("Accept", "application/vnd.urbanairship+json; version=3")

      val response = fromInputStream(con.getInputStream).mkString
      val bodyJson = Json.parse(response)
      (bodyJson \ "status").as[String]
    }
  
}

object UrbanAirship {
  val urlPrefix = "https://go.urbanairship.com/api/lists/"
}