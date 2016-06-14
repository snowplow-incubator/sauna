package com.snowplowanalytics.sauna
package apis



import java.io.DataOutputStream
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }
import org.apache.commons.io.IOUtils
import java.io.File
import java.util.Properties
import java.io.FileInputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.util.ArrayList
import java.io.InputStream
import java.util.HashMap
import java.util.Iterator
import scala.io.Source.fromFile
import java.net.URL
import akka.actor.{ActorRef, Props}
import java.util.Map.Entry
import scala.io.Source
import scala.collection
import scala.collection.mutable.{ Map => MutableMap }
import javax.net.ssl.HttpsURLConnection
import java.util.zip.GZIPOutputStream
import gvjava.org.json.JSONObject
import responders.urbanairship.UAResponder.Airship
import scala.collection.immutable.ListMap
import play.api.libs.json.Json
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import scala.concurrent.ExecutionContext.Implicits.global

class UrbanAirship(implicit logger: ActorRef) {
  import UrbanAirship._
  
//  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
  def MaptoRequest(appMap: Map[String, Map[String, List[Airship]]]): Unit =
    {

      var listNamesToKeyMap = Map[String, String]()
      for ((appKey, value) <- appMap) {
        var count = 0;
        for ((listName, value1) <- value) {
          val f = Future {

            val url = urlPrefix + listName + "/csv/"
            val obj: URL = new URL(url)
            val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

            con.setRequestMethod("PUT")
            
             val urbanairshipFile = new File(Sauna.respondersLocation+"/urban_airship_config.json")

            val urbanairshipJson = Json.parse(fromFile(urbanairshipFile).mkString)
            val master = (urbanairshipJson \ "data" \ "parameters" \ "credentials" \ appKey ).as[String]
            val userpass = appKey + ":" + master
            
            listNamesToKeyMap += (listName -> userpass)
            con.setRequestProperty("Authorization", "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes()))
            con.setRequestProperty("Accept", "application/vnd.urbanairship+json; version=3")
            con.setRequestProperty("Content-Type", "text/csv")
            con.setRequestProperty("Content-Encoding", "gzip")

            val urlParameters = new StringBuffer()

            for (listElem <- value1) {
              urlParameters.append(listElem.identifierType + "," + listElem.identifier).append("\n")
            }

            con.setDoOutput(true)
            val wr = new DataOutputStream(new GZIPOutputStream(con.getOutputStream()))
            wr.writeBytes(urlParameters.toString())
            wr.flush()
            wr.close()


            val responseCode = con.getResponseCode()
            val response = con.getResponseMessage()
            
            println("Sending 'POST' request to URL : " + url + ">>" + responseCode + "<<" + response)
            if (responseCode != 202)
              println("make logger for error")
            count += 1

          }
          f.onComplete {
            case Success(value) => 
            case Failure(e) => e.printStackTrace
          }
        }

        while (count < value.size) {

          Thread.sleep(1000);
        }
      }
     
      val timeout = System.currentTimeMillis() + 15 * 60 * 1000;
    

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
          if(listNamesToKeyMap.size > 0)
            Thread.sleep(30000)
        }

      }

      if (timeout >= System.currentTimeMillis()) {

        for ((listName, userpass) <- listNamesToKeyMap) {
          println("listName " + listName + " upload failed");
        }

      }
      println("upload complete")
    }
  def checkStatus(listName: String, userpass: String): String =
    {
      val url = urlPrefix + listName
      val obj: URL = new URL(url)
      val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

      //add request header
      con.setRequestMethod("GET")

      con.setRequestProperty("Authorization", "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes()))
      con.setRequestProperty("Accept", "application/vnd.urbanairship+json; version=3")
      val in: InputStream = con.getInputStream()
      var encoding: String = con.getContentEncoding()
      encoding = if (encoding == null) "UTF-8" else encoding
      val body: String = IOUtils.toString(in, encoding)

      val bodyJson: JSONObject = new JSONObject(body)
      bodyJson.getString("status")
      

    }

}

object UrbanAirship  {

  val urlPrefix = "https://go.urbanairship.com/api/lists/"

}


