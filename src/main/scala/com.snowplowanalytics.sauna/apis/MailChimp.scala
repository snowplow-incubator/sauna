package com.snowplowanalytics.sauna
package apis

import play.api.libs.json.Json
import play.api.Play.current
import play.api.libs.ws._
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder

import akka.actor.ActorRef

import java.io.File

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Failure, Success }
import scala.language.postfixOps
import scala.io.Source.fromFile
import scala.concurrent.ExecutionContext.Implicits.global

class mailChimp(implicit logger: ActorRef) {
//  import MailChimp._

def uploadToMailChimpRequest(bodyJson:String): Unit = {
		val url = "https://us9.api.mailchimp.com/3.0/batches/"
		val client = {
        val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
        new play.api.libs.ws.ning.NingWSClient(builder.build())
      }
		
		val urbanairshipFile = new File(Sauna.respondersLocation + "/mailchimp_config.json")

      val urbanairshipJson = Json.parse(fromFile(urbanairshipFile).mkString)
      val apiKey = (urbanairshipJson \ "data" \ "parameters" \ "apiKey").as[String]
		 val  futureResponse:Future[WSResponse]  = client.url(url).withHeaders("content-type" -> "application/json").withAuth("anystring", apiKey, WSAuthScheme.BASIC).withBody(bodyJson).execute("POST")
		 
     val response = Await.result(futureResponse, 5000 milliseconds)
    
     client.close()
    
      val responseJson = response.json
      
       val id=(responseJson \ "id").as[String]
		
		println(getStatus(id,apiKey))
		
	}

  def getStatus(listId:String, apiKey:String): String = {
		val url = "https://us9.api.mailchimp.com/3.0/batches/"+listId
		
		 val client = {
        val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
        new play.api.libs.ws.ning.NingWSClient(builder.build())
      }
		 
		 val futureResult: Future[String] = client.url(url).withAuth("anyString", apiKey, WSAuthScheme.BASIC).get().map {
          response =>
            (response.json \ "status").as[String]
        }
		 
val result = Await.result(futureResult, 5000 milliseconds)
      client.close()
      result
	}

  
}

object MailChimp {
  val urlPrefix = "https://go.urbanairship.com/api/lists/"
}
