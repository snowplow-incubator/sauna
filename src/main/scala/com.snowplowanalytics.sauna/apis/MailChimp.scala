package com.snowplowanalytics.sauna
package apis

import play.api.libs.json.Json
import play.api.libs.json.JsString
import play.api.libs.json.JsArray
import play.api.Play.current
import play.api.libs.ws._
import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder

import akka.actor.ActorRef

import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import java.io.File
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.io.FileInputStream
import java.io.BufferedInputStream
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

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
		//println("id is"+id)
		var status2=""
		var url2=""
		while(status2!="finished")
		{
		  val (status, url1) = getStatus(id,apiKey)
		  Thread.sleep(1000)
		  status2=status
		  url2=url1
	//	  println(status2+" "+url2)
		}

//		getErrorStatus(url2)

		
		getErrorStatus(url2)
	}

//
//def getErrorStatus2(url:String):Unit={
//   val client:HttpClient = new HttpClient();
//
//    // Create a method instance.
//    val method:GetMethod = new GetMethod(url);
//    
//    // Provide custom retry handler is necessary
//   
//
//   
//      // Execute the method.
//     client.executeMethod(method);
//
//    /*  if (statusCode != HttpStatus.SC_OK) {
//        System.err.println("Method failed: " + method.getStatusLine());
//      }*/
//
//      // Read the response body.
//      
//
//      // Deal with the response.
//      // Use caution: ensure correct character encoding and is not binary data
//     // val body = method.getResponseBody()
//    // val bodystr= new String(body)
// // val myInputStream:InputStream  = new ByteArrayInputStream(bodystr.trim().getBytes()); 
//    // val st:ByteArrayInputStream=new ByteArrayInputStream(new String(body).getBytes());
//	val in:GZIPInputStream  = new GZIPInputStream(method.getResponseBodyAsStream())
//val encoding = "UTF-8"
//			      val body1 = IOUtils.toString(in, encoding)
//			      println(body1.trim())
//}



def getErrorStatus(url:String):Unit={
  
  val obj = new URL(url);
		val con = obj.openConnection().asInstanceOf[HttpsURLConnection];

		con.setRequestMethod("GET");
val in:GZIPInputStream  = new GZIPInputStream(con.getInputStream())
val encoding = "UTF-8"
			      val body1 = IOUtils.toString(in, encoding)

val jsonBody=body1.substring(body1.indexOf('['))
  val jsonFormattedString = jsonBody.trim()
  
  val json=Json.parse(jsonFormattedString)
  //println(json)
  val count=json.as[JsArray].value.size
  
  for( i <- 0 to count-1)
  {
    val jsonElement=json(i)
  
  val statusCode=(jsonElement \ "status_code").as[Int]
    if(statusCode!=200)
    {
      val opId=(jsonElement \ "operation_id").as[String]
      val Array(listId,emailId)=opId.split("_")
      println("Upload for emailId "+emailId+" to listid "+listId+" failed with statuscode "+statusCode)
    }
    
  }
  
}

  def getStatus(listId:String, apiKey:String): (String,String) = {
		val url = "https://us9.api.mailchimp.com/3.0/batches/"+listId
		
		 val client = {
        val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
        new play.api.libs.ws.ning.NingWSClient(builder.build())
      }
		 
		 val futureResult: Future[String] = client.url(url).withAuth("anyString", apiKey, WSAuthScheme.BASIC).get().map {
          response =>
            (response.json).toString
        }
		 
    val responseString = Await.result(futureResult, 5000 milliseconds)
    val responseJson = Json.parse(responseString)
      client.close()
//     println(responseString)
      val result = (responseJson \ "status").as[String]
    val responseUrl= (responseJson \ "response_body_url").as[String]
      (result,responseUrl)
	}

  
}

object MailChimp {
  val urlPrefix = "https://go.urbanairship.com/api/lists/"
}
