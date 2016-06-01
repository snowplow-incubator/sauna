
import java.io.DataOutputStream;
import org.apache.commons.io.IOUtils;
import java.io.File;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.net.URL;
import java.util.Map.Entry;
import scala.io.Source
import scala.collection;
import scala.collection.mutable.{ Map => MutableMap }
import javax.net.ssl.HttpsURLConnection;
import java.util.zip.GZIPOutputStream
import gvjava.org.json.JSONObject


class UAResp {

  
  
  def convertTSV(fileName: String):Unit = {
 
    var appToListMap = Map[String, Map[String, List[Airship]]]()

    var listToIdentifierMap = Map[String, List[Airship]]()
    
    var count=0

    for (line <- Source.fromFile(fileName).getLines()) {

      val rawlines = line.split("\t",-1)
      
      val str = rawlines(rawlines.length - 1).substring(0, rawlines(rawlines.length - 1).length() - 2)
      rawlines(rawlines.length - 1) = str

      val lines = for (i <- rawlines) yield i.substring(1, i.length() - 1)
      
      val airship = new Airship(lines(2), lines(3))

      if (!appToListMap.contains(lines(0))) {
        var identifierList: List[Airship] = List(airship)
        listToIdentifierMap = Map[String, List[Airship]](lines(1) -> identifierList)
      } else {
        
        if (listToIdentifierMap.contains(lines(1))) {
          listToIdentifierMap = appToListMap(lines(0))
          var list: List[Airship] = airship :: listToIdentifierMap(lines(1))
          listToIdentifierMap += (lines(1) -> list)
        } else {
          var list: List[Airship] = List(airship)
          listToIdentifierMap += (lines(1) -> list)
        }
      }

      appToListMap += (lines(0) -> listToIdentifierMap)
      count+=1
      
      if(count>=10000000)                  // Urban aiship limit for a single api call
      {
        MaptoRequest(appToListMap)
        appToListMap=Map()
        count=0
      }
        
      
    }
    
    if(appToListMap.size>0)
      MaptoRequest(appToListMap)
    

  }

  def MaptoRequest(appMap: Map[String, Map[String, List[Airship]]]): Unit =
    {
     
      var listNamesToKeyMap =  Map[String, String]()
      for ((appKey, value) <- appMap) {

        for ((listName, value1) <- value) {

          val url = "https://go.urbanairship.com/api/lists/" + listName + "/csv/"
          val obj: URL = new URL(url)
          val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

          //add request header
          con.setRequestMethod("PUT")
          
          val in=new FileInputStream("/home/manoj/urban_airship_config.json")
          val body: String = IOUtils.toString(in, "UTF-8")
          val json=new JSONObject(body)
          val master=json.getJSONObject("data").getJSONObject("parameters").getJSONObject("credentials").get(appKey).asInstanceOf[String]
          val userpass = appKey + ":"+master
          listNamesToKeyMap += (listName -> userpass)
          con.setRequestProperty("Authorization", "Basic " + javax.xml.bind.DatatypeConverter.printBase64Binary(userpass.getBytes()))
          con.setRequestProperty("Accept", "application/vnd.urbanairship+json; version=3")
          con.setRequestProperty("Content-Type", "text/csv")
          con.setRequestProperty("Content-Encoding", "gzip");

          var urlParameters = ""

          for (listElem <- value1) {
            urlParameters = urlParameters + listElem.identifierType + "," + listElem.identifier + "\n"
          }

          con.setDoOutput(true)
          val wr = new DataOutputStream(new GZIPOutputStream(con.getOutputStream()))
          wr.writeBytes(urlParameters)
          wr.flush()
          wr.close()

          val responseCode = con.getResponseCode()
          println("\nSending 'POST' request to URL : " + url + ">>" + responseCode)

        }
      }
      
      val timeout = System.currentTimeMillis() + 15 * 60 * 1000;     //timeout for a list to process (15 minutes)
      while (listNamesToKeyMap.size > 0 && timeout >= System.currentTimeMillis()) {
        
        for ((listName, userpass) <- listNamesToKeyMap) {
         
       
          val status = checkStatus(listName, userpass)
          if (status == "ready")
            listNamesToKeyMap -= listName
        }
        Thread.sleep(30000); // retry for status after 30s
      }

      if (timeout >= System.currentTimeMillis()) {

        for ((listName, userpass) <- listNamesToKeyMap) {
          println("listName " + listName + " upload failed");
        }

      }
     
    }
  def checkStatus(listName: String, userpass: String): String =
    {
      val url = "https://go.urbanairship.com/api/lists/" + listName
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
     
      val bodyJson:JSONObject= new JSONObject(body)
      bodyJson.getString("status")
      
    }

}

object UAResp extends App {
 
  val u = new UAResp()
  u.convertTSV("/home/manoj/data/test.tsv")

}

