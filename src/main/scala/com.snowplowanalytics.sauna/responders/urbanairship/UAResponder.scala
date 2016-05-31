
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
 
    var appMap: Map[String, Map[String, List[Airship]]] = Map[String, Map[String, List[Airship]]]()

    var listMap: Map[String, List[Airship]] = Map[String, List[Airship]]()
    
    var count:Int=0;

    for (line <- Source.fromFile(fileName).getLines()) {

      var rawlines = line.split("	",-1)
      val str = rawlines(rawlines.length - 1).substring(0, rawlines(rawlines.length - 1).length() - 2)
      rawlines(rawlines.length - 1) = str

      var lines = for (i <- rawlines) yield i.substring(1, i.length() - 1)

      var a = new Airship(lines(2), lines(3))

      if (!appMap.contains(lines(0))) {
        var list: List[Airship] = List()
        list ::= a
        listMap = Map[String, List[Airship]](lines(1) -> list)
      } else {
        var str = ""
        listMap = appMap(lines(0))
        var list: List[Airship] = listMap(lines(1))
        list ::= a
        listMap += (lines(1) -> list)
      }

      appMap += (lines(0) -> listMap)
      count+=1
      
      if(count>=10000000)
      {
        MaptoRequest(appMap)
        appMap=Map()
        count=0
      }
        
      
    }

    

  }

  def MaptoRequest(appMap: Map[String, Map[String, List[Airship]]]): Unit =
    {
      var listNamesToKeyMap: Map[String, String] = Map()
      for ((appKey, value) <- appMap) {

        for ((listName, value1) <- value) {

          val url = "https://go.urbanairship.com/api/lists/" + listName + "/csv/"
          val obj: URL = new URL(url)
          val con: HttpsURLConnection = obj.openConnection().asInstanceOf[HttpsURLConnection]

          //add request header
          con.setRequestMethod("PUT")
          
          val userpass: String = appKey + ":"+"22NrhpfZRZ-7MNCoJ0h-ag"
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
      
      var timeout: Long = System.currentTimeMillis() + 15 * 60 * 1000;
      while (listNamesToKeyMap.size > 0 && timeout >= System.currentTimeMillis()) {
        
        for ((listName, userpass) <- listNamesToKeyMap) {
         
       
          var status: String = checkStatus(listName, userpass)
          
          if (status == "ready")
            listNamesToKeyMap -= listName
        }
        Thread.sleep(30000);
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
      var in: InputStream = con.getInputStream()
      var encoding: String = con.getContentEncoding()
      encoding = if (encoding == null) "UTF-8" else encoding
      var body: String = IOUtils.toString(in, encoding)
     
      var bodyJson:JSONObject= new JSONObject(body)
      bodyJson.getString("status")
      
    }

}

object UAResp extends App {
  
  val u = new UAResp()
  u.convertTSV("/home/manoj/data/test.tsv")	
  //u.MaptoRequest(map)  
 // println("test" + u.checkStatus("weekly_offers", "5AkEYOJWQ1yWPS4bLOBW4Q:22NrhpfZRZ-7MNCoJ0h-ag"))
}

