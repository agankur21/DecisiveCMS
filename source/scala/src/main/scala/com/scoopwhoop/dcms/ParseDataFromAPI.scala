package com.scoopwhoop.dcms

import net.liftweb.json.JsonAST.{JString, JField}
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.commons.lang3.StringEscapeUtils
import scala.io.Source


object ParseDataFromAPI {
    

    case class Posts(posts: List[CommonFunctions.Page])
    
    val apiForSingleURLParsing = "http://www.scoopwhoop.com/api/v1/?vendor=android&type=single&url="
    val apiForMultipleURLParsing = "http://www.scoopwhoop.com/api/v1.1/?vendor=es&type=articles"
    
    def getTitleFromURL(url:String):String = {
        val api  = apiForSingleURLParsing + url
        val jsonData =Source.fromURL(api,"UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val componentList = for (JField("title", JString(x)) <- jsonObj) yield x
            return StringEscapeUtils.unescapeHtml4(componentList.toString().stripPrefix("List(").stripSuffix(")").replaceAll("\\p{C}", ""))
        }
        catch {
            case pe: ParseException => return ""

        }
    }

    def getCategoryFromURL(url:String):String = {
        val api  = apiForSingleURLParsing + url
        val jsonData =Source.fromURL(api,"UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val componentList = for (JField("category", JString(x)) <- jsonObj) yield x
            return componentList.toString().stripPrefix("List(").stripSuffix(")")

        }
        catch {
            case pe: ParseException => return ""
        }
    }
    
    def getPagesFromAPI(offset:Int,limit:Int): List[CommonFunctions.Page] ={
        implicit val formats = DefaultFormats
        val api = apiForMultipleURLParsing + "&offset=" + offset.toString + "&limit=" + limit.toString
        val jsonData =Source.fromURL(api,"UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val posts = jsonObj.extract[Posts]
            return posts.posts;

        }
        catch {
            case pe: ParseException => return List()
        }
    }
    


}
