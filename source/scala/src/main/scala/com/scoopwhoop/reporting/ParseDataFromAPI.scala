package com.scoopwhoop.reporting

import com.scoopwhoop.logger.Logger
import net.liftweb.json.JsonAST.{JString, JField}
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.commons.lang3.StringEscapeUtils
import scala.io.Source
import scala.xml.XML


object ParseDataFromAPI {


    case class Posts(posts: List[CommonFunctions.Page])

    val apiForSingleURLParsing = "http://www.scoopwhoop.com/api/v1/?vendor=android&type=single&url="
    val apiForMultipleURLParsing = "http://www.scoopwhoop.com/api/v1.1/?vendor=es&type=articles"

    def getTitleFromURL(url: String): String = {
        val api = apiForSingleURLParsing + url
        val jsonData = Source.fromURL(api, "UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val componentList = for (JField("title", JString(x)) <- jsonObj) yield x
            return StringEscapeUtils.unescapeHtml4(componentList.toString().stripPrefix("List(").stripSuffix(")").replaceAll("\\p{C}", ""))
        }
        catch {
            case pe: ParseException => return ""

        }
    }

    def getCategoryFromURL(url: String): String = {
        val api = apiForSingleURLParsing + url
        val jsonData = Source.fromURL(api, "UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val componentList = for (JField("category", JString(x)) <- jsonObj) yield x
            return componentList.toString().stripPrefix("List(").stripSuffix(")")

        }
        catch {
            case pe: ParseException => return ""
        }
    }

    def getPagesFromAPI(offset: Int, limit: Int): List[CommonFunctions.Page] = {
        implicit val formats = DefaultFormats
        val api = apiForMultipleURLParsing + "&offset=" + offset.toString + "&limit=" + limit.toString + "&content=1"
        val jsonData = Source.fromURL(api, "UTF-8").mkString
        try {
            val jsonObj = parse(jsonData)
            val posts = jsonObj.extract[Posts]
            val pages = posts.posts.map { case (page: CommonFunctions.Page) =>
                CommonFunctions.Page(page.postid,page.title, page.link, page.author, page.pubon, page.s_heading, page.category,
                    page.tags, getTextFromXMLContent(page.content, page.title))
            }
            return pages;
        }
        catch {
            case pe: ParseException => return List()
        }
    }


    def getTextFromXMLContent(xmlContent: String, title: String): String = {
        val xmlString = """<root>""" + xmlContent + """</root>"""
        try {
            val xml = XML.loadString(xmlString.replaceAll( """&""", """&amp;"""))
            val textList = for (n <- xml.child) yield n.text
            val compiledText = textList.map(_.trim()).filter(x => (x != "") && (x != "\u00A0")).toArray.mkString(" ")
            return (compiledText.replaceAll("\\p{C}", "") + " " + StringEscapeUtils.unescapeHtml4(title).replaceAll("\\p{C}", "").replaceAll("\u00A0", ""))

        } catch {
            case e: Exception => {
                Logger.logError("Error in content of title: " + title)
                Logger.logError(e.getMessage)
                return StringEscapeUtils.unescapeHtml4(title).replaceAll("\\p{C}", "").replaceAll("\u00A0", "")
            }

        }
    }


}
