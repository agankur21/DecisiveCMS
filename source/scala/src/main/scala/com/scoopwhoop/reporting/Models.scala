package com.scoopwhoop.reporting

object Models {

    case class Page(postid: String, title: String, link: String, author: String, pubon: String,
                    s_heading: String, category: List[String], tags: List[String], content: String)

    case class Posts(posts: List[Page])

    case class EventProperties(where: Option[String], desc: Option[String])

    case class ReferrerInfo(url: Option[String], domain: Option[String])

    case class EventLog(product_id: String, event_type: String, time: Long, post_id: String, cookie_id: Option[String],
                        event_properties: EventProperties, user_agent: Option[String], referrer_info: ReferrerInfo,
                        author: Option[String], category: Option[String], ip: Option[String])

}
