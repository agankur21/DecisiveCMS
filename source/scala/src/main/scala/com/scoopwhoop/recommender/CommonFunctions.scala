package com.scoopwhoop.recommender

import com.datastax.spark.connector.CassandraRow
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object CommonFunctions {

    val eventScore = Map("Desktop PVs" -> 1, "Mobile PVs" -> 1, "itemClick" -> 4, "shareClick" -> 8)

    def nnHash(tag: String): Int = (tag.hashCode & 0x7FFFFF).toInt

    def getDate(timestamp: Long): String = {
        if (timestamp == 0) return ""
        val date_time = new DateTime(timestamp * 1000L);
        return date_time.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
    }
    
    def isGreaterOrEqual(timestamp: Long, date: String): Boolean = {
        return (getDate(timestamp) >= date)
    }

    def isLowerOrEqual(timestamp: Long, date: String): Boolean = {
        return (getDate(timestamp) <= date)
    }

    def getStringFromCassandraRow(row: CassandraRow, element: String): String = {
        if (row.isNullAt(element)) "" else row.getString(element)
    }
}
