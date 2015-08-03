package com.scoopwhoop.recommender

object CommonFunctions {
    
    val eventScore = Map("Desktop PVs" -> 1,"Mobile PVs" -> 1,"itemClick" -> 5, "shareClick" -> 10)

    def nnHash(tag: String) : Int= (tag.hashCode & 0x7FFFFF).toInt
}
