package com.scoopwhoop.dcms
import java.util.Calendar

object Logger extends Serializable{

    def logInfo(message:String) :Unit  = {
        val time = Calendar.getInstance.getTime;
        println(s"[$time] [INFO] [$message]");
    }

    def logWarning(message:String) :Unit  = {
        val time = Calendar.getInstance.getTime;
        println(s"[$time] [WARNING] [$message]");
    }

    def logError(message:String) :Unit  = {
        val time = Calendar.getInstance.getTime;
        println(s"[$time] [ERROR] [$message]");
    }
}
