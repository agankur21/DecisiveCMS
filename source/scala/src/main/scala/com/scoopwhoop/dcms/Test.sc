import java.text.NumberFormat

val a = 1


def getDateString(input:String): String ={
    val pattern = "yyyyMMdd";
    val dateTime  = DateTime.parse(input, DateTimeFormat.forPattern(pattern));
    dateTime.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
}





val startEndDate  = Array(Array("20150601", "20150607"))
val Array(startDate,endDate) = startEndDate(0).map(getDateString)

def getNumberFromPercentage(input:String):Double = {
    val format = NumberFormat.getPercentInstance();
    val number = format.parse(input) ;
    number.doubleValue() ;
}

getNumberFromPercentage("83.42%")


val timestamp :Long =  1433116803 * 1000L
val date_time = new DateTime(timestamp);
val chrin = date_time.getChronology
date_time.toString(DateTimeFormat.forPattern("yyyy-MM-dd"))
val t = "2015-01-03" > "2015-01-04"



