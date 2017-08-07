package fr.polytechnique.cmap.cnam.util

import java.sql.Timestamp
import java.text.{ParsePosition, SimpleDateFormat}
import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{Column, Dataset}


package object functions {

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

  def computeDateUsingMonthYear(month: Column, year: Column): Column = {
    unix_timestamp(
      concat(lit("1"), lit("-"), month, lit("-"), year),
      "dd-MM-yyyy"
    ).cast(TimestampType)
  }

  class DateParseException(val msg: String) extends Exception(msg)

  def parseTimestamp(value: String, pattern: String = "yyyy-MM-dd HH:mm:ss.S"): Option[Timestamp] = {

    if (value == null || value.trim == "") return None

    val dateFormat = new SimpleDateFormat(pattern)
    // Only exact patterns should be accepted
    // https://docs.oracle.com/javase/8/docs/api/java/text/DateFormat.html#setLenient-boolean-
    dateFormat.setLenient(false)

    Option(dateFormat.parse(value.trim, new ParsePosition(0))) match {
      case None => throw new DateParseException(
        s"""Cannot parse Timestamp with pattern "$pattern" from the string value "$value""""
      )
      case Some(date: java.util.Date) => Some(new Timestamp(date.getTime))
    }
  }

  def parseDate(value: String, pattern: String = "yyyy-MM-dd"): Option[java.sql.Date] = {
    parseTimestamp(value, pattern).map(ts => new java.sql.Date(ts.getTime))
  }

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    if(!year.between(1000, 3000) || !month.between(1, 12) || !day.between(1,31) ||
      !hour.between(0, 23) || !minute.between(0, 59) || !second.between(0,59))
      throw new java.lang.IllegalArgumentException("Out of bounds.")

    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }

  def makeTS(timestampParam: List[Integer]): Timestamp = timestampParam match {

    case List(year, month, day) => makeTS(year, month, day)
    case List(year, month, day, hour) => makeTS(year, month, day, hour)
    case List(year, month, day, hour, minute) => makeTS(year, month, day, hour, minute)
    case List(year, month, day, hour, minute, second) => makeTS(year, month, day, hour, minute, second)
    case _ => throw new IllegalArgumentException("Illegal Argument List for makeTS function")
  }

  def daysBetween(end: Timestamp, start: Timestamp): Double = {
    (end.getTime - start.getTime) / (24.0 * 3600.0 * 1000.0)
  }

  def unionDatasets[A](datasets: Dataset[A]*): Dataset[A] = datasets.reduce(_.union(_))

  implicit class IntFunctions(num: Int) {
    def between(lower: Int, upper: Int): Boolean = num >= lower && num <= upper
  }
}
