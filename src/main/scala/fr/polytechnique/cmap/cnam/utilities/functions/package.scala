package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}


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

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    if(!year.between(1000, 3000) || !month.between(1, 12) || !day.between(1,31) ||
      !hour.between(0, 23) || !minute.between(0, 59) || !second.between(0,59))
      throw new java.lang.IllegalArgumentException("Out of bounds.")

    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }

  def daysBetween(end: Timestamp, start: Timestamp): Double = {
    (end.getTime - start.getTime) / (24.0 * 3600.0 * 1000.0)
  }

  implicit class IntFunctions(num: Int) {
    def between(lower: Int, upper: Int): Boolean = num >= lower && num <= upper
  }
}
