package fr.polytechnique.cmap.cnam.utilities

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}

object functions {

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

  def computeDateUsingMonthYear(month: Column, year: Column): Column = {
    unix_timestamp(
      concat(lit("1"), lit("-"), month, lit("-"), year),
      "dd-MM-yyyy"
    ).cast(TimestampType)
  }
}
