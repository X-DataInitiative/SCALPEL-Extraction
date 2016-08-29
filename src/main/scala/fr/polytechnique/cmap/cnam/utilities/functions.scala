package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.Column

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
  * Created by burq on 25/06/16.
  */
object functions {

  def classToSchema[CaseClass : TypeTag](): StructType = {
    ScalaReflection.schemaFor[CaseClass].dataType.asInstanceOf[StructType]
  }

  def minAmongColumns(columns: Column*): Column = {
    if(columns.size == 1) columns(0)
    else columns.reduce {
      (col1: Column, col2: Column) => when(
        col2.isNull, col1
      ).otherwise(
        when(
          col1 <= col2, col1
        ).otherwise(col2)
      )
    }
  }

  def maxAmongColumns(columns: Column*): Column = {
    if(columns.size == 1) columns(0)
    else columns.reduce {
      (col1: Column, col2: Column) => when(
        col2.isNull, col1
      ).otherwise(
        when(
          col1 >= col2, col1
        ).otherwise(col2)
      )
    }
  }
}
