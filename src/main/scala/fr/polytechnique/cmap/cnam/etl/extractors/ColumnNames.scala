package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

@deprecated("What even is this")
trait ColumnNames {

  type ColName = String

  implicit class RichColName(colName: ColName) {
    def toCol: Column = col(colName)
  }
}
