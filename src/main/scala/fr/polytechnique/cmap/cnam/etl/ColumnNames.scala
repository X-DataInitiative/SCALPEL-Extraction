package fr.polytechnique.cmap.cnam.etl

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait ColumnNames {

  type ColName = String

  implicit class RichColName(colName: ColName) {
    def toCol: Column = col(colName)
  }
}
