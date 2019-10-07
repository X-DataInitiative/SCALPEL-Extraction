// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

trait ColumnNames {

  type ColName = String

  implicit class RichColName(colName: ColName) {
    def toCol: Column = col(colName)
  }

}
