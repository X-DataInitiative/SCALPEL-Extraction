package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{concat, lit, month, unix_timestamp}
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.getMeanTimestampColumn

object PatientUtils {

  def estimateBirthDateCol(ts1: Column, ts2: Column, birthYear: Column): Column = {
    unix_timestamp(
      concat(
        month(getMeanTimestampColumn(ts1, ts2)),
        lit("-"),
        birthYear
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

}
