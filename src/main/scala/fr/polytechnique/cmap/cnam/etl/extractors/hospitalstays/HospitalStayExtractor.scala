package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

class HospitalStayExtractor(config: HospitalStayConfig) {

  def extract(sources: Sources): Dataset[Event[HospitalStay]] = {

    val inputCols: List[Column] = List(
      col("NUM_ENQ").as("patientID"),
      col("ETA_NUM").as("value"),
      col("EXE_SOI_DTD").cast(TimestampType).as("start"),
      col("EXE_SOI_DTF").cast(TimestampType).as("end")
    )

    val mco = sources.mco.get

    import mco.sqlContext.implicits._

    mco.select(inputCols: _*)
      .filter(col("start").between(config.minYear, config.maxYear))
      .filter(col("end").between(config.minYear, config.maxYear))
      .distinct()
      .map(HospitalStay.fromRow(_))
  }

}
