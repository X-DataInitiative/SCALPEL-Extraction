package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.sources.Sources

/**
  * This transformer looks for CIM10 codes containing DiseaseTransformer.DiseaseCode in PMSI expected.MCO.
  */
object McoDiseaseTransformer extends DiseaseTransformer {

  val mcoInputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("`MCO_D.ASS_DGN`").as("diseaseAss"),
    col("`MCO_UM.DGN_PAL`").as("diseasePalUm"),
    col("`MCO_UM.DGN_REL`").as("diseaseRelUm"),
    col("`MCO_B.DGN_PAL`").as("diseasePal"),
    col("`MCO_B.DGN_REL`").as("diseaseRel"),
    col("`MCO_B.SOR_MOI`").as("stayMonthEndDate"),
    col("`MCO_B.SOR_ANN`").as("stayYearEndDate"),
    col("`MCO_B.SEJ_NBJ`").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  )

  val mcoDiseaseColumns: List[Column] = List(
    col("diseaseAss"),
    col("diseasePalUm"),
    col("diseaseRelUm"),
    col("diseasePal"),
    col("diseaseRel")
  )

  implicit class pmsiMcoDataFrame(df: DataFrame) {

    def extractMcoDisease: DataFrame = {
      df.filter(
        mcoDiseaseColumns.map(_.contains(DiseaseCode)).reduce(_ or _)
      )
    }

    /**
      * Estimate the stay starting date according to the different versions of PMSI MCO
      * Please note that in the case of early MCO (i.e. < 2009), the estimator is
      * date(01/month/year) - number of days of the stay.
      * This estimator is quite imprecise, and if one patient has several stays of the same
      * length in one month, it results in duplicate events.
      */
    def estimateStayStartTime: DataFrame = {
      val dayInMs = 24L * 60 * 60
      val timeDelta: Column = coalesce(col("stayLength"), lit(0)) * dayInMs
      val estimate: Column = (col("stayEndDate").cast(LongType) - timeDelta).cast(TimestampType)
      val roughEstimate: Column = (
          unix_timestamp(
            concat_ws("-", col("stayYearEndDate"), col("stayMonthEndDate"), lit("01 00:00:00"))
          ).cast(LongType) - timeDelta
        ).cast(TimestampType)

      df.withColumn(
        "eventDate",
        coalesce(col("stayStartTime"), estimate, roughEstimate)
      )
    }

  }

  override def transform(sources: Sources): Dataset[Event] = {

    val pmsiMco: DataFrame = sources.pmsiMco.get

    import pmsiMco.sqlContext.implicits._

    pmsiMco.select(mcoInputColumns: _*)
      .extractMcoDisease
      .estimateStayStartTime
      .select(outputColumns: _*)
      .as[Event]
  }
}
