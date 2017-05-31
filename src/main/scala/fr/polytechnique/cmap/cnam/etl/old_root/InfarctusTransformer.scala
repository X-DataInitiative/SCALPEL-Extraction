package fr.polytechnique.cmap.cnam.etl.old_root

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Created by burq on 19/01/17.
  */
object InfarctusTransformer extends DiseaseTransformer {

  val DiseaseCodes = List(
    "I110",
    "I130",
    "I132",
    "I139",
    "K761",
    "J81"
  )

  override val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit("infarctus").as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  val mcoInputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("`MCO_B.DGN_PAL`").as("diseasePal"),
    col("`MCO_B.DGN_REL`").as("diseaseRel"),
    col("`MCO_D.ASS_DGN`").as("diseaseAss"),
    col("`MCO_B.SOR_MOI`").as("stayMonthEndDate"),
    col("`MCO_B.SOR_ANN`").as("stayYearEndDate"),
    col("`MCO_B.SEJ_NBJ`").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  )

  implicit class InfarctusDataFrame(df: DataFrame) {

    def extractDisease: DataFrame = {
      df.where(
        col("diseasePal").startsWith("I50").or(
        DiseaseCodes.map(col("diseasePal").startsWith).reduce(_ or _)
          and (col("diseaseRel").startsWith("I50") or col("diseaseAss").startsWith("I50"))
        )
      )
    }
  }

  override def transform(sources: Sources): Dataset[Event] = {

    val pmsiMco: DataFrame = sources.pmsiMco.get

    import McoDiseaseTransformer.pmsiMcoDataFrame
    import pmsiMco.sqlContext.implicits._

    pmsiMco.select(mcoInputColumns: _*)
      .extractDisease
      .estimateStayStartTime
      .select(outputColumns: _*)
      .as[Event]
  }
}
