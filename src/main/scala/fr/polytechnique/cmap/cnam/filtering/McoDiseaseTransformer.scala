package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions._

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
    col("`MCO_B.ENT_DAT`").as("stayStartDate"),
    col("`MCO_B.SOR_DAT`").as("stayEndDate")
  )

  val mcoDiseaseColumns: List[Column] =
    mcoInputColumns.filter(x => x.toString.contains("disease"))

  implicit class pmsiMcoDataFrame(df: DataFrame) {

    def extractMcoDisease: DataFrame = {
      df.filter(mcoDiseaseColumns
        .map(diseaseColumn => diseaseColumn.contains(DiseaseCode))
        .reduce((x,y) => x or y)
      )
    }

    def estimateEventDate: DataFrame = {
      val dayInMs = 24L * 60 * 60

      df.withColumn("timeDelta", col("stayLength") * dayInMs)
        .withColumn("estimStayStartDate", col("stayEndDate") - col("timeDelta"))
        .withColumn("estimStayStartDateWoDays",
          unix_timestamp(
            concat_ws("-", col("stayYearEndDate"), col("stayMonthEndDate"), lit("01 00:00:00"))
              - col("timeDelta")
          )
        )
        .withColumn("eventDate",
          coalesce(col("stayStartDate"), col("estimStayStartDate"), col("estimStayStartDateWoDays"))
        )
    }

  }

  override def transform(sources: Sources): Dataset[Event] = {

    val pmsiMco: DataFrame = sources.pmsiMco.get

    import pmsiMco.sqlContext.implicits._

    pmsiMco.select(mcoInputColumns: _*)
      .extractMcoDisease
      .estimateEventDate
      .na.drop(Seq("patientId", "eventDate"))
      .select(outputColumns: _*)
      .as[Event]
  }
}
