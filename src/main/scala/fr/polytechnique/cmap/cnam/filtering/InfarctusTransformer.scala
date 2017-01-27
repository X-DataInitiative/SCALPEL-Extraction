package fr.polytechnique.cmap.cnam.filtering
import fr.polytechnique.cmap.cnam.filtering.McoDiseaseTransformer.{DiseaseCode, outputColumns}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

/**
  * Created by burq on 19/01/17.
  */
object InfarctusTransformer extends DiseaseTransformer {

  val DiseaseCodes = List(
    "I21",
    "I22"
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
    col("`MCO_B.SOR_MOI`").as("stayMonthEndDate"),
    col("`MCO_B.SOR_ANN`").as("stayYearEndDate"),
    col("`MCO_B.SEJ_NBJ`").as("stayLength"),
    col("`ENT_DAT`").as("stayStartTime").cast("Timestamp"),
    col("`SOR_DAT`").as("stayEndDate").cast("Timestamp")
  )

  val mcoDiseaseColumn: Column = col("diseasePal")

  implicit class InfarctusDataFrame(df: DataFrame) {

    def extractDisease: DataFrame = {
      df.filter(
        mcoDiseaseColumn.startsWith("I21") or
        mcoDiseaseColumn.startsWith("I22")
      )
    }

  }

  override def transform(sources: Sources): Dataset[Event] = {

    val pmsiMco: DataFrame = sources.pmsiMco.get

    import pmsiMco.sqlContext.implicits._
    import McoDiseaseTransformer.pmsiMcoDataFrame

    pmsiMco.select(mcoInputColumns: _*)
      .extractDisease
      .estimateStayStartTime
      .select(outputColumns: _*)
      .as[Event]
  }
}
