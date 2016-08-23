package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/** This transformer looks for CIM10 code starting with C67 in IR_IMB_R
  *
  */
object CancerTransformer extends Transformer[Event] {
  private final val DiseaseCode: String = "C67"

  val imbInputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("MED_NCL_IDT").as("imbEncoding"),
    col("MED_MTF_COD").as("disease"),
    col("IMB_ALD_DTD").as("eventDate")
  )

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

  val mcoNonDiseaseColumns: List[Column] =
    mcoInputColumns.filter(x => !x.toString.contains("disease"))

  val outputColumns: List[Column] = List(
    col("patientID"),
    lit("disease").as("category"),
    lit(DiseaseCode).as("eventId"),
    lit(1.00).as("weight"),
    col("eventDate").cast(TimestampType).as("start"),
    lit(null).cast(TimestampType).as("end")
  )

  implicit class imbDataFrame(df: DataFrame) {

    def extractImbDisease: DataFrame = {
      df.filter(col("imbEncoding") === "CIM10")
        .filter(col("disease") contains DiseaseCode)
    }

  }

  implicit class pmsiMcoDataFrame(df: DataFrame) {

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

    val irImb: DataFrame = sources.irImb.get
    val pmsiMco: DataFrame = sources.pmsiMco.get

    import irImb.sqlContext.implicits._

    val imbDiseases = irImb.select(imbInputColumns: _*)
      .extractImbDisease
      .select(outputColumns: _*)
      .as[Event]

    val mcoDiseases = pmsiMco.select(mcoNonDiseaseColumns: _*, concat_ws(" ", mcoDiseaseColumns: _*)
      .as("disease"))
      .filter(col("disease") contains DiseaseCode)
      .estimateEventDate
      .na.drop(Seq("patientId", "eventDate"))
      .select(outputColumns: _*)
      .as[Event]

    imbDiseases.union(mcoDiseases)
  }
}
