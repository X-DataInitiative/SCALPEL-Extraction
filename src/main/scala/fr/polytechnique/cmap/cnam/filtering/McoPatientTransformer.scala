package fr.polytechnique.cmap.cnam.filtering

import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.functions._

case class McoPatient(patientID: String, deathDate: Timestamp)

object McoPatientTransformer extends Transformer[McoPatient] with PatientsTransformer {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("`MCO_B.SOR_MOD`").as("SOR_MOD"),
    col("SOR_MOI"),
    col("SOR_ANN"))

  val outputColumns: List[Column] = List(
    col("patientID"),
    col("deathDate")
  )

  implicit class PatientTransformer(data: DataFrame) {

    def getDeathDates: DataFrame = {
      // TODO: We may need to check the consistency of {SOR_MOI, SOR_ANN} against SOR_DAT in MCO_C.
      val deathDates: DataFrame = data.filter(col("SOR_MOD") === DeathCode)
        .withColumn("deathDate", computeDateUsingMonthYear(col("SOR_MOI"), col("SOR_ANN")))

      val result = deathDates
        .groupBy("patientID")
        .agg(
          countDistinct(col("deathDate")).as("count"),
          min(col("deathDate")).as("deathDate")
        ).cache()

      val conflicts = result
        .filter(col("count") > 1)
        .select(col("patientID"))
        .distinct
        .collect

      if(conflicts.length != 0)
        Logger.getLogger(getClass).warn("The patients in " +
          conflicts.deep.mkString("\n") +
          "\nhave conflicting DEATH DATES in MCO." +
          "\nTaking Minimum Death Dates")

      result
    }
  }

  override def transform(sources: Sources): Dataset[McoPatient] = {

    val pmsiMco: DataFrame = sources.pmsiMco.get
    import pmsiMco.sqlContext.implicits._

    pmsiMco.select(inputColumns: _*)
      .distinct
      .getDeathDates
      .select(outputColumns: _*)
      .as[McoPatient]
  }
}
