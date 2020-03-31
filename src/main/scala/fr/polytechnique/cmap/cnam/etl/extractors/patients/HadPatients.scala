package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.util.functions.computeDateUsingMonthYear

private[patients] object HadPatients {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("HAD_B__SOR_MOD").as("SOR_MOD"),
    col("HAD_B__SOR_MOI").as("SOR_MOI"),
    col("HAD_B__SOR_ANN").as("SOR_ANN")
  )

  val outputColumns: List[Column] = List(
    col("patientID"),
    col("deathDate")
  )

  implicit class HadPatientsDataFrame(data: DataFrame) {

    def getDeathDates(deathCode: Int): DataFrame = {
      val deathDates: DataFrame = data.filter(col("SOR_MOD") === deathCode)
        .withColumn("deathDate", computeDateUsingMonthYear(col("SOR_MOI"), col("SOR_ANN")))

      val result = deathDates
        .groupBy("patientID")
        .agg(
          countDistinct(col("deathDate")).as("count"),
          min(col("deathDate")).as("deathDate")
        ).cache()
      /*
      val conflicts = result
        .filter(col("count") > 1)
        .select(col("patientID"))
        .distinct
        .collect

      if(conflicts.length != 0)
        Logger.getLogger(getClass).warn("The patients in " +
          conflicts.deep.mkString("\n") +
          "\nhave conflicting DEATH DATES in HAD." +
          "\nTaking Minimum Death Dates")
    */
      result
    }
  }

  def extract(had: DataFrame, hadDeathCode: Int = 9): DataFrame = {

    had
      .select(inputColumns: _*)
      .distinct
      .getDeathDates(hadDeathCode)
      .select(outputColumns: _*)
  }
}
