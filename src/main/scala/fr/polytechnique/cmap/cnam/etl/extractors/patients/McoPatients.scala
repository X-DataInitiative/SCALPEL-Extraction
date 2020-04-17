// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.util.functions.computeDateUsingMonthYear

private[patients] object McoPatients {

  val inputColumns: List[Column] = List(
    col("NUM_ENQ").as("patientID"),
    col("MCO_B__SOR_MOD").as("SOR_MOD"),
    col("SOR_MOI"),
    col("SOR_ANN")
  )

  val outputColumns: List[Column] = List(
    col("patientID"),
    col("deathDate")
  )

  implicit class McoPatientsDataFrame(data: DataFrame) {

    def getDeathDates(deathCode: Int): DataFrame = {
      // TODO: We may need to check the consistency of {SOR_MOI, SOR_ANN} against SOR_DAT in MCO_C.
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
          "\nhave conflicting DEATH DATES in MCO." +
          "\nTaking Minimum Death Dates")
    */
      result
    }
  }

  def extract(mco: DataFrame, mcoDeathCode: Int = 9): DataFrame = {

    mco
      .select(inputColumns: _*)
      .distinct
      .getDeathDates(mcoDeathCode)
      .select(outputColumns: _*)
  }
}
