package fr.polytechnique.cmap.cnam.etl.extractors.patients

import org.apache.spark.sql.functions.{coalesce, col, when, year}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.etl.patients.Patient
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object AllPatientExtractor {

  def extract(sources: Sources): Dataset[Patient] = {

    val irBenPatients: Dataset[Patient] = IrBenPatients.extract(sources).as("irBen")
    val dcirPatients: Dataset[Patient] = DcirPatients.extract(sources).as("dcir")
    val mcoPatients: Dataset[Patient] = McoPatients.extract(sources).as("mco")

    val joinColumn: Column = coalesce(col("irBen.patientID"), col("mco.patientID"))

    val patients: DataFrame = irBenPatients
      .join(mcoPatients, col("irBen.patientID") === col("mco.patientID"), "outer")
      .join(dcirPatients, joinColumn === col("dcir.patientID"), "outer")

    val patientID: Column = coalesce(
      col("dcir.patientID"),
      col("irBen.patientID"),
      col("mco.patientID")
    )

    val gender: Column = coalesce(
      col("irBen.gender"),
      col("dcir.gender")
    )

    val birthDate: Column = coalesce(
      col("irBen.birthDate"),
      col("dcir.birthDate")
    )

    val deathDate: Column = coalesce(
      when(
        validateDeathDate(col("irBen.deathDate"), birthDate),
        col("irBen.deathDate")
      ),
      when(
        validateDeathDate(col("dcir.deathDate"), birthDate),
        col("dcir.deathDate")
      ),
      when(
        validateDeathDate(col("mco.deathDate"), birthDate),
        col("mco.deathDate")
      ))

    import patients.sparkSession.implicits._

    val birthYearErrors = List(-1, 0, 1, 1600)

    val filteredPatients = patients.where(birthDate.isNotNull && !year(birthDate).isin(birthYearErrors: _*)).select(
      patientID.as("patientID"),
      gender.as("gender"),
      birthDate.as("birthDate"),
      deathDate.as("deathDate")
    ).as[Patient]

    sources.mcoCe match {
      case None => filteredPatients.as[Patient]
      case Some(_) =>
        val mcocePatients: Dataset[Patient] = McocePatients.extract(sources).as("mco_ce")

        val allPatients = filteredPatients.as("patients")
          .join(mcocePatients, col("patients.patientID") === col("mco_ce.patientID"), "full")

        val idCol = coalesce(col("patients.patientID"), col("mco_ce.patientID"))
          .alias("patientID")
        val genderCol = coalesce(col("patients.gender"), col("mco_ce.gender"))
          .alias("gender")
        val birthDateCol = coalesce(col("patients.birthDate"), col("mco_ce.birthDate"))
          .alias("birthDate")
        val deathDateCol = coalesce(col("patients.deathDate"), col("mco_ce.deathDate"))
          .alias("deathDate")

        allPatients
          .select(idCol, genderCol, birthDateCol, deathDateCol)
          .filter(col("patientID").isNotNull && col("gender").isNotNull && col("birthDate").isNotNull)
          .as[Patient]
    }
  }

  def validateDeathDate(deathDate: Column, birthDate: Column): Column =
    deathDate >= birthDate
}
