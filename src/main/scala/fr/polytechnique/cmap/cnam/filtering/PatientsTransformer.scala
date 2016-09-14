package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

import fr.polytechnique.cmap.cnam.utilities.functions._


trait PatientsTransformer {

  final val MinAge = 18
  final val MaxAge = 1000
  final val MinGender = 1
  final val MaxGender = 2
  final val MinYear = 1900
  final val MaxYear = 2020
  final val MaxDeathDate = makeTS(2020, 1, 1)
  final val MinMonth = 1
  final val MaxMonth = 12
  final val deathCode = 9

}

object PatientsTransformer extends Transformer[Patient] with PatientsTransformer {

  def isDeathDateValid(deathDate: Column, birthDate: Column): Column = {
    deathDate.between(birthDate, MaxDeathDate)
  }

  def transform(sources: Sources): Dataset[Patient] = {

    val irBen = IrBenPatientTransformer.transform(sources).toDF.as("irBen")
    val mco = McoPatientTransformer.transform(sources).toDF.as("mco")
    val dcir = DcirPatientTransformer.transform(sources).toDF.as("dcir")

    import dcir.sqlContext.implicits._

    val joinColumn = coalesce(col("irBen.patientID"), col("mco.patientID"))

    val patientID: Column = coalesce(
      col("dcir.patientID"),
      col("irBen.patientID"),
      col("mco.patientID")
    )

    val patients = irBen
      .join(mco, col("irBen.patientID") === col("mco.patientID"), "outer")
      .join(dcir, joinColumn === col("dcir.patientID"), "outer")

    val gender: Column = coalesce(col("irBen.gender"), col("dcir.gender"))

    val birthdate: Column = coalesce(col("irBen.birthDate"), col("dcir.birthDate"))

    val deathDate: Column = coalesce(
      when(isDeathDateValid(col("irBen.deathDate"), birthdate),
        col("irBen.deathDate")),
      when(isDeathDateValid(col("dcir.deathDate"), birthdate),
        col("dcir.deathDate")),
      when(isDeathDateValid(col("mco.deathDate"), birthdate),
        col("mco.deathDate"))
    )


    patients.select(
      patientID.as("patientID"),
      gender.as("gender"),
      birthdate.as("birthDate"),
      deathDate.as("deathDate")
    ).as[Patient]

  }
}
