package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}
import fr.polytechnique.cmap.cnam.utilities.functions._

trait PatientsTransformer {
  final val MinAge: Int = FilteringConfig.limits.minAge
  final val MaxAge: Int = FilteringConfig.limits.maxAge
  final val MinGender: Int = FilteringConfig.limits.minGender
  final val MaxGender: Int = FilteringConfig.limits.maxGender
  final val MinYear: Int = FilteringConfig.limits.minYear
  final val MaxYear: Int = FilteringConfig.limits.maxYear
  final val MinMonth: Int = FilteringConfig.limits.minMonth
  final val MaxMonth: Int = FilteringConfig.limits.maxMonth
  final val DeathCode: Int = FilteringConfig.mcoDeathCode
  final val AgeReferenceDate: java.sql.Timestamp = FilteringConfig.dates.ageReference
}

object PatientsTransformer extends Transformer[Patient] with PatientsTransformer {

  def isDeathDateValid(deathDate: Column, birthDate: Column): Column =
    deathDate.between(birthDate, makeTS(MaxYear, 1, 1))

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

    val age = floor(months_between(lit(AgeReferenceDate), birthdate) / 12)
    val filterPatientsByAge = age >= 40 && age < 80

    patients.where(filterPatientsByAge)
      .select(
      patientID.as("patientID"),
      gender.as("gender"),
      birthdate.as("birthDate"),
      deathDate.as("deathDate")
    ).as[Patient]
  }
}
