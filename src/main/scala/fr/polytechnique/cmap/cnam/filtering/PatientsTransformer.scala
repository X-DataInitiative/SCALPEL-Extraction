package fr.polytechnique.cmap.cnam.filtering

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import fr.polytechnique.cmap.cnam.utilities.ColumnUtilities
import fr.polytechnique.cmap.cnam.utilities.functions.computeDateUsingMonthYear
import org.apache.log4j.Logger

/**
  * Transformer object for patients data
  * Note that all transformers should cache the DataFrames that are going to be used
  */
object PatientsTransformer extends Transformer[Patient] {

  def estimateBirthDateCol(ts1: Column, ts2: Column, birthYear: Column): Column = {
    unix_timestamp(
      concat(
        month(ColumnUtilities.getMeanTimestampColumn(ts1, ts2)),
        lit("-"),
        birthYear
      ), "MM-yyyy"
    ).cast(TimestampType)
  }

  implicit class PatientTransformerPipeline(data: DataFrame) {

    final val MinAge = 18
    final val MaxAge = 1000
    final val MinGender = 1
    final val MaxGender = 2
    final val MinYear = 1900
    final val MaxYear = 2020
    final val MinMonth = 1
    final val MaxMonth = 12
    final val deathCode = 9

    def selectFromDCIR: DataFrame = {
      val dcirCols: List[Column] = List(
        col("NUM_ENQ").cast(StringType).as("patientID"),
        when(
          // Invalid values of gender will be set to null in order to allow the "average" gender to
          //   be calculated and then the correct gender to be found
          !col("BEN_SEX_COD").between(MinGender, MaxGender), null
        ).otherwise(col("BEN_SEX_COD")).cast(IntegerType).as("gender"),
        col("BEN_AMA_COD").cast(IntegerType).as("age"),
        col("BEN_NAI_ANN").cast(StringType).as("birthYear"),
        col("EXE_SOI_DTD").cast(DateType).as("eventDate"),
        col("BEN_DCD_DTE").cast(DateType).as("deathDate")
      )

      data.select(dcirCols: _*).where(
        col("age").between(MinAge, MaxAge) &&
          (col("deathDate").isNull || year(col("deathDate")).between(MinYear, MaxYear))
      )// adding distinct at the end would avoid large number of redundant data. But it would affect gender code computation.
    }

    // The birth year for each patient is found by grouping by patientId and birthYear and then
    //   by taking the most frequent birth year for each patient.
    def findBirthYears: DataFrame = {
      val window = Window.partitionBy(col("patientID")).orderBy(col("count").desc, col("birthYear"))
      data
        .groupBy(col("patientID"), col("birthYear")).agg(count("*").as("count"))
        // "first" is only deterministic when applied over an ordered window:
        .select(col("patientID"), first(col("birthYear")).over(window).as("birthYear"))
        .distinct
    }

    // After selecting the data, the next step is to group by patientId and age, because we need to
    //   estimate the birthDate ant we use min(eventDate) and max(eventDate) for each age to achieve
    //   that.
    def groupByIdAndAge: DataFrame = {
      data
        .groupBy(col("patientID"), col("age"))
        .agg(
          count("gender").as("genderCount"), // We will use it to find the appropriate gender (avg)
          sum("gender").as("genderSum"), // We will use it to find the appropriate gender (avg)
          min("eventDate").as("minEventDate"), // the min event date for each age of a patient
          max("eventDate").as("maxEventDate"), // the max event date for each age of a patient
          min("deathDate").as("deathDate") // the earliest death date
        )
    }

    // Then we aggregate again by taking the mean between the closest dates where the age changed.
    // For example, if the patient was 60yo when an event happened on Apr/2010 and he was 61yo when
    //   another event happened on Jun/2010, we calculate the mean and estimate his birthday as
    //   being in May of the year found in "findBirthYears"
    def estimateDCIRFields: DataFrame = {
      val birthDateAggCol: Column = estimateBirthDateCol(
        max(col("minEventDate")).cast(TimestampType),
        min(col("maxEventDate")).cast(TimestampType),
        first(col("birthYear"))
      )

      data
        .groupBy(col("patientID"))
        .agg(
          // Here we calculate the average of gender values and then we round. So, if 1 is more
          //   common, the average will be less than 1.5 and the final value will be 1. The same is
          //   valid for the case where 2 is more common. This is the reason why we set invalid
          //   values for gender to null.
          round(sum(col("genderSum")) / sum(col("genderCount"))).cast(IntegerType).as("dcirGender"),
          birthDateAggCol.as("dcirBirthDate"),
          min(col("deathDate")).cast(TimestampType).as("dcirMinDeathDate")
        )
    }

    def getIrBenBirthDatesAndGender: DataFrame = {
      val cleanData: DataFrame = data
        .filter(col("BEN_NAI_MOI").between(MinMonth, MaxMonth) &&
                 col("BEN_NAI_ANN").between(MinYear, MaxYear))

      val distinctPatients = cleanData.select("NUM_ENQ").distinct
      val distinctPatientsGender = cleanData.select("NUM_ENQ", "BEN_SEX_COD").distinct
      val distinctPatientsBirthDateGender = cleanData
        .select("NUM_ENQ", "BEN_SEX_COD", "BEN_NAI_MOI", "BEN_NAI_ANN")
        .distinct

      // This check makes sure, patients don't have conflicting sex code.
      // Actually, i verified it and it is true. But we still need to keep this check
      // to ensure whether the hypothesis still holds for any new/old versions of IR_BEN_R.
      if(distinctPatients.count != distinctPatientsGender.count)
        throw new Exception("One or more patients have conflicting SEX CODE in IR_BEN_R")

      // This check makes sure, patients don't have conflicting birth dates.
      if (distinctPatients.count != distinctPatientsBirthDateGender.count)
        throw new Exception("One or more patients have conflicting BIRTH DATES in IR_BEN_R")

      val columnsToSelect: List[Column] = List(
        col("NUM_ENQ").cast(StringType).as("patientID"),

        col("BEN_SEX_COD").cast(IntegerType).as("irBenGender"),

        when(col("BEN_NAI_MOI").between(MinMonth, MaxMonth),
          col("BEN_NAI_MOI")).cast(IntegerType).as("birthMonth"),

        when(col("BEN_NAI_ANN").between(MinYear, MaxYear),
          col("BEN_NAI_ANN")).cast(IntegerType).as("birthYear")
      )

      distinctPatientsBirthDateGender.select(columnsToSelect: _*)
        .withColumn("irBenBirthDate", computeDateUsingMonthYear(col("birthMonth"), col("birthYear")))
        .select("patientID", "irBenGender", "irBenBirthDate")
    }

    def getIrBenDeathDates: DataFrame = {
      val cleanData = data.filter(col("BEN_DCD_DTE").isNotNull)

      val distinctPatients = cleanData.select("NUM_ENQ").distinct
      val distinctPatientsDeathDate = cleanData
        .select("NUM_ENQ", "BEN_DCD_DTE").distinct

      // We should not bother about gender code here. Bcoz death date methods are only supplementary.
      // Gender codes should be computed only by irBenBirthDates otherwise by DCIR.
      if (distinctPatients.count != distinctPatientsDeathDate.count)
        throw new Exception("One or more patients have conflicting DEATH DATES in IR_BEN_R")

      val columnsToSelect: List[Column] = List(
        col("NUM_ENQ").cast(StringType).as("patientID"),
        col("BEN_DCD_DTE").cast(TimestampType).as("irBenDeathDate")
      )

      distinctPatientsDeathDate.select(columnsToSelect: _*)
    }

    def getMcoDeathDates: DataFrame = {
      // TODO: We may need to check the consistency of {SOR_MOI, SOR_ANN} against SOR_DAT in MCO_C.
      val mcoDeathDates: DataFrame = data.filter(col("SOR_MOD") === deathCode)
        .withColumn("mcoDeathDate", computeDateUsingMonthYear(col("SOR_MOI"), col("SOR_ANN")))
        .select(col("patientID"), col("mcoDeathDate"))

      val mcoDistinctPatients = mcoDeathDates.select("patientID").distinct
      val mcoDistinctPatientsDeathDates = mcoDeathDates.select("patientID", "mcoDeathDate").distinct

      // This check is recently added as MCO_C contains more than one death date for a patient.
      if(mcoDistinctPatients.count != mcoDistinctPatientsDeathDates.count){
        Logger.getLogger("org").error("One or more patients have conflicting DEATH DATES in MCO, " +
                                        "Taking Minimum Death Dates")

        mcoDeathDates.groupBy("patientID").agg(min(col("mcoDeathDate")).as("mcoDeathDate"))
      }
      else
        mcoDeathDates
    }
  }

  def isDeathDateValid(deathDate: Column, irBenBirthDate: Column, dcirBirthDate: Column): Column = {
    val birthDate = coalesce(irBenBirthDate, dcirBirthDate)
    // According to the java.util.Date class, year is calculated 1900+supplied value.
    // So 120 -> 1900+120 = 2020 (year); 0 -> Jan, 1 -> date.
    // TODO: java.util.Date(year,month,date) is deprecated, so find some other way to do it.
    val maxDeathDate = new java.sql.Timestamp(new java.util.Date(120,0,1).getTime)// 2020-01-01
    deathDate.between(birthDate, maxDeathDate)
  }

  def transform(sources: Sources): Dataset[Patient] = {

    val irBenNeededColumns: List[Column] = List(col("NUM_ENQ"), col("BEN_SEX_COD"),
      col("BEN_NAI_MOI"), col("BEN_NAI_ANN"), col("BEN_DCD_DTE"))

    val irBen: DataFrame = sources.irBen.get.select(irBenNeededColumns: _*)

    // In this version, getIrBenBirthDates should compute right gender code as well.
    val irBenBirthDates: DataFrame = irBen.getIrBenBirthDatesAndGender

    // We should not compute gender code in this method. We will compute it
    // with dcir. This column is only supplementary alike mcoDeathDates.
    val irBenDeathDates: DataFrame = irBen.getIrBenDeathDates

    val pmsiMcoNeededColumns: List[Column] = List(
      col("NUM_ENQ").as("patientID"),
      col("`MCO_B.SOR_MOD`").as("SOR_MOD"),
      col("SOR_MOI"),
      col("SOR_ANN"))

    // distinct at the end is important (bcoz, its a flarDF), otherwise there will be many duplicates.
    val pmsiMco: DataFrame = sources.pmsiMco.get
      .select(pmsiMcoNeededColumns: _*)
      .distinct

    // Same comment for gender code.
    val pmsiMcoDeathDates: DataFrame = pmsiMco.getMcoDeathDates

    val dcir: DataFrame = sources.dcir.get.selectFromDCIR.cache()

    val patientsIdMissingInIrBen = dcir.select(col("patientID")).distinct
                                       .except(irBenBirthDates.select(col("patientID")))
//        .join(dcir, Seq("patientID"), "left_outer")
//    }

    val patientsDataMissingInIrBen = patientsIdMissingInIrBen
      .join(dcir.distinct, Seq("patientID"), "left_outer")


    val birthYearPatientsNotInIrBen: DataFrame = patientsDataMissingInIrBen.findBirthYears

    val transformPatientsNotExistsInIrBen: DataFrame =
      patientsDataMissingInIrBen
      .groupByIdAndAge
      .join(birthYearPatientsNotInIrBen, "patientID")
      .estimateDCIRFields

    // This select expression should be used only on the final output dataframe
    // after computing the birthDates of all the patients.
    val columnReconciliation: List[Column] = List(
      col("patientID"),

      coalesce(
        col("irBenGender"),
        col("dcirGender")).cast(IntegerType).as("gender"),

      coalesce(
        col("irBenBirthDate"),
        col("dcirBirthDate")).cast(TimestampType).as("birthDate"),

      when(
        col("irBenDeathDate").isNotNull &&
          isDeathDateValid(col("irBenDeathDate"), col("irBenBirthDate"), col("dcirBirthDate")), col("irBenDeathDate")
      ).otherwise(
          when(col("mcoDeathDate").isNotNull &&
            isDeathDateValid(col("mcoDeathDate"), col("irBenBirthDate"), col("dcirBirthDate")), col("mcoDeathDate")
          ).otherwise(
              when(col("dcirMinDeathDate").isNotNull &&
                isDeathDateValid(col("dcirMinDeathDate"), col("irBenBirthDate"), col("dcirBirthDate")), col("dcirMinDeathDate")
              ))
      ).cast(TimestampType).as("deathDate")
    )

    import dcir.sqlContext.implicits._ // Necessary for the DataFrame -> Dataset conversion

    // In the join, if the gender code of a patient that comes from irBenBirth, irBenDeath, mcoDeath,
    // transformPatientsNotExistsInIrBen don't match, then we will have two entry for a patient (conflict).
    val result = dcir
      .select(col("patientID")).distinct
      .join(irBenBirthDates, Seq("patientID"), "left_outer")
      .join(irBenDeathDates, Seq("patientID"), "left_outer")
      .join(pmsiMcoDeathDates, Seq("patientID"), "left_outer")
      .join(transformPatientsNotExistsInIrBen, Seq("patientID"), "left_outer")
      .select(columnReconciliation: _*)
      .as[Patient]

    dcir.unpersist()
    result
  }
}
