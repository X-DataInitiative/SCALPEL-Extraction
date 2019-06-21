package fr.polytechnique.cmap.cnam.etl.extractors

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts._, mco._
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class EventExtractorForAllTypesSuite extends SharedContext with MCOSourceInfo with DCIRSourceInfo {
  ///* Handy for debugging datasets */
  /*
  val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
  showString.setAccessible(true)
  */
  // println(showString.invoke(df, 10.asInstanceOf[Object], 20.asInstanceOf[Object], false.asInstanceOf[Object]).asInstanceOf[String])

  "DCIR.parquet extraction" should "DCIRSourceExtractor of DcirAct" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val codes = List("AAAA", "CCCC")
    val dcir = Seq(
      ("Patient_A", "AAAA", makeTS(2010, 1, 1), None, None, None),
      ("Patient_A", "BBBB", makeTS(2010, 2, 1), Some(1D), Some(0D), Some(1D)),
      ("Patient_B", "CCCC", makeTS(2010, 3, 1), None, None, None),
      ("Patient_B", "CCCC", makeTS(2010, 4, 1), Some(7D), Some(0D), Some(2D)),
      ("Patient_C", "BBBB", makeTS(2010, 5, 1), Some(1D), Some(0D), Some(2D))
    ).toDF(
      PatientCols.PatientID, PatientCols.CamCode, PatientCols.Date,
      PatientCols.InstitutionCode, PatientCols.GHSCode, PatientCols.Sector
    )
    val result : Dataset[Event[MedicalAct]] = new DCIRSourceExtractor(new Sources(dcir = Some(dcir)), List(new DCIRMedicalActEventExtractor(codes)))
        .extract()
    val expected : Dataset[Event[MedicalAct]] = Seq[Event[MedicalAct]](
      DcirAct("Patient_A", DcirAct.groupID.Liberal, "AAAA", makeTS(2010, 1, 1)),
      DcirAct("Patient_B", DcirAct.groupID.Liberal, "CCCC", makeTS(2010, 3, 1)),
      DcirAct("Patient_B", DcirAct.groupID.PrivateAmbulatory, "CCCC", makeTS(2010, 4, 1))
    ).toDS
    assertDSs(expected, result)
  }

  "MCO.parquet extraction" should "MCOSourceExtractor of MedicalAct" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val result : Dataset[Event[MedicalAct]] = new MCOSourceExtractor(new Sources(mco = Some(mco)), List(
      new MCOMedicalActEventExtractor(MCOCols.DP, List("C670", "C671"), McoCIM10Act),
      new MCOMedicalActEventExtractor(MCOCols.CCAM, List("AAAA123"), McoCCAMAct)
    )).extract()
    val expected = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8)),
      McoCIM10Act("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15))
    ).toDS
    assertDSs(expected, result)
  }

  "MCO.parquet extraction" should "MCOSourceExtractor of Diagnosis" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val result : Dataset[Event[Diagnosis]] = new MCOSourceExtractor(new Sources(mco = Some(mco)), List(
      new MCODiagnosisEventExtractor(MCOCols.DP, List("C67"), MainDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DR, List("E05", "E08"), LinkedDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DA, List("C66"), AssociatedDiagnosis)))
        .extract()
    val expected = Seq[Event[Diagnosis]](
      MainDiagnosis("Patient_02", "10000123_20000123_2007", "C67", makeTS(2007, 1, 29)),
      LinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      AssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_20000345_2007", "C67", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_10000987_2006", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "10000123_10000543_2006", "C67", makeTS(2005, 12, 24)),
      LinkedDiagnosis("Patient_02", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24)),
      AssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "10000123_30000546_2008", "C67", makeTS(2008, 3, 8)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C67", makeTS(2008, 3, 15))).toDS
    assertDSs(expected, result)
  }

  "IR_IMB_R.parquet extraction" should "IMBSourceExtractor of ImbDiagnosis" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val input = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))).toDS
    val output : Dataset[Event[Diagnosis]]  = new IMBSourceExtractor(new Sources(irImb = Some(input)),
        List(new IMBDiagnosisEventExtractor(List("C67")))).extract()
    assertDSs(expected, output)
  }

  "MCO.parquet extraction" should "MCOSourceExtractor of GHMClassification" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    // println(showString.invoke(mco, 10.asInstanceOf[Object], 20.asInstanceOf[Object], false.asInstanceOf[Object]).asInstanceOf[String])
    val ghmCodes = List("12H50L")
    val result : Dataset[Event[Classification]] =
      new MCOSourceExtractor(new Sources(mco = Some(mco)), List(
        new ClassificationEventExtractor(MCOColsFull.GHM, ghmCodes, GHMClassification))).extract()
    val expected = Seq(
      GHMClassification("Patient_02", "10000123_20000123_2007", "12H50L", makeTS(2007,1,29)),
      GHMClassification("Patient_02", "10000123_10000987_2006", "12H50L", makeTS(2005,12,29)),
      GHMClassification("Patient_02", "10000123_30000546_2008", "12H50L", makeTS(2008,3,8))
    ).toDS
    assertDSs(result, expected)
  }

  it should "return correct empty when given empty list" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val ghmCodes = List()
    val result : Dataset[Event[Classification]] =
      new MCOSourceExtractor(new Sources(mco = Some(mco)), List(
        new ClassificationEventExtractor(MCOColsFull.GHM, ghmCodes, GHMClassification)))
          .extract()
    val expected = sqlContext.sparkSession.emptyDataset[Event[Classification]]
    assertDSs(result, expected)
  }

  "MCO.parquet extraction" should "match everything" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val result : Dataset[Event[AnyEvent]] =
      new MCOSourceExtractor(new Sources(mco = Some(mco)), List(
      new MCODiagnosisEventExtractor(MCOCols.DP, List("C67"), MainDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DR, List("E05", "E08"), LinkedDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DA, List("C66"), AssociatedDiagnosis),
      new MCOMedicalActEventExtractor(MCOCols.DP, List("C670", "C671"), McoCIM10Act),
      new MCOMedicalActEventExtractor(MCOCols.CCAM, List("AAAA123"), McoCCAMAct),
      new ClassificationEventExtractor(MCOColsFull.GHM, List("12H50L"), GHMClassification)))
        .extract()
    val expected : Dataset[Event[AnyEvent]] = Seq(
      MainDiagnosis("Patient_02", "10000123_20000123_2007", "C67", makeTS(2007, 1, 29)),
      LinkedDiagnosis("Patient_02", "10000123_20000123_2007", "E05", makeTS(2007, 1, 29)),
      AssociatedDiagnosis("Patient_02", "10000123_20000123_2007", "C66", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_20000345_2007", "C67", makeTS(2007, 1, 29)),
      MainDiagnosis("Patient_02", "10000123_10000987_2006", "C67", makeTS(2005, 12, 29)),
      MainDiagnosis("Patient_02", "10000123_10000543_2006", "C67", makeTS(2005, 12, 24)),
      LinkedDiagnosis("Patient_02", "10000123_10000543_2006", "E08", makeTS(2005, 12, 24)),
      AssociatedDiagnosis("Patient_02", "10000123_10000543_2006", "C66", makeTS(2005, 12, 24)),
      MainDiagnosis("Patient_02", "10000123_30000546_2008", "C67", makeTS(2008, 3, 8)),
      MainDiagnosis("Patient_02", "10000123_30000852_2008", "C67", makeTS(2008, 3, 15)),
      McoCIM10Act("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8)),
      McoCIM10Act("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15)),
      GHMClassification("Patient_02", "10000123_20000123_2007", "12H50L", makeTS(2007,1,29)),
      GHMClassification("Patient_02", "10000123_10000987_2006", "12H50L", makeTS(2005,12,29)),
      GHMClassification("Patient_02", "10000123_30000546_2008", "12H50L", makeTS(2008,3,8))
    ).toDS
    assertDSs(result, expected)
  }
}
