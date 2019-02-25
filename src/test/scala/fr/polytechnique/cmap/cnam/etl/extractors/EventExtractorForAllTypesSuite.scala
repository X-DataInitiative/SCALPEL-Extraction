package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam._
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.acts._, mco._
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class EventExtractorForAllTypesSuite extends SharedContext  with MedicalActsInfo {

  // Handy for debugging datasets
  /*
  val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
  showString.setAccessible(true)
  println(showString.invoke(df, 10.asInstanceOf[Object], 20.asInstanceOf[Object], false.asInstanceOf[Object]).asInstanceOf[String])
  */

  "MCO.parquet extraction" should "MCOMedicalActExtractor of MedicalAct" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val result : Dataset[Event[MedicalAct]] = new MCOMedicalActExtractor().extract(mco, List(
      new MCOMedicalActEventExtractor(MCOCols.DP, List("C670", "C671"), McoCIM10Act),
      new MCOMedicalActEventExtractor(MCOCols.CCAM, List("AAAA123"), McoCCAMAct)))
    val expected = Seq[Event[MedicalAct]](
      McoCIM10Act("Patient_02", "10000123_10000543_2006", "C671", makeTS(2005, 12, 24)),
      McoCIM10Act("Patient_02", "10000123_10000987_2006", "C670", makeTS(2005, 12, 29)),
      McoCCAMAct("Patient_02", "10000123_10000987_2006", "AAAA123", makeTS(2005, 12, 29)),
      McoCIM10Act("Patient_02", "10000123_20000123_2007", "C670", makeTS(2007, 1, 29)),
      McoCCAMAct("Patient_02", "10000123_20000123_2007", "AAAA123", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_20000345_2007", "C671", makeTS(2007, 1, 29)),
      McoCIM10Act("Patient_02", "10000123_30000546_2008", "C670", makeTS(2008, 3, 8)),
      McoCCAMAct("Patient_02", "10000123_30000546_2008", "AAAA123", makeTS(2008, 3, 8)),
      McoCIM10Act("Patient_02", "10000123_30000852_2008", "C671", makeTS(2008, 3, 15))).toDS
    assertDSs(expected, result)
  }

  "DCIR.parquet extraction" should "DCIRMedicalActExtractor of DcirAct" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val pcol = col(PatientCols.CamCode)
    val dcir = spark.read.parquet("src/test/resources/test-input/DCIR.parquet")
      .withColumn(PatientCols.CamCode, when(pcol.isNull, "ABCD123").otherwise(pcol))
    val result = new DCIRMedicalActExtractor().extract(dcir, List(new DCIRMedicalActEventExtractor(List("ABCD123"))))
    val expected = List(
      DcirAct("Patient_01", "liberal", "ABCD123", makeTS(2006, 1, 15) ),
      DcirAct("Patient_01", "liberal", "ABCD123", makeTS(2006, 1, 30)),
      DcirAct("Patient_02", "liberal", "ABCD123", makeTS(2006, 1, 5)),
      DcirAct("Patient_02", "liberal", "ABCD123", makeTS(2006, 1, 15)),
      DcirAct("Patient_02", "liberal", "ABCD123", makeTS(2006, 1, 30)),
      DcirAct("Patient_02", "liberal", "ABCD123", makeTS(2006, 1, 30))).toDS
    assertDSs(expected, result)
  }

  "MCO.parquet extraction" should "MCOMedicalActExtractor of Diagnosis" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val dpCodes = List("C67")
    val drCodes = List("E05", "E08")
    val daCodes = List("C66")

    val mco: DataFrame = sqlContext.read.load("src/test/resources/test-input/MCO.parquet")
    val result : Dataset[Event[Diagnosis]] = new MCOMedicalActExtractor().extract(mco, List(
      new MCODiagnosisEventExtractor(MCOCols.DP, dpCodes, MainDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DR, drCodes, LinkedDiagnosis),
      new MCODiagnosisEventExtractor(MCOCols.DA, daCodes, AssociatedDiagnosis)))

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

  "IR_IMB_R.parquet extraction" should "IMBExtractor of ImbDiagnosis" in {
    val sqlCtx = sqlContext; import sqlCtx.implicits._
    val input = sqlContext.read.load("src/test/resources/test-input/IR_IMB_R.parquet")
    val expected = Seq(ImbDiagnosis("Patient_02", "C67", makeTS(2006, 3, 13))).toDS
    val output = new IMBExtractor().extract(input, List(new IMBEventExtractor(List("C67"))))
    assertDSs(expected, output)
  }
}
