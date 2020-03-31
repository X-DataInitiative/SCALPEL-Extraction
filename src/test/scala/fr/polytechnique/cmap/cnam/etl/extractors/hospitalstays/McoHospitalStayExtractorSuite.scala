// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay, McoHospitalStay}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoHospitalStayExtractorSuite extends SharedContext {

  "extract" should "return the hospital stays from mco sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      McoHospitalStay("Patient_02", "10000123_20000123_2007", 8.0D, makeTS(2007, 2, 1), makeTS(2007, 2, 10)),
      McoHospitalStay("Patient_02", "10000123_20000345_2007", 8.0D, makeTS(2007, 2, 1), makeTS(2007, 2, 10)),
      McoHospitalStay("Patient_02", "10000123_30000546_2008", 8.0D, makeTS(2008, 3, 1), makeTS(2008, 3, 10)),
      McoHospitalStay("Patient_02", "10000123_30000852_2008", 8.0D, makeTS(2008, 3, 1), makeTS(2008, 3, 10)),
      McoHospitalStay("Patient_02", "10000123_10000987_2006", 8.0D, makeTS(2006, 1, 1), makeTS(2006, 1, 10)),
      McoHospitalStay("Patient_02", "10000123_10000543_2006", -1.0D, makeTS(2006, 1, 1), makeTS(2006, 1, 10))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = McoHospitalStaysExtractor.extract(sources)

    //Then
    assertDSs(expected, result)
  }

  "extract" should "return the hospital stays from mco sources with non empty set codes" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mco = spark.read.parquet("src/test/resources/test-input/MCO.parquet")
    val sources = Sources(mco = Some(mco))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      McoHospitalStay("Patient_02", "10000123_20000123_2007", 8.0D, makeTS(2007, 2, 1), makeTS(2007, 2, 10)),
      McoHospitalStay("Patient_02", "10000123_20000345_2007", 8.0D, makeTS(2007, 2, 1), makeTS(2007, 2, 10)),
      McoHospitalStay("Patient_02", "10000123_30000546_2008", 8.0D, makeTS(2008, 3, 1), makeTS(2008, 3, 10)),
      McoHospitalStay("Patient_02", "10000123_30000852_2008", 8.0D, makeTS(2008, 3, 1), makeTS(2008, 3, 10)),
      McoHospitalStay("Patient_02", "10000123_10000987_2006", 8.0D, makeTS(2006, 1, 1), makeTS(2006, 1, 10)),
      McoHospitalStay("Patient_02", "10000123_10000543_2006", -1.0D, makeTS(2006, 1, 1), makeTS(2006, 1, 10))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = McoHospitalStaysExtractor.extract(sources)

    //Then
    assertDSs(expected, result)
  }

  "extractWeight" should "calculate correct weight from mco sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df = Seq(
      ("Patient_02", "10000123", "20000123", "2007", makeTS(2007, 1, 1), makeTS(2007, 1, 10), "8", "5"),
      ("Patient_02", "10000123", "20000345", "2007", makeTS(2007, 2, 1), makeTS(2007, 2, 10), "8", "5"),
      ("Patient_02", "10000123", "20000546", "2007", makeTS(2007, 3, 1), makeTS(2007, 3, 10), "8", "R"),
      ("Patient_02", "10000123", "20000852", "2007", makeTS(2007, 4, 1), makeTS(2007, 4, 10), "8", null),
      ("Patient_02", "10000123", "20000987", "2007", makeTS(2007, 5, 1), makeTS(2007, 5, 10), null, null)
    ).toDF("NUM_ENQ", "ETA_NUM", "RSA_NUM", "SOR_ANN", "EXE_SOI_DTD", "EXE_SOI_DTF", "MCO_B__ENT_MOD", "MCO_B__ENT_PRV")

    val expected = Seq(8.5D, 8.5D, 8.8D, 8.0D, -1.0D).toDS()

    //When
    val result = df.map(r => McoHospitalStaysExtractor.extractWeight(r))

    //Then
    assertDSs(expected, result)

  }

}
