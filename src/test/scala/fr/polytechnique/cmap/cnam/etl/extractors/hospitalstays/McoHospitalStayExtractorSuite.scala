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
    val result: Dataset[Event[HospitalStay]] = McoHospitalStaysExtractor.extract(sources, Set.empty)

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
    val result: Dataset[Event[HospitalStay]] = McoHospitalStaysExtractor.extract(sources, Set("Test"))

    //Then
    assertDSs(expected, result)
  }

}
