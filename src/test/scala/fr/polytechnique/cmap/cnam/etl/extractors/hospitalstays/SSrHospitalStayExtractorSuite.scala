package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay, SsrHospitalStay}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS
import org.apache.spark.sql.Dataset

class SSrHospitalStayExtractorSuite extends SharedContext {

  "extract" should "return the hospital stays from ssr sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      SsrHospitalStay("Patient_01", "10000123_30000801_2019", makeTS(2019, 10, 20), makeTS(2019, 11, 11)),
      SsrHospitalStay("Patient_02", "10000123_30000546_2019", makeTS(2019, 8, 11), makeTS(2019, 8, 30))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = SsrHospitalStaysExtractor.extract(sources, Set.empty)

    //Then
    assertDSs(expected, result)
  }

  "extract" should "return the hospital stays from ssr sources with non empty set codes" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val ssr = spark.read.parquet("src/test/resources/test-input/SSR.parquet")
    val sources = Sources(ssr = Some(ssr))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      SsrHospitalStay("Patient_01", "10000123_30000801_2019", makeTS(2019, 10, 20), makeTS(2019, 11, 11)),
      SsrHospitalStay("Patient_02", "10000123_30000546_2019", makeTS(2019, 8, 11), makeTS(2019, 8, 30))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = SsrHospitalStaysExtractor.extract(sources, Set("Test"))

    //Then
    assertDSs(expected, result)
  }

}
