package fr.polytechnique.cmap.cnam.etl.extractors.events.hospitalstays

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HadHospitalStay, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HadHospitalStayExtractorSuite extends SharedContext {

  "extract" should "return the hospital stays from had sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      HadHospitalStay("patient01", "10000123_30000123_2019", makeTS(2019, 11, 21), makeTS(2019, 11, 21)),
      HadHospitalStay("patient01", "10000123_30000124_2019", makeTS(2019, 10, 10), makeTS(2019, 10, 10)),
      HadHospitalStay("patient02", "10000201_30000150_2019", makeTS(2019, 12, 24), makeTS(2019, 12, 25))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = HadHospitalStaysExtractor.extract(sources)

    //Then
    assertDSs(expected, result)
  }

  "extract" should "return the hospital stays from had sources with non empty set codes" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val had = spark.read.parquet("src/test/resources/test-input/HAD.parquet")
    val sources = Sources(had = Some(had))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      HadHospitalStay("patient01", "10000123_30000123_2019", makeTS(2019, 11, 21), makeTS(2019, 11, 21)),
      HadHospitalStay("patient01", "10000123_30000124_2019", makeTS(2019, 10, 10), makeTS(2019, 10, 10)),
      HadHospitalStay("patient02", "10000201_30000150_2019", makeTS(2019, 12, 24), makeTS(2019, 12, 25))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = HadHospitalStaysExtractor.extract(sources)

    //Then
    assertDSs(expected, result)
  }

}
