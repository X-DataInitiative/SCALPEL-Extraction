package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import org.apache.spark.sql.Dataset
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay}
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class HospitalStayExtractorSuite extends SharedContext {

  "extract" should "return the hospital stays from mco sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val input = Seq(
      ("20041", "830100392", makeTS(2012, 6, 7), makeTS(2012, 6, 7)),
      ("20041", "830100392", makeTS(2013, 6, 1), makeTS(2013, 6, 12)),
      ("20041", "830100392", makeTS(2014, 1, 28), makeTS(2014, 2, 3)),
      ("20041", "830100392", makeTS(2014, 1, 28), makeTS(2014, 2, 3)),
      ("2004100", "650780174", makeTS(2015, 12, 30), makeTS(2015, 12, 31)),
      ("2004100", "750712184", makeTS(2017, 7, 3), makeTS(2017, 10, 5))
    ).toDF("NUM_ENQ", "ETA_NUM", "EXE_SOI_DTD", "EXE_SOI_DTF")

    val sources = Sources(mco = Some(input))

    val expected: Dataset[Event[HospitalStay]] = Seq(
      HospitalStay("20041", "830100392", makeTS(2014, 1, 28), makeTS(2014, 2, 3)),
      HospitalStay("2004100", "650780174", makeTS(2015, 12, 30), makeTS(2015, 12, 31))
    ).toDS()

    //When
    val result: Dataset[Event[HospitalStay]] = new HospitalStayExtractor(
      HospitalStayConfig(makeTS(2014, 1, 1), makeTS(2016, 1, 1))).extract(sources)

    //Then
    assertDSs(expected, result)

  }

}
