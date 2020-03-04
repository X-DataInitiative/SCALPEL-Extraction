package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.McoceEmergency
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class McoceEmergenciesExtractorSuite extends SharedContext {

  "extract" should "return the hospital stays(emergencies) from mcoce sources" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df = Seq(
      ("20041", "830100525", "00030885", "2012", makeTS(2012, 4, 21), makeTS(2012, 4, 21), "ATU"),
      ("20041", "830100525", "00032716", "2012", makeTS(2012, 4, 28), makeTS(2012, 4, 29), "ATU"),
      ("20041", "830100525", "00032738", "2012", makeTS(2012, 4, 29), makeTS(2012, 4, 29), "ATU"),
      ("20041", "830100525", "00032038", "2013", makeTS(2013, 4, 29), makeTS(2013, 4, 29), "FTN"),
      ("200410", "190000059", "00044158", null, makeTS(2010, 3, 5), makeTS(2010, 3, 5), null),
      ("200410", "190000059", "00027825", null, makeTS(2011, 5, 13), makeTS(2011, 5, 13), null),
      ("200410", "190000059", "00020161", null, makeTS(2012, 4, 10), makeTS(2012, 4, 10), null),
      ("200410", "190000059", "00022621", null, makeTS(2014, 4, 18), makeTS(2014, 5, 18), null),
      ("2004838055", "680000395", "00018597", "2010", makeTS(2010, 7, 11), makeTS(2010, 7, 11), "ATU F"),
      ("2006191920", "680000395", "00009656", "2013", makeTS(2013, 9, 24), makeTS(2013, 9, 24), "ATU N")
    ).toDF("NUM_ENQ", "ETA_NUM", "SEQ_NUM", "MCO_FBSTC__SOR_ANN", "EXE_SOI_DTD", "EXE_SOI_DTF", "MCO_FBSTC__ACT_COD")

    val sources = Sources(mcoCe = Some(df))

    val expected = Seq(
      McoceEmergency("20041", "830100525_00032716_2012", makeTS(2012, 4, 28), makeTS(2012, 4, 29)),
      McoceEmergency("20041", "830100525_00030885_2012", makeTS(2012, 4, 21), makeTS(2012, 4, 21)),
      McoceEmergency("20041", "830100525_00032738_2012", makeTS(2012, 4, 29), makeTS(2012, 4, 29)),
      McoceEmergency("2004838055", "680000395_00018597_2010", makeTS(2010, 7, 11), makeTS(2010, 7, 11)),
      McoceEmergency("2006191920", "680000395_00009656_2013", makeTS(2013, 9, 24), makeTS(2013, 9, 24))
    ).toDS()

    val res = McoceEmergenciesExtractor.extract(sources, Set.empty[String])

    assertDSs(expected, res)

  }

}
