package fr.polytechnique.cmap.cnam.etl.old_root

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.util.RichDataFrames._
/**
  * Created by firas on 11/21/16.
  */
class ImbExtractorSuite extends SharedContext {
  "extract" should "filter null event dates column IMB_ALD_DTD" in {
    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val path = "src/test/resources/extractors/Imb.gz.parquet"
    val irImbExtract = new IrImbExtractor(sqlCtx)

    val expected = Seq(
      ("Patient_02","1","25/01/2006","24/01/2011","17","41","Y","000","Y","18/02/2006","E11","CIM10","10/01/2016"),
      ("Patient_02","1","13/03/2006","13/03/2016","17","41","Y","000","Y","18/02/2006","C67","CIM10","10/01/2016"),
      ("Patient_02","1","25/04/2006","25/04/2016","17","41","Y","000","Y","18/02/2006","9999","9999999999","10/01/2016"))
      .toDF("NUM_ENQ","BEN_RNG_GEM","IMB_ALD_DTD","IMB_ALD_DTF","IMB_ALD_NUM","IMB_ETM_NAT","IMB_MLP_BTR","IMB_MLP_TAB","IMB_SDR_LOP","INS_DTE","MED_MTF_COD","MED_NCL_IDT","UPD_DTE")

    // When
    val result = irImbExtract.extract(path)

    // Then
    assert(result === expected)

  }
}
