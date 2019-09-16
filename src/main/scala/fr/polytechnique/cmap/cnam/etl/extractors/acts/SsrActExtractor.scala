package fr.polytechnique.cmap.cnam.etl.extractors.acts

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ssr.SsrExtractor
import org.apache.spark.sql.Row

object SsrCcamActExtractor extends SsrExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CCAM
  override val eventBuilder: EventBuilder = SsrCCAMAct

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RhaNum) + "_" +
      r.getAs[String](ColNames.RhsNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
}

object SsrCimMedicalActExtractor extends SsrExtractor[MedicalAct] {
  final override val columnName: String = ColNames.FP_PEC
  override val eventBuilder: EventBuilder = SsrCIM10Act

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RhaNum) + "_" +
      r.getAs[String](ColNames.RhsNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
}

object SsrCsarrActExtractor extends SsrExtractor[MedicalAct] {
  final override val columnName: String = ColNames.CSARR
  override val eventBuilder: EventBuilder = SsrCSARRAct

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RhaNum) + "_" +
      r.getAs[String](ColNames.RhsNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
}