// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.takeoverreasons

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HadAssociatedTakeOver, HadMainTakeOver, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.IsInStrategy
import fr.polytechnique.cmap.cnam.etl.extractors.codes.SimpleExtractorCodes
import fr.polytechnique.cmap.cnam.etl.extractors.sources.had.HadSimpleExtractor

final case class HadMainTakeOverExtractor(codes: SimpleExtractorCodes) extends HadSimpleExtractor[MedicalTakeOverReason]
  with IsInStrategy[MedicalTakeOverReason] {

  override val columnName: String = ColNames.PEC_PAL
  override val eventBuilder: EventBuilder = HadMainTakeOver

  override def extractValue(row: Row): String = row.getAs[Int](columnName).toString

  override def getCodes: SimpleExtractorCodes = codes
}

final case class HadAssociatedTakeOverExtractor(codes: SimpleExtractorCodes) extends HadSimpleExtractor[MedicalTakeOverReason]
  with IsInStrategy[MedicalTakeOverReason] {

  override val columnName: String = ColNames.PEC_ASS
  override val eventBuilder: EventBuilder = HadAssociatedTakeOver
  override def extractValue(row: Row): String = row.getAs[Int](columnName).toString

  override def getCodes: SimpleExtractorCodes = codes
}
