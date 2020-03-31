package fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.events.{EventBuilder, HadAssociatedTakeOver, HadMainTakeOver, MedicalTakeOverReason}
import fr.polytechnique.cmap.cnam.etl.extractors.{BaseExtractorCodes, IsInStrategy}
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadBasicExtractor

final case class HadMainTakeOverExtractor(codes: BaseExtractorCodes) extends HadBasicExtractor[MedicalTakeOverReason]
  with IsInStrategy[MedicalTakeOverReason] {

  override val columnName: String = ColNames.PEC_PAL
  override val eventBuilder: EventBuilder = HadMainTakeOver

  override def extractValue(row: Row): String = row.getAs[Int](columnName).toString

  override def getCodes: BaseExtractorCodes = codes
}

final case class HadAssociatedTakeOverExtractor(codes: BaseExtractorCodes) extends HadBasicExtractor[MedicalTakeOverReason]
  with IsInStrategy[MedicalTakeOverReason] {

  override val columnName: String = ColNames.PEC_ASS
  override val eventBuilder: EventBuilder = HadAssociatedTakeOver
  override def extractValue(row: Row): String = row.getAs[Int](columnName).toString

  override def getCodes: BaseExtractorCodes = codes
}
