package fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.had.HadExtractor
import fr.polytechnique.cmap.cnam.etl.extractors.takeOverReasons.HadMainTakeOverExtractor.code
import org.apache.spark.sql.Row

object HadMainTakeOverExtractor extends HadExtractor[MedicalTakeOverReason] {

  final override val columnName: String = ColNames.PEC_PAL

  override val eventBuilder: EventBuilder = HadMainTakeOver

  override def isInStudy(codes: Set[String])
               (row: Row): Boolean = codes.exists(code(row) == _)
}

object HadAssociatedTakeOverExtractor extends HadExtractor[MedicalTakeOverReason] {

  final override val columnName: String = ColNames.PEC_ASS

  override val eventBuilder: EventBuilder = HadAssociatedTakeOver

  override def isInStudy(codes: Set[String])
                        (row: Row): Boolean = codes.exists(code(row) == _)
}


