// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.extractors.events.ngapacts

import scala.util.Try
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.etl.events.{Event, EventBuilder, McoCeFbstcNgapAct, McoCeFcstcNgapAct, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.extractors.sources.mcoce.McoCeRowExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

sealed abstract class McoCeNgapActExtractor(ngapActsConfig: NgapActConfig[NgapActClassConfig]) extends Extractor[NgapAct, NgapActConfig[NgapActClassConfig]]
  with McoCeRowExtractor {
  // abstract values for implementing classes
  val keyLetterColumn: String
  val coeffColumn: String
  val eventBuilder: EventBuilder

  // Implementation of the EventRowExtractor
  override def usedColumns: List[String] = super.usedColumns ++ List(keyLetterColumn, coeffColumn)

  override def extractValue(row: Row): String = {
    val letter = getNgapLetter(row)
    val coeff = getNgapCoeff(row)
    s"PmsiCe_${letter}_${coeff}"
  }

  // Implementation of the Extractor Trait
  override def getCodes: NgapActConfig[NgapActClassConfig] = ngapActsConfig

  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(keyLetterColumn))

  override def isInStudy(row: Row): Boolean = {
    lazy val letter = getNgapLetter(row)
    lazy val coeff = getNgapCoeff(row)
    ngapActsConfig.actsCategories.exists(category => ngapIsInCategory(category, letter, coeff))
  }

  private def ngapIsInCategory(category: NgapActClassConfig, ngapLetter: => String, ngapCoeff: => String): Boolean =
    category.ngapKeyLetters.contains(ngapLetter) && {
      category.ngapCoefficients.isEmpty || category.ngapCoefficients.contains(ngapCoeff)
    }

  private def getNgapLetter(row: Row): String = row.getAs[String](keyLetterColumn)
  private def getNgapCoeff(row: Row): String = {
    Try(row.getAs[Double](coeffColumn).toString) recover {
      case _: NullPointerException => "0"
    }
  }.get


  def builder(row: Row): Seq[Event[NgapAct]] = {
    val patientId = extractPatientId(row)
    val groupId = extractGroupId(row)
    val value = extractValue(row)
    val eventDate = extractStart(row)
    val endDate = extractEnd(row)
    val weight = extractWeight(row)

    Seq(eventBuilder[NgapAct](patientId, groupId, value, weight, eventDate, endDate))
  }

  override def getInput(sources: Sources): DataFrame = sources.mcoCe.get.select(usedColumns.map(col): _*)
}

final case class McoCeFbstcNgapActExtractor(ngapConfig: NgapActConfig[NgapActClassConfig]) extends McoCeNgapActExtractor(ngapConfig) {
  val keyLetterColumn: String = ColNames.NgapKeyLetterFbstc
  val coeffColumn: String = ColNames.NgapCoefficientFbstc
  override val eventBuilder: EventBuilder = McoCeFbstcNgapAct
  val ngapActsConfig: NgapActConfig = ngapConfig
  val coeffColumn: String = ColNames.NgapCoefficientFbstc
}

final case class McoCeFcstcNgapActExtractor(ngapConfig: NgapActConfig[NgapActClassConfig]) extends McoCeNgapActExtractor(ngapConfig) {
  val keyLetterColumn: String = ColNames.NgapKeyLetterFcstc
  val coeffColumn: String = ColNames.NgapCoefficientFcstc

  override val eventBuilder: EventBuilder = McoCeFcstcNgapAct
  val ngapActsConfig: NgapActConfig = ngapConfig
  val coeffColumn: String = ColNames.NgapCoefficientFcstc
}
