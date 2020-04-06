package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import scala.reflect.runtime.universe._
import scala.util.Try
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

trait McoCeNgapActExtractor extends McoCeExtractor[NgapAct] {
  val ngapActsConfig: NgapActConfig
  val keyLetterColumn: String
  val coeffColumn: String

  val columnName: String = keyLetterColumn

  override def code: Row => String = (row: Row) => {
    val coeff = Try(row.getAs[Double](coeffColumn).toString) recover {
      case _: NullPointerException => "0"
    }
    "PmsiCe_" + row.getAs[String](keyLetterColumn) + "_" + coeff.get
  }

  override def extract(
    sources: Sources,
    codes: Set[String])
    (implicit ctag: TypeTag[NgapAct]): Dataset[Event[NgapAct]] = {

    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (ngapActsConfig.actsCategories.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
    }.flatMap(builder _).distinct()
  }

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    pmsiIsInCategories(
      ngapActsConfig.actsCategories,
      keyLetterColumn,
      coeffColumn,
      row
    )
  }

  /** User could be interested by different Ngap categories each defined by a list of key letters
    * and a list of coefficients. This function iterates over each category. More détails in the NgapActConfig class.
    *
    * @param categories      : A list of Ngap prestation and coefficient codes
    * @param ngapKeyColumn   : the Ngap prestation code for MCO CE
    * @param ngapCoeffColumn : the Ngap coefficient which complete the prestation code for MCO CE
    * @param row
    * @return
    */
  def pmsiIsInCategories(
    categories: List[NgapActClassConfig],
    ngapKeyColumn: String,
    ngapCoeffColumn: String,
    row: Row): Boolean = {

    val letter = row.getAs[String](ngapKeyColumn)
    val coeff = Try(row.getAs[Double](ngapCoeffColumn).toString) recover {
      case _: NullPointerException => "0"
    }

    categories
      .exists(category => pmsiIsInCategory(category, letter, coeff.get))
  }

  def pmsiIsInCategory(
    category: NgapActClassConfig,
    ngapLetter: String,
    ngapCoeff: String): Boolean = {
    if (category.ngapCoefficients.isEmpty) {
      category.ngapKeyLetters.contains(ngapLetter)
    }
    else {
      category.ngapCoefficients.contains(ngapCoeff) &&
        category.ngapKeyLetters.contains(ngapLetter)
    }
  }

  override def getInput(sources: Sources): DataFrame = {
    sources.mcoCe.get.select((coeffColumn :: keyLetterColumn :: ColNames.core).map(col): _*)
  }
}

class McoCeFbstcNgapActExtractor(ngapConfig: NgapActConfig) extends McoCeNgapActExtractor {
  val keyLetterColumn: String = ColNames.NgapKeyLetterFbstc
  override val columnName: String = keyLetterColumn
  override val eventBuilder: EventBuilder = McoCeFbstcNgapAct
  val ngapActsConfig: NgapActConfig = ngapConfig
  val coeffColumn: String = ColNames.NgapCoefficientFbstc
}

class McoCeFcstcNgapActExtractor(ngapConfig: NgapActConfig) extends McoCeNgapActExtractor {
  val keyLetterColumn: String = ColNames.NgapKeyLetterFcstc
  override val columnName: String = keyLetterColumn
  override val eventBuilder: EventBuilder = McoCeFcstcNgapAct
  val ngapActsConfig: NgapActConfig = ngapConfig
  val coeffColumn: String = ColNames.NgapCoefficientFcstc
}
