package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import fr.polytechnique.cmap.cnam.etl.events.{Event, EventBuilder, McoCeFbstcNgapAct, McoCeFcstcNgapAct, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.reflect.runtime.universe._
import scala.util.Try

trait McoCeNgapActExtractor extends McoCeExtractor[NgapAct] {
  val ngapActsConfig: NgapActConfig
  val keyLetterColumn: String
  val coeffColumn: String

  val columnName: String = keyLetterColumn

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    ngapActsConfig.pmsiIsInCategories(
      ngapActsConfig.acts_categories,
      keyLetterColumn,
      coeffColumn,
      row
    )
  }

  override def code: Row => String = (row: Row) => {
    val coeff = Try(row.getAs[Double](coeffColumn).toString) recover {
      case _: NullPointerException => "0"
    }
    "PmsiCe_" + row.getAs[String](keyLetterColumn) + "_" + coeff.get
  }

  override def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.SeqNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  override def extract(
    sources: Sources,
    codes: Set[String])
    (implicit ctag: TypeTag[NgapAct]): Dataset[Event[NgapAct]] = {

    val input: DataFrame = getInput(sources)

    import input.sqlContext.implicits._

    {
      if (ngapActsConfig.acts_categories.isEmpty) {
        input.filter(isInExtractorScope _)
      }
      else {
        input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
      }
      }.flatMap(builder _).distinct()
  }
}

class McoCeFbstcNgapActExtractor(ngapConfig: NgapActConfig) extends McoCeNgapActExtractor {
  val ngapActsConfig: NgapActConfig = ngapConfig
  val keyLetterColumn: String = ColNames.NgapKeyLetterFbstc
  val coeffColumn: String = ColNames.NgapCoefficientFbstc
  override val columnName: String = keyLetterColumn
  override val eventBuilder: EventBuilder = McoCeFbstcNgapAct
}

class McoCeFcstcNgapActExtractor(ngapConfig: NgapActConfig) extends McoCeNgapActExtractor {
  val ngapActsConfig: NgapActConfig = ngapConfig
  val keyLetterColumn: String = ColNames.NgapKeyLetterFcstc
  val coeffColumn: String = ColNames.NgapCoefficientFcstc
  override val columnName: String = keyLetterColumn
  override val eventBuilder: EventBuilder = McoCeFcstcNgapAct
}
