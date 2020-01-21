package fr.polytechnique.cmap.cnam.etl.extractors.ngapacts

import fr.polytechnique.cmap.cnam.etl.events.{Event, EventBuilder, NgapAct}
import fr.polytechnique.cmap.cnam.etl.extractors.mcoCe.McoCeExtractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources
import scala.reflect.runtime.universe._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.util.Try

class McoCeNgapActExtractor(ngapActsConfig: NgapActConfig) extends McoCeExtractor[NgapAct] {

  val columnName: String = ColNames.NgapKeyLetter

  override val eventBuilder: EventBuilder = NgapAct

  override def isInStudy(codes: Set[String])(row: Row): Boolean = {
    ngapActsConfig.pmsiIsInCategories(
      ngapActsConfig.acts_categories,
      ColNames.NgapKeyLetter,
      ColNames.NgapCoefficient,
      row
    )
  }

  override def code: Row => String = (row: Row) => {
    val coeff = Try(row.getAs[Double](ColNames.NgapCoefficient).toString) recover {
      case _: NullPointerException => "0"
    }
    "PmsiCe_" + row.getAs[String](ColNames.NgapKeyLetter) + "_" + coeff.get
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
