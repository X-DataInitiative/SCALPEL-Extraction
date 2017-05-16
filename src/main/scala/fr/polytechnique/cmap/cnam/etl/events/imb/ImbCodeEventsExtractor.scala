package fr.polytechnique.cmap.cnam.etl.events.imb

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import fr.polytechnique.cmap.cnam.etl.config.ExtractionConfig
import fr.polytechnique.cmap.cnam.etl.events.diagnoses.ImbDiagnosis
import fr.polytechnique.cmap.cnam.etl.events.{AnyEvent, Event}

object ImbCodeEventsExtractor extends ImbEventRowExtractor {

  def eventsFromRow(codes: List[String])(r: Row): List[Event[AnyEvent]] = {

    val foundCode: Option[String] = extractCode(r, codes)

    foundCode.map {
      code => ImbDiagnosis(
        extractPatientId(r), extractGroupId(r), code, extractWeight(r), extractStart(r), None
      )
    }.toList
  }

  def extract(config: ExtractionConfig, imb: DataFrame): Dataset[Event[AnyEvent]] = {

    import imb.sqlContext.implicits._
    val imbCodes: List[String] = config.codesMap("imb")
    imb
      .withColumn(ColNames.Date, col(ColNames.Date).cast(TimestampType))
      .flatMap(eventsFromRow(imbCodes))
  }
}
