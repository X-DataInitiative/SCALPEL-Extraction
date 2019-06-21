package fr.polytechnique.cmap.cnam.etl.extractors

import java.sql.Timestamp
import org.apache.spark.sql.Row

trait EventRowExtractor {

  def extractPatientId(r: Row): String

  def extractGroupId(r: Row): String = "NA"

  def extractWeight(r: Row): Double = 0.0

  def extractStart(r: Row): Timestamp

  def extractEnd(r: Row): Option[Timestamp] = None
}
