package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.ColumnNames

trait EventRowExtractor { self: ColumnNames =>

  def extractPatientId(r: Row): String

  def extractGroupId(r: Row): String

  def extractWeight(r: Row): Double

  def extractStart(r: Row): Timestamp

  def extractEnd(r: Row): Option[Timestamp]
}
