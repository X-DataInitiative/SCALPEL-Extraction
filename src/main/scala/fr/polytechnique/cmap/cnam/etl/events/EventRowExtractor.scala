package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp
import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.etl.ColumnNames

trait EventRowExtractor { self: ColumnNames =>

  def getPatientId(r: Row): String

  def getGroupId(r: Row): String

  def getWeight(r: Row): Double

  def getStart(r: Row): Timestamp

  def getEnd(r: Row): Option[Timestamp]
}
