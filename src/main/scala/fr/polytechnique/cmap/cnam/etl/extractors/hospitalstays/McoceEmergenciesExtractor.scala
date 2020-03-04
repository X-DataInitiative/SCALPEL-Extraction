package fr.polytechnique.cmap.cnam.etl.extractors.hospitalstays

import java.sql.{Date, Timestamp}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import fr.polytechnique.cmap.cnam.etl.events.{Event, HospitalStay, McoceEmergency}
import fr.polytechnique.cmap.cnam.etl.extractors.Extractor
import fr.polytechnique.cmap.cnam.etl.sources.Sources

object McoceEmergenciesExtractor extends Extractor[HospitalStay] with McoceEmergenciesExtractor {
  /** Allows to check if the Row from the Source is considered in the current Study.
   *
   * @param codes A set of codes being considered in the Study.
   * @param row   The row itself.
   * @return A boolean value.
   */
  override def isInStudy(codes: Set[String])(row: Row): Boolean = true

  /** Checks if the passed Row has the information needed to build the Event.
   *
   * @param row The row itself.
   * @return A boolean value.
   */
  override def isInExtractorScope(row: Row): Boolean = !row.isNullAt(row.fieldIndex(ColNames.ActCode)) && row.getAs[String](ColNames.ActCode).startsWith("ATU")

  /** Builds the Event.
   *
   * @param row The row itself.
   * @return An event object.
   */
  override def builder(row: Row): Seq[Event[HospitalStay]] = {
    val patientID = extractPatientId(row)
    val groupId = extractGroupId(row)
    val start = extractStart(row)
    val end = extractEnd(row)
    Seq(McoceEmergency(patientID, groupId, start, end))
  }

  /** Gets and prepares all the needed columns from the Source.
   *
   * @param sources Source object [[Sources]] that contains all sources.
   * @return A dataframe with mco columns.
   */
  override def getInput(sources: Sources): DataFrame = sources.mcoCe.get.select(ColNames.all.map(col): _*)
}

trait McoceEmergenciesExtractor {

  final object ColNames extends Serializable {
    final val PatientID: String = "NUM_ENQ"
    final val EtaNum: String = "ETA_NUM"
    final val SeqNum: String = "SEQ_NUM"
    final val StartDate: String = "EXE_SOI_DTD"
    final val EndDate: String = "EXE_SOI_DTF"
    final val Year: String = "MCO_FBSTC__SOR_ANN"
    final val ActCode: String = "MCO_FBSTC__ACT_COD"
    final val all: List[String] = List(PatientID, EtaNum, SeqNum, Year, StartDate, EndDate, ActCode)
  }

  def extractPatientId(r: Row): String = {
    r.getAs[String](ColNames.PatientID)
  }

  def extractGroupId(r: Row): String = {
    r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.SeqNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }

  def extractEnd(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.EndDate).getTime)

  def extractStart(r: Row): Timestamp = new Timestamp(r.getAs[Date](ColNames.StartDate).getTime)

}
