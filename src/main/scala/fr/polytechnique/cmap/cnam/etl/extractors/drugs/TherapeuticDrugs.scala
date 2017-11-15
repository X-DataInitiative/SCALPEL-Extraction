package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import java.sql.Timestamp

import fr.polytechnique.cmap.cnam.etl.events.{Drug, Event}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

case class InputRow(patientID: String, CIP13: String, eventDate: Timestamp)
class TherapeuticDrugs(dcir: DataFrame, Therapeuticfamilies:  List[DrugConfig]) extends DrugPurchases with java.io.Serializable {


  val drugCodes: Map[String, Set[String]] = Therapeuticfamilies.map(family =>
    (family.name, family.cip13Codes)).toMap
  
  val drugDosage: Double = 0.0

  // Returns Some with the family name if the code is found in one of the lists, or None if the code is not found.
  def findTherapeuticFamily(code: String, familyMap: Map[String, Set[String]]): Option[String] = {
    familyMap.collectFirst {
      case (familyName, codes) if codes.contains(code) => familyName
    }
  }

  // Returns Some with a filled DrugEvent if the drugFamily is not empty, or None otherwise
  def buildDrugEvent(patientID: String, drugFamily: Option[String], eventDate: Timestamp): Option[Event[Drug]] = {
    drugFamily.map(family => Drug(patientID, family, drugDosage, eventDate))
  }

  def extract: Dataset[Event[Drug]] = {

    val neededColumns: List[Column] = List(
      col("NUM_ENQ").cast(StringType).as("patientID"),
      col("ER_PHA_F__PHA_PRS_C13").cast(StringType).as("CIP13"),
      col("EXE_SOI_DTD").cast(TimestampType).as("eventDate")
    )

    val spark: SparkSession = dcir.sparkSession
    import spark.implicits._

    dcir
      .select(neededColumns: _*)
      .na.drop(Seq("eventDate", "CIP13"))
      .as[InputRow]
      .flatMap { row: InputRow =>
        val drugFamily = findTherapeuticFamily(row.CIP13, drugCodes)
        buildDrugEvent(row.patientID, drugFamily, row.eventDate)
      }

  }
}
