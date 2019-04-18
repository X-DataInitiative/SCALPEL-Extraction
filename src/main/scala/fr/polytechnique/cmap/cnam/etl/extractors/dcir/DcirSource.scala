package fr.polytechnique.cmap.cnam.etl.extractors.dcir

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame}
import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames
import fr.polytechnique.cmap.cnam.util.ColumnUtilities.parseTimestamp

trait DcirSource extends ColumnNames {

  final object ColNames extends Serializable {
    val PatientID: ColName = "NUM_ENQ"
    val MSpe: ColName = "PSE_SPE_COD"
    val NonMSpe: ColName = "PSE_ACT_NAT"
    val ExecPSNum: ColName = "PFS_EXE_NUM"
    val PrestaStart: ColName = "EXE_SOI_DTD"
  }

  final val eventBuilder: Map[ColName, EventBuilder] = Map(
    ColNames.MSpe -> MedicalPrestation,
    ColNames.NonMSpe -> NonMedicalPrestation
  )

  val colNameFromConfig: Map[String, ColName] = Map(
    "medspe" -> ColNames.MSpe,
    "nonmedspe" -> ColNames.NonMSpe
  )
}
