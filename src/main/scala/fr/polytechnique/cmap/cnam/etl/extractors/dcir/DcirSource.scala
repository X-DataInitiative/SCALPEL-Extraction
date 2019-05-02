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
    val DcirEventStart: ColName = "EXE_SOI_DTD"
    val DcirFluxDate: ColName = "FLX_DIS_DTD"
    lazy val CamCode: String = "ER_CAM_F__CAM_PRS_IDE"
    lazy val GHSCode: String = "ER_ETE_F__ETE_GHS_NUM"
    lazy val InstitutionCode: String = "ER_ETE_F__ETE_TYP_COD"
    lazy val Sector: String = "ER_ETE_F__PRS_PPU_SEC"
    lazy val Date: String = "EXE_SOI_DTD"
    lazy val all = List(PatientID, CamCode, GHSCode, InstitutionCode, Sector, Date)

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
