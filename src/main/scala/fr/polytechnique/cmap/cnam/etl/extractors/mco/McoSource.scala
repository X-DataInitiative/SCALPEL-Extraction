package fr.polytechnique.cmap.cnam.etl.extractors.mco

import fr.polytechnique.cmap.cnam.etl.events._
import fr.polytechnique.cmap.cnam.etl.extractors.ColumnNames

trait McoSource extends ColumnNames {

  final object ColNames {
    val PatientID: ColName = "NUM_ENQ"
    val DP: ColName = "MCO_B__DGN_PAL"
    val DR: ColName = "MCO_B__DGN_REL"
    val DA: ColName = "MCO_D__ASS_DGN"
    val CCAM: ColName = "MCO_A__CDC_ACT"
    val GHM: ColName = "MCO_B__GRG_GHM"
    val EtaNum: ColName = "ETA_NUM"
    val RsaNum: ColName = "RSA_NUM"
    val Year: ColName = "SOR_ANN"
    val StayEndMonth: ColName = "MCO_B__SOR_MOI"
    val StayEndYear: ColName = "MCO_B__SOR_ANN"
    val StayLength: ColName = "MCO_B__SEJ_NBJ"
    val StayStartDate: ColName = "ENT_DAT"
    val StayEndDate: ColName = "SOR_DAT"
  }

  object NewColumns {
    val EstimatedStayStart: ColName = "estimated_start"
  }

  final val eventBuilder: Map[ColName, EventBuilder] = Map(
    ColNames.DP -> MainDiagnosis,
    ColNames.DR -> LinkedDiagnosis,
    ColNames.DA -> AssociatedDiagnosis
  )

  val colNameFromConfig: Map[String, ColName] = Map(
    "dp" -> ColNames.DP,
    "dr" -> ColNames.DR,
    "da" -> ColNames.DA
  )

}
