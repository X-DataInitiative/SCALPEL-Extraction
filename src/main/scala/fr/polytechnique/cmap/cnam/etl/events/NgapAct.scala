// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.etl.events

import java.sql.Timestamp

/** The NGAP is one of the two different nomenclatures used in the SNDS to facture healthcare acts.
 *
 * It concerns mainly nurses, physiotherapist masseurs and medical auxiliaries and
 * some acts of dental surgeons as well as clinical acts of doctors. It has to be distinguished from the CCAM,
 * which groups together the technical acts performed by doctors (much more precise).
 * A updated date version can be found on the website of the CNAM :
 * https://www.ameli.fr/ain/masseur-kinesitherapeute/exercice-liberal/facturation-remuneration/nomenclatures-ngap-et-lpp/nomenclatures-ngap-lpp)
 *
 */
trait NgapAct extends AnyEvent with EventBuilder {

   override val category: EventCategory[NgapAct] = "ngap_act"

  def apply(patientID: String, groupID: String, ngapCoefficient: String, weight: Double, date: Timestamp): Event[NgapAct] = {
    Event(patientID, category, groupID, ngapCoefficient, weight, date, None)
  }

  def apply(patientID: String, groupID: String, ngapCoefficient: String, date: Timestamp): Event[NgapAct] = {
    Event(patientID, category, groupID, ngapCoefficient, 0.0, date, None)
  }
}



object DcirNgapAct extends NgapAct {
  override val category: EventCategory[NgapAct] = "dcir_ngap_act"

  object groupID {
    val PrivateAmbulatory = "private_ambulatory"
    val PublicAmbulatory = "public_ambulatory"
    val PrivateHospital = "private_hospital"
    val Liberal = "liberal"
    val DcirNgapAct = "dcir_ngap_act"
    val Unknown = "unknown_source"
  }

}

/**
 * Tables of hospital services (FBSTC) and procedures (FCSTC) are not completed for each stay and are complementary.
 * All the details are in the collaborative documentation on the SNDS here :
 * https://documentation-snds.health-data-hub.fr/fiches/actes_consult_externes.html#reperage-des-ace-dans-la-table-des-prestations-dcir
 */
object McoCeFbstcNgapAct extends NgapAct {
  override val category: EventCategory[NgapAct] = "mco_ce_fbstc_act"
}

object McoCeFcstcNgapAct extends NgapAct {
  override val category: EventCategory[NgapAct] = "mco_ce_fcstc_act"
}

object SsrCeFbstcNgapAct extends NgapAct {
  override val category: EventCategory[NgapAct] = "ssr_ce_fbstc_act"
}

object SsrCeFcstcNgapAct extends NgapAct {
  override val category: EventCategory[NgapAct] = "ssr_ce_fcstc_act"
}