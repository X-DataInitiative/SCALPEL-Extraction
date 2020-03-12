package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Bronchodilatateurs extends DrugClassConfig {

  val name: String = "Bronchodilatateurs"
  val cip13Codes: Set[String] = Set()

  /*
  list pharmacological classes
   */

  val adrenergiquesCourteDuree = new PharmacologicalClassConfig(
    name = "Adrenergiques_Courtes_Durees",
    ATCCodes = List(
      "R03AC02", // Bêta-2 agoniste de courte durée d’action
      "R03AC03",
      "R03AL01", // Bêta-2 + Anticholinergique de courte durée d’action
      "R03BB01" //ATC Anticholinergique de courte durée d’action
    )
  )
  val adrenergiquesBeta2= new PharmacologicalClassConfig(
    name = "Bronchodilatateurs_Adrenergiques_Beta2",
    ATCCodes = List(
      "R03AC13", // Beta-2 agonistes de longue durée d’action (LABA)
      "R03AC18",
      "R03AC19",
      "R03AC12",
      "R03AL04", // Bêta-2 agoniste de longue durée d’action et Anticholinergique de longue durée d’actio
      "R03AL06",
      "R03BB54",
      "R03AL03",
      "R03AK08", // Beta 2 agoniste de longue durée d’action et corticostéroïde inhalé
      "R03AK06",
      "R03AK07",
      "R03AK10",
      "R03AL09", // beta 2 agoniste de longue durée d’action + anticholinergique de longue durée d’action + corticostéroïde inhalé
      "R03AL08"
    )
  )

  val adrenergiquesAnticolinergiques= new PharmacologicalClassConfig(
    name = "Bronchodilatateurs_Adrenergiques_anticolinergiques",
    ATCCodes = List(
      "R03BB06", // Anticholinergique de longue durée d’action (LAMA)
      "R03BB04",
      "R03BB07"
    )
  )
  val pharmacologicalClasses = List(
    adrenergiquesCourteDuree,
    adrenergiquesBeta2,
    adrenergiquesAnticolinergiques
  )

}
