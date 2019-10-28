package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.study.dreesChronic._

/*
 * The codes needed for this study's outcomes are listed in Confluence.
 * Link: https://datainitiative.atlassian.net/wiki/display/CFC/Outcomes
 */


trait BpcoCodes {

  /*
   *  Diagnoses
   */
  val ALDcodes: List[String] = List(
    "J45", "J961", "J42", "J43", // BPCO et asthmes ald 14
    // "J47", "J459", "L209", "J420", "J451", "J841", "J440", "J849" presents in ALD14 with less than 5 occurrences in the echantillon
    "F00", "F03", "F01", "G30", // ald 15 alzheimer,
    "E84" // ald 18 mucovisidose
  )

  //val primaryDiagCode: String = "C67"
  val primaryDiagCodes: List[String] = List(
    "J440", // Maladie pulmonaire obstructive avec infection aigue des VR inférieures
    "J441", // Maladie pulmonaire obstructive avec épisode aigue sans précision
    "J448", // Autres maladies pulmonaires obstructives précisées
    "J449", // Maladie pulmonaire obstructive sans précision
    "J960", // Insuffisance respi aigue (à chercher avec DA = J44*)
    "J181", // Pneumopathie lombaire (à chercher avec DA = J44*)
    "Z515" // Soins palliatifs
  )
  val secondaryDiagCodes: List[String] = List("J44")

  val otherCIM10Codes: List[String] = List(
    "J45", // asthme en DP, DA ou DR
    "J46", // etat de mal asthmatique en DP, DA ou DR
    "Z5180", // oxygenotherapie
    "J93", "J938", "S270", "S272", "J931", "J930", "S2700", "S2701", "S2720", "S271", // pneumothorax
    "I21", "I22", "I63", // infarctus
    "Z515" // palliatifs
  )
  /*
   *  MCO Acts
   */
  //val mcoCIM10ActCodes: List[String] = List()

  //val mcoCCAMActCodes: List[String] = List("")

  val otherCCAMCodes: List[String] = List(
    "GEME121", // Thermoplastie bronchique
    "GLLD019", "GLMF001", // VNI
    "GLQP017", "GLLD017", "GLQF001", "GLLD021", "GLLP007", "GLLP006" //oxygenotherapie
  )

//  val spiroCCAMCodes: List[String] = List(
//    "GLQP012" // mesure de la capacité vitale lente et de l’expiration forcée avec enregistrement (spirométrie standard)
//  )

  val efrCCAMCodes: List[String] = List(
    "GLQP012", // mesure de la capacité vitale lente et de l’expiration forcée avec enregistrement (spirométrie standard)
    "GLQP008", // mesure de la capacité vitale lente et de l’expiration forcée avec gazométrie sanguine artérielle (spirométrie standard avec gaz du sang)
    "GLQP002", // mesure de la capacité vitale lente et de l’expiration forcée avec mesure des volumes pulmonaires mobilisables et non mobilisables par pléthysismographie
    "GLQP009", // mesure de la capacité vitale lente et du volume courant par pléthysismographie d’inductance
    "GLQP003", // mesure de l’expiration forcée
    "GLQP014", // mesure du débit expiratoire maximal par technique de compression
    "GLQP011" // mesure des volumes pulmonaires mobilisables et non mobilisables par pléthysismographie
  )

  val gazSangCCAMCodes: List[String] = List(
    "GLHF001", // prélèvement de sang artériel avec gazométrie et mesure du PH sans épreuve d’hyperoxie
    "GLHF002" // prélèvement de sang artériel avec gazométrie et mesure du PH avec épreuve d’hyperoxie
  )

  val speCodes: List[String] = List(
    "13", "1", "22", "23" // pneumologues et MG
  )

  val nonSpeCodes: List[String] = List(
    //"26" // MK
  )

  val csarrCodes: List[String] = List()

//  val ghmCodes: List[String] = List(
//    "7956",
//    "7957",
//    "7958",
//    "7991",
//    "7992",
//    "7993",
//    "7994" // tous soins palliatifs
//  )
  /*
   *  DCIR Acts
   */
  //val dcirCCAMActCodes: List[String] = List("")
}

