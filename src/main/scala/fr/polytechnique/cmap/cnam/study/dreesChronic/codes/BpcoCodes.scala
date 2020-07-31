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
    "J45", "J961", "J42", "J43", "J44", // BPCO et asthmes : ald 14
    // "J47", "J459", "L209", "J420", "J451", "J841", "J440", "J849" presents in ALD14 with less than 5 occurrences in the echantillon
    "F00", "F03", "F02", "F01", "G30", // ald 15 alzheimer,
    "E84", // ald 18 mucovisidose
    "I42", "I49", "I27"// Insuffisance cardiaque ?  regarder dans les données
  )

  //val primaryDiagCode: String = "C67"
  val primaryDiagCodes: List[String] = List(
    "J440", // Maladie pulmonaire obstructive avec infection aigue des VR inférieures
    "J441", // Maladie pulmonaire obstructive avec épisode aigue sans précision
    "J448", // Autres maladies pulmonaires obstructives précisées
    "J449", // Maladie pulmonaire obstructive sans précision
    "J960", // Insuffisance respi aigue (à chercher avec DA = J44*)
    "J180", // bronchopneumopathie (à chercher avec DA = J44*)
    "J189", // Pneumonie (à chercher avec DA = J44*)
    "I269", // Embolie pumonaire (à chercher avec DA = J44*)
    "I509", // Insuffisance cardiaque aigue (à chercher avec DA = J44*)
    "J181", // Pneumopathie lobaire (à chercher avec DA = J44*)
    "J069", // infection des voies aériennes (à chercher avec DA = J44*)
    "J681", // oedème aigu du poumon (à chercher avec DA = J44*)
    "J10", // Grippe (à chercher avec DA = J44*)
    "Z515"// Soins palliatifs
  )
  val secondaryDiagCodes: List[String] = List("J44")

  val otherCIM10Codes: List[String] = List(
    "J450", // asthme en DP, DA ou DR
    "J46",// etat de mal asthmatique en DP, DA ou DR
    "J451", "J458", "J459",
    "J45",
   // "Z5180", // oxygenotherapie
    "J93", "J942", "J86", "GGJB002", "GGJB001", // pneumothorax
    "I21", "I22", // infarctus
    "Z515", // palliatifs
    "Z532" //sortie contre avis médical (possiblement vide)
  )

  val otherCCAMCodes: List[String] = List(
    "GEME121", // Thermoplastie bronchique
    "EQQP002", //	Mesure de la distance de marche en terrain plat en 6 minutes, avec surveillance de la saturation en oxygène par mesure transcutanée et mesure du débit d'oxygène utile
    "EQQP003", //	Mesure de la distance de marche en terrain plat en 6 minutes, avec surveillance de la saturation en oxygène par mesure transcutanée
    "GLRP001", //	Séance de réentraînement à l'exercice d'un enfant asthmatique, sur machine
    "GLRP002", //	Séance de réentraînement à l'exercice d'un insuffisant respiratoire chronique, sur machine
    "GLRP003", //	Épreuve d'effort sur tapis roulant ou bicyclette ergométrique, avec mesure des gaz du sang [Épreuve d'effort simplifiée] [Gazométrie à l'effort]
    "GLRP004", //	Épreuve d'effort sur tapis roulant ou bicyclette ergométrique, avec mesure des gaz du sang et du débit d'oxygène consommé [VO2], et surveillance électrocardioscopique discontinue
    "GLLD017", // Oxygénothérapie avec surveillance continue de l'oxymétrie, en dehors de la ventilation mécanique, par 24 heures
    "GLLD012" // Ventilation mécanique continue au masque facial pour suppléance ventilatoire, par 24 heures
  )

  val efrCCAMCodes: List[String] = List(
    "GLQP012",	//Mesure de la capacité vitale lente et de l'expiration forcée, avec enregistrement [Spirométrie standard]
    "GLQP003",	//Mesure de l'expiration forcée [Courbe débit-volume] avec enregistrement
    "GERD001",	//Épreuve pharmacodynamique par agent bronchodilatateur, au cours d'une épreuve fonctionnelle respiratoire
    "GERD002",	//Épreuve de provocation par agent bronchoconstricteur ou facteur physique, au cours d'une épreuve fonctionnelle respiratoire
    "GLQD001",	//Mesure de la capacité de transfert pulmonaire du monoxyde de carbone [TLCO] ou d'un autre gaz en apnée ou en état stable, au cours d'une épreuve fonctionnelle respiratoire
    "GLQD003",	//Mesure des volumes pulmonaires non mobilisables par dilution ou rinçage d'un gaz indicateur, au cours d'une épreuve fonctionnelle respiratoire
    "YYYY025",	//Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard	Supplément pour mesure du volume résiduel de la ventilation maximale au cours d'une spirométrie standard
    "GLQP008",	//Mesure de la capacité vitale lente et de l'expiration forcée, avec gazométrie sanguine artérielle [Spirométrie standard avec gaz du sang]
    "GLQP002",	//Mesure de la capacité vitale lente et de l'expiration forcée, avec mesure des volumes pulmonaires mobilisables et non mobilisables par pléthysmographie 
    "GLQP009",	//Mesure de la capacité vitale et du volume courant par pléthysmographie d'inductance
    "GLQP014",	//Mesure du débit expiratoire maximal par technique de compression
    "GLQP011"	  //Mesure des volumes pulmonaires mobilisables et non mobilisables par Pléthysmographie

  )

  //@todo unused because we dont have implemented Lpp AND these are ccam codes (replicated in other ccam)
  val oxygenoLpp: List[String] = List(
    "GLLD017", // Oxygénothérapie avec surveillance continue de l'oxymétrie, en dehors de la ventilation mécanique, par 24 heures
    "GLLD012" // Ventilation mécanique continue au masque facial pour suppléance ventilatoire, par 24 heures
  )

  val gazSangCCAMCodes: List[String] = List(
    "GLHF001", // prélèvement de sang artériel avec gazométrie et mesure du PH sans épreuve d’hyperoxie
    "GLMF001", //	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures	Adaptation des réglages d'une ventilation non effractive par mesures répétées des gaz du sang, par 24 heures
    "GLQF001" //	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures	Réglage du débit d'oxygène par mesures répétées des gaz du sang, pour instauration ou adaptation d'une oxygénothérapie de débit défini, par 24 heures

  )

  val speCodes: List[String] = List(
    "13", "1", "22", "23" // pneumologues et MG
  )

  val nonSpeCodes: List[String] = List(
    //"26" // MK
  )

  val csarrCodes: List[String] = List(
    "DKQ+008",
    "EQQ+206",
    "EQR+175",
    "EQR+275",
    "ANR+036",
    "GLR+074",
    "GLR+077",
    "GLR+093",
    "GLR+131",
    "GLR+139",
    "GLR+167",
    "GLR+169",
    "GLR+170",
    "GLR+186",
    "GLR+224",
    "GLR+226",
    "GLR+236",
    "GLR+285",
    "PCQ+179",
    "PCR+025",
    "PEQ+266",
    "NKR+059",
    "NKR+085",
    "PER+118",
    "PER+285",
    "DKR+013",
    "DKR+016",
    "DKR+061",
    "DKR+118",
    "DKR+181",
    "DKR+182",
    "DKR+194",
    "DKR+195",
    "DKR+200",
    "DKR+247",
    "DKR+254",
    "DKR+291",
    "PCM+064",
    "PCM+253",
    "PCM+262",
    "PCM+283",
    "PCR+004",
    "PCR+272",
    "ZZC+028",
    "ZZC+255",
    "ZZQ+027",
    "ZZQ+261",
    "ZZR+227",
    "ZZR+238",
    "ZZQ+112",
    "ZZQ+192",
    "ZZR+020",
    "ZZR+293",
    "GLQ+043",
    "GLQ+175",
    "GLR+206"
  )
}

