package fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object AntihypertenseursHorsPathologies extends DrugClassConfig {
  override val name: String = "Antihypertenseurs Hors Pathologies"
  override val cip13Codes: Set[String] = Set.empty[String]
  override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List(horsPathologies)


  val horsPathologies = new PharmacologicalClassConfig(
    name = "Antihypertenseurs Hors Pathologies",
    ATCCodes = List(
      "C02AB02", "C02AC01", "C02AC02", "C02AC05", "C02AC06", "C02CA01", "C02CA06", "C02DC01", "C02LA01", "C03AA01",
      "C03AA03", "C03BA04", "C03BA10", "C03BA11", "C03BX03", "C03CA01", "C03CA02", "C03CA03", "C03DA01", "C03DB01",
      "C03EA01", "C03EA04", "C07AA02", "C07AA03", "C07AA05", "C07AA06", "C07AA12", "C07AA15", "C07AA16", "C07AA23",
      "C07AB02", "C07AB03", "C07AB04", "C07AB05", "C07AB07", "C07AB08", "C07AB12", "C07AG01", "C07BA02", "C07BB02",
      "C07BB03", "C07BB07", "C07BB12", "C07CA03", "C07DA06", "C07FB02", "C07FB03", "C08CA01", "C08CA02", "C08CA03",
      "C08CA04", "C08CA05", "C08CA08", "C08CA09", "C08CA11", "C08CA13", "C08CX01", "C08DA01", "C08DB01", "C08GA02",
      "C09AA01", "C09AA02", "C09AA03", "C09AA04", "C09AA05", "C09AA06", "C09AA07", "C09AA08", "C09AA09", "C09AA10",
      "C09AA13", "C09AA15", "C09AA16", "C09BA01", "C09BA02", "C09BA03", "C09BA04", "C09BA05", "C09BA06", "C09BA07",
      "C09BA09", "C09BA15", "C09BB02", "C09BB04", "C09BB10", "C09BX02", "C09CA01", "C09CA02", "C09CA03", "C09CA04",
      "C09CA06", "C09CA07", "C09CA08", "C09DA01", "C09DA02", "C09DA03", "C09DA04", "C09DA06", "C09DA07", "C09DA08",
      "C09DB01", "C09DB02", "C09DB04", "C09XA02", "C09XA52", "C10BX03", "C03EA"
    )
  )

}
