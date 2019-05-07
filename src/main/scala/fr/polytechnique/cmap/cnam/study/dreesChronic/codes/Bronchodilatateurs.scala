package fr.polytechnique.cmap.cnam.study.dreesChronic.codes

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Bronchodilatateurs extends DrugClassConfig {

  val name: String = "Bronchodilatateurs"
  val cip13Codes: Set[String] = Set(
    "3400931812616",
    "3400932668274",
    "3400932698721",
    "3400932962020",
    "3400932967513",
    "3400932990344",
    "3400933029777",
    "3400933030148",
    "3400933123031",
    "3400933189617",
    "3400933286170",
    "3400933457396",
    "3400933457686",
    "3400933457808",
    "3400933459000",
    "3400933459468",
    "3400933459697",
    "3400933536381",
    "3400933773373",
    "3400933773434",
    "3400933919467",
    "3400933919818",
    "3400934060335",
    "3400934060625",
    "3400934082276",
    "3400934082337",
    "3400934438738",
    "3400934653896",
    "3400935159052",
    "3400935320742",
    "3400935321114",
    "3400935842664",
    "3400936351820",
    "3400936412026",
    "3400936412194",
    "3400936566460",
    "3400936573093",
    "3400936573154",
    "3400936573383",
    "3400936578067",
    "3400936578418",
    "3400936579019",
    "3400936579309",
    "3400936579538",
    "3400936579996",
    "3400936580138",
    "3400936580367",
    "3400936580596",
    "3400936580947",
    "3400936581319",
    "3400936581548",
    "3400936581777",
    "3400936584389",
    "3400936715165",
    "3400936715394",
    "3400936869202",
    "3400936951266",
    "3400936951495",
    "3400937672689",
    "3400937672740",
    "3400937672979",
    "3400938192032",
    "3400938940091",
    "3400938940671",
    "3400938953688",
    "3400938954111",
    "3400939982236",
    "3400939982526",
    "3400949129560",
    "3400949411429",
    "3400949411658"
  )

  /*
  list pharmacological classes
   */
  val adrenergiquesBeta2= new PharmacologicalClassConfig(
    name = "Bronchodilatateurs_Adrenergiques_Beta2",
    ATCCodes = List("R03AC*")
  )

  val adrenergiquesAnticolinergiques= new PharmacologicalClassConfig(
    name = "Bronchodilatateurs_Adrenergiques_anticolinergiques",
    ATCCodes = List("R03BB*")
  )
  val pharmacologicalClasses = List(
    adrenergiquesBeta2,
    adrenergiquesAnticolinergiques
  )

}
