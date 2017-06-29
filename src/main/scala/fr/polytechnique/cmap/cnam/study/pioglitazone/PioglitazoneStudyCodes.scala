package fr.polytechnique.cmap.cnam.study.pioglitazone

/*
 * The codes needed for this study's outcomes are listed in Confluence.
 * Link: https://datainitiative.atlassian.net/wiki/display/CFC/Outcomes
 */

trait PioglitazoneStudyCodes {

  /*
   *  Diagnoses
   */
  val primaryDiagCode: String = "C67"
  val secondaryDiagCodes: List[String] = List("C77", "C78", "C79")

  /*
   *  MCO Acts
   */
  val mcoCIM10ActCodes: List[String] = List(
    "Z511", // Radiotherapy
    "Z510" // Chemotherapy
  )
  val mcoCCAMActCodes: List[String] = List(
    "JDFA001", "JDFA003", "JDFA004", "JDFA005", "JDFA006", "JDFA008", "JDFA009", "JDFA011",
    "JDFA014", "JDFA015", "JDFA016", "JDFA017", "JDFA018", "JDFA019", "JDFA020", "JDFA021",
    "JDFA022", "JDFA023", "JDFA024", "JDFA025", "JDFC023", // List 1
    "JDLD002" // List 2
  )

  /*
   *  DCIR Acts
   */
  val dcirCCAMActCodes: List[String] = List(
    "YYYY045", "YYYY099", "YYYY101", "YYYY046", "YYYY136", "YYYY312", "YYYY047", "YYYY152",
    "YYYY323", "YYYY048", "YYYY211", "YYYY334", "YYYY049", "YYYY197", "YYYY345", "YYYY050",
    "YYYY244", "YYYY356", "YYYY301", "YYYY302", "YYYY313", "YYYY324", "YYYY343", "YYYY335",
    "YYYY346", "YYYY357", "YYYY367", "YYYY368", "YYYY379", "YYYY383", "YYYY390", "YYYY392",
    "YYYY457", "YYYY468", "YYYY471", "YYYY479", "YYYY497", "YYYY303", "YYYY310", "YYYY314",
    "YYYY325", "YYYY336", "YYYY307", "YYYY347", "YYYY358", "YYYY369", "YYYY380", "YYYY387",
    "YYYY391", "YYYY458", "YYYY460", "YYYY469", "YYYY480", "YYYY491", "YYYY299", "YYYY304",
    "YYYY315", "YYYY326", "YYYY331", "YYYY337", "YYYY348", "YYYY359", "YYYY370", "YYYY377",
    "YYYY381", "YYYY398", "YYYY450", "YYYY459", "YYYY470", "YYYY481", "YYYY493", "YYYY492",
    "YYYY305", "YYYY316", "YYYY320", "YYYY327", "YYYY338", "YYYY349", "YYYY360", "YYYY365",
    "YYYY371", "YYYY382", "YYYY451", "YYYY393", "YYYY500", "YYYY511", "YYYY520", "YYYY522",
    "YYYY533", "YYYY544", "YYYY051", "YYYY122", "YYYY555", "YYYY052", "YYYY053", "YYYY054",
    "YYYY055", "YYYY056", "YYYY141", "YYYY175", "YYYY223", "YYYY256", "YYYY267", "YYYY566",
    "YYYY577", "YYYY588", "YYYY599", "YYYY306", "YYYY016", "YYYY021", "YYYY023"
  )
}
