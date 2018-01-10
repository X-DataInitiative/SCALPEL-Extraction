package fr.polytechnique.cmap.cnam.study.fall

import fr.polytechnique.cmap.cnam.SharedContext

class SiteSuite extends SharedContext {

  "extractCodeSites" should "extract the right codes given a list of sites" in {

    //Given
    val input = List(Clavicule, MembreInferieurDistal)

    //When
    val result = Site.extractCodesFromSites(input)

    val expected = List("S420", "S827", "S829", "M80.-7", "S920", "S921", "S922", "S923", "S924", "S925", "S927", "S929",
       "M80.-6", "S825", "S826", "S828", "S823", "S820", "S821", "S822", "S824")

    //Then
    assert(result.sorted == expected.sorted)
  }

  "extractCodeSites" should "extract the right codes given the root site" in {

    //Given
    val input = List(BodySites)

    //When
    val result = Site.extractCodesFromSites(input)
    val expected = List("S420", "S422", "S423", "M80.-2", "S427", "M80.-1", "S421", "S429", "S428", "S424", "S520", "S521", "S522", "S523", "S524",
      "S525", "S526", "S620", "S621", "S622", "S623", "S624", "S625", "S626", "S627", "S527", "S529", "M80.-3", "M80.-4", "S628", "S528", "S720",
      "S721", "S723", "S724", "S728", "S727", "S729", "S722", "S820", "S821", "S822", "S824", "M80.-6", "S825", "S826", "S828", "S823", "S920",
      "S921", "S922", "S923", "S924", "S925", "S927", "S929", "S827", "S829", "M80.-7", "S222", "S223", "S224", "S225", "S120", "S121", "S122",
      "S127", "S220", "S221", "S320", "T08", "M485", "S321", "S322", "S323", "S324", "S325", "S327", "S328", "S128", "S129", "S020",
      "S021", "S025", "S022", "S023", "S024", "S026", "S027", "S028", "S029", "M80.-5", "M80.-8", "M80.-9", "M80.-0"
    )

    //Then
    assert(result.sorted == expected.sorted)
  }

  "doesSiteContainsCode" should "return true if the site contains the code" in {

    //Given
    val input = "S420"
    //When
    val result = Site.siteContainsCode(input, Clavicule)
    //Then
    assert(result)
  }

  "doesSiteContainsCode" should "return false if the site doesn't contains the code" in {

    //Given
    val input = "S420"
    //When
    val result = Site.siteContainsCode(input, BodySites)
    //Then
    assert(!result)
  }

  "getSiteFromCode" should "get the correct site given the code" in {

    //Given
    val input = "S420"
    //When
    val result = Site.getSiteFromCode(input, List(BodySites))
    val expected = "clavicule"

    //Then
    assert(result == expected)
  }
}
