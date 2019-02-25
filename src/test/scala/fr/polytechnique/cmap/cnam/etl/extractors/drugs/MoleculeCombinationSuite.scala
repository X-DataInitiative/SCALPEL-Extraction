package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Drug
import fr.polytechnique.cmap.cnam.study.fall.codes.Antidepresseurs
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MoleculeCombinationSuite extends SharedContext{

  "transform" should "filter the purchase when the drug doesn't belong to the family" in {

    //Given
    val purchase = PurchaseDAO("PA", "3400934207839654", "N01AA06", makeTS(2014, 3, 3), "a_b_c", 1)
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeCombinationLevel(purchase, families)

    //Then
    assert(result == List.empty)
  }

  "transform" should "convert a purchase to drug events when the drug belongs to the family" in {

    //Given
    val purchase = PurchaseDAO("PA", "3400934207839", "N06AA03", makeTS(2014, 3, 3), "a_b_c", 1)
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeCombinationLevel(purchase, families)
    val expected = List(
      Drug(purchase.patientID, "a_b_c", 1, makeTS(2014, 3, 3))
    )
    //Then
    assert(result == expected)
  }

}
