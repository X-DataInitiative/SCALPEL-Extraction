package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Drug
import fr.polytechnique.cmap.cnam.study.fall.codes.Antidepresseurs
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MoleculeSuite extends SharedContext {

  "transform" should "filter the purchase when the drug doesn't belong to the family" in {

    //Given
    val purchase = Purchase("PA", "123456", "N05", makeTS(2014, 3, 3), "a_b_c")
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeLevel(purchase, families)

    //Then
    assert(result == List.empty)
  }

  "transform" should "convert a purchase to drug events when the drug belongs to the family" in {

    //Given
    val purchase = Purchase("PA", "3400934207839", "N05", makeTS(2014, 3, 3), "a_b_c")
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeLevel(purchase, families)
    val expected = List(
      Drug(purchase.patientID, "a", 0, makeTS(2014, 3, 3)),
      Drug(purchase.patientID, "b", 0, makeTS(2014, 3, 3)),
      Drug(purchase.patientID, "c", 0, makeTS(2014, 3, 3))
      )
    //Then
    assert(result == expected)
  }

}
