package fr.polytechnique.cmap.cnam.etl.extractors.drugs

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.etl.events.Drug
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.Antidepresseurs
import fr.polytechnique.cmap.cnam.etl.extractors.drugs.level.MoleculeLevel
import fr.polytechnique.cmap.cnam.util.functions.makeTS

class MoleculeSuite extends SharedContext {

  "transform" should "filter the purchase when the drug doesn't belong to the family" in {

    //Given
    val purchase = Purchase("PA", "123456", "N05", makeTS(2014, 3, 3), "a_b_c", 1)
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeLevel(purchase, families)

    //Then
    assert(result == List.empty)
  }

  "transform" should "convert a purchase to drug events when the drug belongs to the family" in {

    //Given
    val purchase = Purchase("PA", "3400934207839", "N05", makeTS(2014, 3, 3), "a_b_c", 1)
    val families = List(Antidepresseurs)
    //When
    val result = MoleculeLevel(purchase, families)
    val expected = List(
      Drug(purchase.patientID, "a", 1, makeTS(2014, 3, 3)),
      Drug(purchase.patientID, "b", 1, makeTS(2014, 3, 3)),
      Drug(purchase.patientID, "c", 1, makeTS(2014, 3, 3))
      )
    //Then
    assert(result == expected)
  }

}
