package fr.polytechnique.cmap.cnam.filtering

import fr.polytechnique.cmap.cnam.SharedContext

import org.scalatest.mock.MockitoSugar


/**
  * Created by maryanmorel on 23/08/16.
  */
class DiseaseTransformerSuite extends SharedContext with MockitoSugar {
  "DiseaseTransformer" should "use child transformers' transform method in its own transform " +
    "method" in {

    // Given
    val sources: new Sources(null, null, null)

    // When
    // create some mock DiseaseTransformers

    // Then
    // check that all the transform are called

  }

}
