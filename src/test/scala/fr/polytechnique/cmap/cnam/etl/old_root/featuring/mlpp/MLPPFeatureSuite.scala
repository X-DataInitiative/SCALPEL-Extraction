package fr.polytechnique.cmap.cnam.etl.old_root.featuring.mlpp

import org.scalatest.FlatSpec

class MLPPFeatureSuite extends FlatSpec {
  "fromLaggedExposure" should "convert a LaggedExposure object into a MLPPFeature object" in {
    // Given
    val bucketCount = 10
    val lagCount = 10
    val exposure = LaggedExposure("PE", 4, 1, 40, None, "Mol3", 2, 3, 9, 5, 1.0)
    val expected = MLPPFeature("PE", 4, "Mol3", 2, 3, 5, 43, 25, 1.0)
    // When
    val result = MLPPFeature.fromLaggedExposure(exposure, bucketCount, lagCount)
    // Then
    assert(result == expected)
  }
}
