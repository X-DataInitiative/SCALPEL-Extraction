package fr.polytechnique.cmap.cnam.etl.transformer.exposure

import java.sql.Timestamp

/**
  * A class to represent the exposure we want to generate from the data
  *
  * @param periodStrategy Period stratgy. Possible values: "unlimited" | "limited"
  *                       (multiple exposures with start and end)
  * @param startDelay Number of months after which a patient will be considered exposed
  *                   after the min purchases, window.
  * @param minPurchases Minimum number of purchases that have to be made in order
  *                     to be considered exposed.
  * @param purchasesWindow Purchase window, within which the min number of purchases
  *                        have to be made.
  * @param weightAggStrategy Weight Aggregation strategy.
  *                          Possible values: "non-cumulative" |  "purchase-based" |
  *                          "dosage-based" | "time-based"
  * @param cumulativeExposureWindow Number of months to quantile.
  * @param cumulativeStartThreshold Number of months within which more than
  *                                 one purchases have to made
  * @param cumulativeEndThreshold Number of months during which no purchases of
  *                               the particular molecule have to be made
  * @param dosageLevelIntervals List of consumption levels in mg / put only 0 when we want
  *                             all the weights to 1
  * @param purchaseIntervals List of consumption levels in mg / put only 0
  *                          when we want all the weights to 1
  */

case class ExposureDefinition(
    periodStrategy: ExposurePeriodStrategy = ExposurePeriodStrategy.Unlimited,
    startDelay: Int = 3,
    minPurchases: Int = 2,
    purchasesWindow: Int = 6,
    weightAggStrategy: WeightAggStrategy = WeightAggStrategy.NonCumulative,
    cumulativeExposureWindow: Int = 1,
    cumulativeStartThreshold: Int = 6,
    cumulativeEndThreshold: Int = 4,
    dosageLevelIntervals: List[Int] = List(0, 100, 200, 300, 400, 500),
    purchaseIntervals: List[Int] = List(0, 3, 5),
    studyStart: Timestamp, // TODO : remove
    filterDelayedPatients: Boolean, // TODO : remove
    diseaseCode: String) // TODO : remove

