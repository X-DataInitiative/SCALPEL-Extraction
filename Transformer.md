# Transformers

A transformer is used to turn one or multiple  datasets  
into another one. A  `Transformer`  accept a configuration class as a parameter 
a configuration class of type
 [TransformerConfig](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/transformers/TransformerConfig.scala).
This configuration file control the behaviour of the `Transformer` through its methods and values.


`transform` is the main and the unique public available method of `Transformer`, this method accepts one or several datasets  as input
 and after the application of the transformation logic, returns a dataset of type `Event`([Events](Events.md)) with a new `category`.
 
 If we take as an example [ExposureTransformer](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/transformers/exposures/ExposureTransformer.scala),
 we pass an instance of the class [ExposuresTransformerConfig](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/transformers/exposures/ExposuresTransformerConfig.scala)
 as parameter, it extends a `TransformerConfig`.
 ```scala  
/**
  * A tag to improve readability of subclasses and to allow type binding
  */

trait TransformerConfig 
 ```
The config class is used to control the Transformer's  behaviour.
 ```scala  
class ExposuresTransformerConfig(
  val exposurePeriodAdder: ExposurePeriodAdder) extends TransformerConfig with Serializable
 ```  
This main method take as parameter two datasets of type `Event` and subtypes `FollowUp` and `Drug` respectively
and return a dataset of type `Event` and subtype `Exposure`.

```scala 
  def transform(followUps: Dataset[Event[FollowUp]])(drugs: Dataset[Event[Drug]]): Dataset[Event[Exposure]] = {
    drugs
      .transform(config.exposurePeriodAdder.toExposure(followUps))
      .transform(regulateWithFollowUps(followUps))
  }
 ```
The objective of the `transform` method is thus to combine, filter, check relations between multiples `Event`s to form a new `Event`.
In the example of the `ExposureTransformer`, it combines multiples `Drug` `Event`s based on the logic of 
the `exposurePeriodAdder` of the `ExposuresTransformerConfig` to form an `Exposure` while 
making sure that it is contained within the period defined in the `FollowUp` `Event`.