

# Extractors

Extractors are a kind of jobs that allows to extract the required columns from the sources and maps them to the `Event` ([Events](Events.md)) it is extracting. 
An extractor is composed of several basic components that are grouped to give all the functionalities to the extractors. 
From a point of view hierarchical we have:

1) Base elements are traits as [Extractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/Extractor.scala), 
[McoSource](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/mco/McoSource.scala) and 
[EventRowExtractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/EventRowExtractor.scala).
Each manages different parts of extraction. They are all necessary for the creation of an element of the next level.
2) Intermediate elements are traits that bring together the basic methods to create a common trait to extract the data from the sources (mco,dcir,ssr,had).In our example, 
[McoExtractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/mco/McoExtractor.scala) 
is the base for retrieving the necessary data from mco source. These elements inherit the base elements and accept as a parameter a trait of the type EventType
3) The upper elements  are objects and  they are the entry point of the job itself, they inherit from the middle elements and specialize it in one type of `Event`. 
 
Each basic component has a function:
 - [Extractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/Extractor.scala)
 is a trait that works by filtering, extracting and building.Its main method  `extract ` allows to filter the sources and build a dataset of type `Event`.
```scala  
def extract(sources: Sources, codes: Set[String])(implicit ctag: TypeTag[EventType]): Dataset[Event[EventType]] = {
            val input: DataFrame = getInput(sources)      
            import input.sqlContext.implicits._        
            {
              if (codes.isEmpty) {
                input.filter(isInExtractorScope _)
              }
              else {
                input.filter(isInExtractorScope _).filter(isInStudy(codes) _)
              }
            }.flatMap(builder _).distinct()
          }
```
- [McoSource](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/mco/McoSource.scala)
This trait is specific to each source (mco,dcir,ssr,had), containing the values relating to the columns and the methods specific to that source.
- [EventRowExtractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/EventRowExtractor.scala) 
This trait contains methods for extracting the fields needed to create an `Event`.

The intermediate elements implement the required methods and adapt them if necessary. 
Two good examples of implementation and modification to suit the specificities of `Event` type are `builder` method and  `extractGroupId` method.

```scala  
def builder(row: Row): Seq[Event[EventType]] = {
    lazy val patientId = extractPatientId(row)
    lazy val groupId = extractGroupId(row)
    lazy val eventDate = extractStart(row)
    lazy val endDate = extractEnd(row)
    lazy val weight = extractWeight(row)

    Seq(eventBuilder[EventType](patientId, groupId, code(row), weight, eventDate, endDate))
  }
```
```scala  
 override def extractGroupId(r: Row): String = {
      r.getAs[String](ColNames.EtaNum) + "_" +
      r.getAs[String](ColNames.RsaNum) + "_" +
      r.getAs[Int](ColNames.Year).toString
  }
```

The above elements are responsible for defining the type of `Event` and must implement at least  `columnName` and  `eventBuilder` values and modify them according to their specificity.
For exemple at [McoHospitalStaysExtractor](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/extractors/hospitalstays/McoHospitalStaysExtractor.scala)
```scala  
object McoHospitalStaysExtractor extends McoExtractor[HospitalStay]{

  override val columnName: String = ColNames.EndDate
  override val eventBuilder: EventBuilder = McoHospitalStay
}
```


