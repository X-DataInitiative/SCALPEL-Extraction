# Events

An [Event](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/events/Event.scala) 
 is a figure that allows to homogenize the content of the output datasets.
It consists, at least, of an object to be instantiated and a trait containing the necessary functions. 
In order to create an  `Event` you first need to understand what it is. 
An event is an occurrence defined by having a patient identifier, category, start, group identifier, value, weight and  end; 
of these only the first three are mandatory.The 7 elements needed to form an `Event` are:
- patientID: is the patient identifier.
- category: define the event's category.
- groupID: contains the ID of a group of related events.
- value: contains string values fot the molecule name, the diagnosis code, etc.
- weight: contains double values for medical acts weighting or other numerical values.
- start: is the start of event's period.
- end: is the end of event's period.
```scala  
  patientID: String,
  category: EventCategory[A],
  groupID: String,
  value: String,
  weight: Double,
  start: Timestamp,
  end: Option[Timestamp] 
```  
All events inherit [AnyEvent](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/events/AnyEvent.scala)
 and  [EventBuilder](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/events/EventBuilder.scala). 
 `AnyEvent` is a trait with category value, and `EventBuilder` is a trait to built  an `Event`.
 
 
[ObservationPeriod](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/events/ObservationPeriod.scala)
and [MedicalAct](https://github.com/X-DataInitiative/SCALPEL-Extraction/blob/master/src/main/scala/fr/polytechnique/cmap/cnam/etl/events/MedicalAct.scala)
are good examples to illustrate the construction of an `Event`.
The first one,`ObservationPeriod`, define the values  `patientID`, `category`, `start` and if exist `end`. 
The other values are present but use default values. Its trait just assigns the value of the category and uses the apply method to build an `Event`.
```scala  
 val category: EventCategory[ObservationPeriod] = "observation_period"

  /** Creates un Event object of type ObservationPeriod using a map function to map a dataset.
   *
   * @param patientID The value patientID from dataset.
   * @param start     The value start from dataset.
   * @param end       The value end from dataset.
   * @return Event[ObservationPeriod].
   */
  def apply(patientID: String, start: Timestamp, end: Timestamp): Event[ObservationPeriod] =
    Event(patientID, category, groupID = "NA", value = "NA", weight = 0D, start, Some(end))
```  
And its object doesn't add up to anything, it just inherits the trait.
```scala  
object ObservationPeriod extends ObservationPeriod
```
On the other side,`MedicalAct` defines values for all elements except `end` one. 
Its trait uses two apply methods to assign values according to need and not assign any value to category one, 
it's assigned in each object according to the type.

```scala  
 
  override val category: EventCategory[MedicalAct]

  def apply(patientID: String, groupID: String, code: String, weight: Double, date: Timestamp): Event[MedicalAct] = {
    Event(patientID, category, groupID, code, weight, date, None)
  }

  def apply(patientID: String, groupID: String, code: String, date: Timestamp): Event[MedicalAct] = {
    Event(patientID, category, groupID, code, 0.0, date, None)
  }
``` 
The MedicalAct's object are various in accordance with the type of medical act. 
Their objects assign categories and in some cases have object that stores groupID values.

```scala 
object BiologyDcirAct extends MedicalAct {
  override val category: EventCategory[MedicalAct] = "dcir_biology_act"

  object groupID {
    val PrivateAmbulatory = "private_ambulatory"
    val PublicAmbulatory = "public_ambulatory"
    val PrivateHospital = "private_hospital"
    val Liberal = "liberal"
    val DcirAct = "DCIR_act"
    val Unknown = "unknown_source"
  }
}
```