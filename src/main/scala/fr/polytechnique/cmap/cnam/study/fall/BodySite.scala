package fr.polytechnique.cmap.cnam.study.fall


sealed trait CodeType

object CodeType {
  case object CCAM extends CodeType
  case object CIM10 extends CodeType
}

trait BodySite {
  val ghm: String
  val codesCIM10: List[String]
  val codesCCAM: List[String]
}

object BodySite{

  def extractCIM10CodesFromSites(sites: List[BodySite]): List[String] = {
    sites.flatMap(
      site =>
        site match {
          case s: LeafSite => s.codesCIM10
          case s: ForkSite => s.codesCIM10 ++ extractCIM10CodesFromSites(s.sites)
        }
    )
  }

  def getSiteFromCode(code: String, sites: List[BodySite], codeType: CodeType): String = {
    if(sites.isEmpty)
      return ""

    val ghmSite = codeType match {
      case CodeType.CCAM => sites.find(s => s.codesCCAM.exists(ccamCode => code.startsWith(ccamCode)))
      case CodeType.CIM10 => sites.find(s => s.codesCIM10.exists(cim10Code => code.startsWith(cim10Code)))
    }

    if(ghmSite.nonEmpty) return ghmSite.get.ghm

    val sitesNodes = sites.flatMap(s =>
      s match {
        case s: ForkSite => s.sites
        case _: LeafSite => List.empty[BodySite]
      })

    getSiteFromCode(code, sitesNodes, codeType)
  }

  def fromString(site: String): BodySite = site match{
    case "MembreSuperieurProximal" => MembreSuperieurProximal
    case "MembreInferieurDistal" => MembreInferieurDistal
    case "MembreSuperieurDistal" => MembreSuperieurDistal
    case "RestOfBody" => RestOfBody
    case "FemurExclusionCol" => FemurExclusionCol
    case "Clavicule" => Clavicule
    case "Jambe" => Jambe
    case "ColDuFemur" => ColDuFemur
    case "Cheville" => Cheville
    case "Pied" => Pied
    case "Doigt" => Doigt
    case "Poignet" => Poignet
    case "CoudeAvantBras" => CoudeAvantbras
    case "Ribs" => Ribs
    case "BassinRachis" => BassinRachis
    case "Rachis" => Rachis
    case "Bassin" => Bassin
    case "CraneFace" => CraneFace
    case "Crane" => Crane
    case "Face" => Face
    case "Dent" => Dent
    case "AllSites" => AllSites
    case "BodySites" => BodySites
  }
}

trait LeafSite extends BodySite with Serializable{
  val ghm: String
  val codesCIM10:  List[String]
  val codesCCAM: List[String] = List.empty
}

trait ForkSite extends BodySite with Serializable{
  val ghm:String
  val sites: List[BodySite]
  val codesCIM10: List[String]
  val codesCCAM: List[String] = List.empty
}

object Clavicule extends LeafSite{
  override val ghm = "clavicule"
  override val codesCIM10 =  List("S420")
  override val codesCCAM =  List("MADP001")
}

object MembreSuperieurProximal extends LeafSite {
  override val ghm: String = "MembreSuperieurProximal"
  override val codesCIM10 =  List(
    "S422",
    "S423",
    "M80.-2",
    "S427",
    "M80.-1",
    "S421",
    "S429",
    "S428"
  )
  override val codesCCAM =  List("MZMP002", "MBEP001", "MBEP003", "MBEB001", "MAEP001")
}

object Jambe extends LeafSite {
  override val ghm: String = "Jambe"
  override val codesCIM10 =  List("S820", "S821", "S822", "S824", "M80.-6")
}

object Cheville extends LeafSite {
  override val ghm: String = "Cheville"
  override val codesCIM10 =  List("S825", "S826", "S828", "S823")
}

object Pied extends LeafSite {
  override val ghm: String = "Pied"
  override val codesCIM10 =  List("S920", "S921", "S922", "S923", "S924", "S925", "S927", "S929")
}

object MembreInferieurDistal extends ForkSite {
  override val ghm: String = "MembreInferieurDistal"
  override val sites: List[BodySite] = List(Pied, Jambe, Cheville)
  override val codesCIM10: List[String] = List("S827", "S829", "M80.-7")
  override val codesCCAM =  List("NZMP008", "NZMP006", "NZMP014", "NCEP002", "NCEP001")
}

object Doigt extends LeafSite {
  override val ghm: String = "Doigt"
  override val codesCIM10 =  List("S622", "S623", "S624", "S625", "S626", "S627")
}

object Poignet extends LeafSite {
  override val ghm: String = "Poignet"
  override val codesCIM10 =  List("S525", "S526", "S620", "S621")
}

object CoudeAvantbras extends LeafSite {
  override val ghm: String = "CoudeAvantbras"
  override val codesCIM10 =  List("S424", "S520", "S521", "S522", "S523", "S524")
}

object MembreSuperieurDistal extends ForkSite {
  override val ghm: String = "MembreSuperieurDistal"
  override val sites: List[BodySite] = List(CoudeAvantbras, Poignet, Doigt)
  override val codesCIM10: List[String] = List("S527", "S529", "M80.-3", "M80.-4", "S628", "S528")
  override val codesCCAM: List[String] = List("MZMP013", "MZMP004", "MZMP007", "MBEP002", "MCEP002")
}

object AllSites extends LeafSite{
  override val ghm: String = "AllSites"
  override val codesCIM10: List[String] = List(
    "S02",
    "S12",
    "S22",
    "S32",
    "S42",
    "S52",
    "S62",
    "S72",
    "S82",
    "S92",
    "T02",
    "T08",
    "T10",
    "T12",
    "T14.2",
    "M48.4",
    "M48.5",
    "M80"
  )
}

object ColDuFemur extends LeafSite {
  override val ghm: String = "ColDuFemur"
  override val codesCIM10: List[String] = List("S720", "S721")
}

object FemurExclusionCol extends LeafSite {
  override val ghm: String = "FemurExclusionCol"
  override val codesCIM10: List[String] = List("S723", "S724", "S728", "S727", "S729", "S722")
  override val codesCCAM: List[String] = List("NBEP002", "NBEP001", "NBEB001")
}

object BassinRachis extends ForkSite {
  override val ghm: String = "BassinRachis"
  override val codesCIM10: List[String] = List("S327", "S328", "S128", "S129")
  override val sites: List[BodySite] = List(Rachis, Bassin)
  override val codesCCAM: List[String] = List("NAEP002", "NAEP001")
}

object Ribs extends LeafSite {
  override val ghm: String = "ribs"
  override val codesCIM10: List[String] = List("S222", "S223", "S224", "S225")
}

object Rachis extends LeafSite {
  override val ghm: String = "rachis"
  override val codesCIM10: List[String] = List("S120", "S121", "S122", "S127", "S220", "S221", "S320","T08", "M485")
}

object Bassin extends LeafSite {
  override val ghm: String = "Bassin"
  override val codesCIM10: List[String] = List("S321", "S322", "S323", "S324", "S325")
}

object CraneFace extends ForkSite {
  override val ghm: String = "CraneFace"
  override val codesCIM10: List[String] = List("S027", "S028", "S029")
  override val sites: List[BodySite] = List(Crane, Dent, Face)
  override val codesCCAM: List[String] = List(
    "LAEA008", "LAEA001", "LAEA003", "HBED009", "HBED015", "LAEP002",
    "LAEP003", "LAEP001", "LBED001", "LBED004", "LBEP009", "LAEA007",
    "LAEB001", "LBEP002", "LBED002", "LBED005", "LBED006", "LBED003")
}

object Crane extends LeafSite {
  override val ghm: String = "Crane"
  override val codesCIM10: List[String] = List("S020", "S021")
}

object Dent extends LeafSite {
  override val ghm: String = "Dent"
  override val codesCIM10: List[String] = List("S025")
}

object Face extends LeafSite {
  override val ghm: String = "Face"
  override val codesCIM10: List[String] = List("S022", "S023", "S024", "S026")
}

object RestOfBody extends LeafSite{
  override val ghm: String = "restOfBody"
  override val codesCIM10: List[String] = List("M80.-5", "M80.-8", "M80.-9", "M80.-0")
}

object BodySites extends ForkSite{
  override val ghm: String = "BodySites"
  override val sites: List[BodySite] = List(Ribs, MembreInferieurDistal, MembreSuperieurProximal, MembreSuperieurDistal, CraneFace, RestOfBody, ColDuFemur, FemurExclusionCol, BassinRachis, Clavicule)
  override val codesCIM10: List[String] = List.empty
}

