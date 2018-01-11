package fr.polytechnique.cmap.cnam.study.fall


trait BodySite {
  val ghm: String
  val codes: List[String]
}

object BodySite{

  val allSites = List(BodySites, AllSites)
  def extractCodesFromSites(sites: List[BodySite]): List[String] = {
    sites.flatMap(
      site =>
        site match {
          case s: LeafSite => s.codes
          case s: ForkSite => s.codes ++ extractCodesFromSites(s.sites)
        }
    )
  }
  def siteContainsCode(code: String, site: BodySite): Boolean = site.codes.exists(code.startsWith(_))

  def getSiteFromCode(code: String, sites: List[BodySite]): String ={

    val ghmSite = sites.find(s => siteContainsCode(code, s))
    if(ghmSite.nonEmpty) return ghmSite.get.ghm

    val sitesnodes = sites.flatMap(s =>
      s match {
        case s: ForkSite => s.sites
        case s: LeafSite => List.empty[BodySite]
      })
    getSiteFromCode(code, sitesnodes)

  }
}

trait LeafSite extends BodySite with Serializable{
  val ghm: String
  val codes:  List[String]
}

trait ForkSite extends BodySite with Serializable{
  val ghm:String
  val sites: List[BodySite]
  val codes: List[String]
}

object Clavicule extends LeafSite{
  override val ghm = "clavicule"
  override val codes =  List("S420")
}

object MembreSuperieurProximal extends LeafSite {
  override val ghm: String = "MembreSuperieurProximal"
  override val codes =  List(
    "S422",
    "S423",
    "M80.-2",
    "S427",
    "M80.-1",
    "S421",
    "S429",
    "S428"
  )
}

object Jambe extends LeafSite {
  override val ghm: String = "Jambe"
  override val codes =  List("S820", "S821", "S822", "S824", "M80.-6")
}

object Cheville extends LeafSite {
  override val ghm: String = "Cheville"
  override val codes =  List("S825", "S826", "S828", "S823")
}

object Pied extends LeafSite {
  override val ghm: String = "Pied"
  override val codes =  List("S920", "S921", "S922", "S923", "S924", "S925", "S927", "S929")
}

object MembreInferieurDistal extends ForkSite {
  override val ghm: String = "MembreInferieurDistal"
  override val sites: List[BodySite] = List(Pied, Jambe, Cheville)
  override val codes: List[String] = List("S827", "S829", "M80.-7")
}

object Doigt extends LeafSite {
  override val ghm: String = "Doigt"
  override val codes =  List("S622", "S623", "S624", "S625", "S626", "S627")
}

object Poignet extends LeafSite {
  override val ghm: String = "Poignet"
  override val codes =  List("S525", "S526", "S620", "S621")
}

object CoudeAvantbras extends LeafSite {
  override val ghm: String = "CoudeAvantbras"
  override val codes =  List("S424", "S520", "S521", "S522", "S523", "S524")
}

object MembreSuperieurDistal extends ForkSite {
  override val ghm: String = "MembreSuperieurDistal"
  override val sites: List[BodySite] = List(CoudeAvantbras, Poignet, Doigt)
  override val codes: List[String] = List("S527", "S529", "M80.-3", "M80.-4", "S628", "S528")
}

object AllSites extends LeafSite{
  override val ghm: String = "AllSites"
  override val codes: List[String] = List(
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
  override val codes: List[String] = List("S720", "S721")
}

object FemurExclusionCol extends LeafSite {
  override val ghm: String = "FemurExclusionCol"
  override val codes: List[String] = List("S723", "S724", "S728", "S727", "S729", "S722")
}

object BassinRachis extends ForkSite {
  override val ghm: String = "BassinRachis"
  override val codes: List[String] = List("S327", "S328", "S128", "S129")
  override val sites: List[BodySite] = List(Rachis, Bassin)
}

object Ribs extends LeafSite {
  override val ghm: String = "ribs"
  override val codes: List[String] = List("S222", "S223", "S224", "S225")
}

object Rachis extends LeafSite {
  override val ghm: String = "rachis"
  override val codes: List[String] = List("S120", "S121", "S122", "S127", "S220", "S221", "S320","T08", "M485")
}

object Bassin extends LeafSite {
  override val ghm: String = "Bassin"
  override val codes: List[String] = List("S321", "S322", "S323", "S324", "S325")
}

object CraneFace extends ForkSite {
  override val ghm: String = "CraneFace"
  override val codes: List[String] = List("S027", "S028", "S029")
  override val sites: List[BodySite] = List(Crane, Dent, Face)
}

object Crane extends LeafSite {
  override val ghm: String = "Crane"
  override val codes: List[String] = List("S020", "S021")
}

object Dent extends LeafSite {
  override val ghm: String = "Dent"
  override val codes: List[String] = List("S025")
}

object Face extends LeafSite {
  override val ghm: String = "Face"
  override val codes: List[String] = List("S022", "S023", "S024", "S026")
}

object RestOfBody extends LeafSite{
  override val ghm: String = "restOfBody"
  override val codes: List[String] = List("M80.-5", "M80.-8", "M80.-9", "M80.-0")
}

object BodySites extends ForkSite{
  override val ghm: String = "BodySites"
  override val sites: List[BodySite] = List(Ribs, MembreInferieurDistal, MembreSuperieurProximal, MembreSuperieurDistal, CraneFace, RestOfBody, ColDuFemur, FemurExclusionCol, BassinRachis, Clavicule)
  override val codes: List[String] = List.empty
}
