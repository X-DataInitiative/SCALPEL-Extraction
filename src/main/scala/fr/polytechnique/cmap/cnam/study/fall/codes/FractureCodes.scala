// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.study.fall.codes

/*
 * The codes needed for this study's outcomes are listed in Confluence.
 * Link: https://datainitiative.atlassian.net/wiki/display/CFC/Outcomes
 */


trait FractureCodes {
  val HospitalizedFracturesCim10 = Set(
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

  val NonHospitalizedFracturesCcam = Set(
    "MZMP002",
    "MZMP007",
    "MZMP013",
    "MZMP004",
    "NZMP008",
    "NZMP006",
    "NZMP014",
    "MADP001",
    "HBED009",
    "HBED015",
    "LAEA008",
    "LAEP002",
    "LAEP003",
    "LAEP001",
    "LBED001",
    "LBED004",
    "LBEP009",
    "LAEA007",
    "LAEA001",
    "LAEA003",
    "LAEB001",
    "LBEP002",
    "LBED002",
    "LBED005",
    "LBED006",
    "LBED003",
    "MAEP001",
    "MBEP001",
    "MBEP002",
    "MBEP003",
    "MBEB001,",
    "MCEP002",
    "MCEP001",
    "MGEP002",
    "MDEP002",
    "MDEP001",
    "NAEP002",
    "NAEP001",
    "NBEP002",
    "NBEP001",
    "NBEB001",
    "NCEP002",
    "NCEP001",
    "NDEP001"
  )

  val GenericGHMCodes = Set(
    "08C13",
    "08C14",
    "08M33",
    "08M18"
  )

  val CCAMExceptions = Set(
    "LAGA002",
    "LAGA003",
    "LAGA004",
    "LAGA005",
    "LDGA001",
    "LDGA002",
    "LEGA001",
    "LEGA002",
    "LFGA001",
    "LHGA004",
    "LHGA006",
    "LHGA007",
    "LJGA001",
    "LJGA002",
    "MAGA001",
    "MDGA002",
    "MDGB001",
    "NAGA001",
    "NBGA007",
    "NDGA003",
    "PAGA008",
    "PAGA009",
    "PAGA010",
    "PAGA011",
    "PAGB001",
    "PAGB002",
    "PAGB003",
    "PAGB004",
    "PAGH001",
    "PAKB001",
    "PAMP001"
  )
}

