package fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.families

import fr.polytechnique.cmap.cnam.etl.extractors.drugs.classification.{DrugClassConfig, PharmacologicalClassConfig}

object Antiepileptics extends DrugClassConfig {
  override val name: String = "Antiepileptics"
  override val cip13Codes: Set[String] = Set(
    "3400934876691",
    "3400935444271",
    "3400932070619",
    "3400934876233",
    "3400930032671",
    "3400930032619",
    "3400939282114",
    "3400938830460",
    "3400933789862",
    "3400939138473",
    "3400939204741",
    "3400921989861",
    "3400937446693",
    "3400938052183",
    "3400937626217",
    "3400935634429",
    "3400935634658",
    "3400937448246",
    "3400937445283",
    "3400937411394",
    "3400921926767",
    "3400936966864",
    "3400938083989",
    "3400938510690",
    "3400938827330",
    "3400937913386",
    "3400935306326",
    "3400936594555",
    "3400930023808",
    "3400930022863",
    "3400930018163",
    "3400930032749",
    "3400936738577",
    "3400935601650",
    "3400930028995",
    "3400930028971",
    "3400930028902",
    "3400930028254",
    "3400930027981",
    "3400930027691",
    "3400930027547",
    "3400930027363",
    "3400935357182",
    "3400938269246",
    "3400938079449",
    "3400937665766",
    "3400937709682",
    "3400936678392",
    "3400936613119",
    "3400937665827",
    "3400949601455",
    "3400937157452",
    "3400939827650",
    "3400936677852",
    "3400934248061",
    "3400937970808",
    "3400937970747",
    "3400937278898",
    "3400949502769",
    "3400949500239",
    "3400936934696",
    "3400936966574",
    "3400936966116",
    "3400936738867",
    "3400936738638",
    "3400936423831",
    "3400932711949",
    "3400930762974",
    "3400926776756",
    "3400926852610",
    "3400930303030",
    "3400926776527",
    "3400922368993",
    "3400938471229",
    "3400949014095",
    "3400939434698",
    "3400927481970",
    "3400935458179",
    "3400926852788",
    "3400937523158",
    "3400937521666",
    "3400937444972",
    "3400930022818",
    "3400930022771",
    "3400930022733",
    "3400930022689",
    "3400926807894",
    "3400926851897",
    "3400930080061",
    "3400926804305",
    "3400927498541",
    "3400922032337",
    "3400937911436",
    "3400937870290",
    "3400937867979",
    "3400937832960",
    "3400937832502",
    "3400930022290",
    "3400930041352",
    "3400930040300",
    "3400930040294",
    "3400939138015",
    "3400939281742",
    "3400939283524",
    "3400939282572",
    "3400939171302",
    "3400937832212",
    "3400937734066",
    "3400937729505",
    "3400927996979",
    "3400930018224",
    "3400930018200",
    "3400930018187",
    "3400930018125",
    "3400930018118",
    "3400927998232",
    "3400936513532",
    "3400941656958",
    "3400930033111",
    "3400939867571",
    "3400939867403",
    "3400935210531",
    "3400935210302",
    "3400936711143",
    "3400934563096",
    "3400934122699",
    "3400934647406",
    "3400934647864",
    "3400930017852",
    "3400922382142",
    "3400922381602",
    "3400922381480",
    "3400922031217",
    "3400922069128",
    "3400921988741",
    "3400930057131",
    "3400930040546",
    "3400930056882",
    "3400930027851",
    "3400930028940",
    "3400937443043",
    "3400937442442",
    "3400937627276",
    "3400936551152",
    "3400936513303",
    "3400934264559",
    "3400926803933",
    "3400926805364",
    "3400926697471",
    "3400926743697",
    "3400926742638",
    "3400927499432",
    "3400921988222",
    "3400927456497",
    "3400926694630",
    "3400936967007",
    "3400927805455",
    "3400933898434",
    "3400936551381",
    "3400936513242",
    "3400936513181",
    "3400936513013",
    "3400930040249",
    "3400930040218",
    "3400930040171",
    "3400930040133",
    "3400927482571",
    "3400936551732",
    "3400937674751",
    "3400937912846",
    "3400937871761",
    "3400934647635",
    "3400934562907",
    "3400938056327",
    "3400937813921",
    "3400937666428",
    "3400930040096",
    "3400930040584",
    "3400930040492",
    "3400930040461",
    "3400949512294",
    "3400949922574",
    "3400949921744",
    "3400949504831",
    "3400949000388",
    "3400939827599",
    "3400939849133",
    "3400938467086",
    "3400938465075",
    "3400938510461",
    "3400939675763",
    "3400939734996",
    "3400930041604",
    "3400930041550",
    "3400930041499",
    "3400930041420",
    "3400932896271",
    "3400932896103",
    "3400932862092",
    "3400930004036",
    "3400930032879",
    "3400930019290",
    "3400930040188",
    "3400949000371",
    "3400949000326",
    "3400941656729",
    "3400935634139",
    "3400935601360",
    "3400935790125",
    "3400936863057",
    "3400931539513",
    "3400934252723",
    "3400932711710",
    "3400933780418",
    "3400934483561",
    "3400934436376",
    "3400934428272",
    "3400934147364",
    "3400927482113",
    "3400932634507",
    "3400921967289",
    "3400921966688",
    "3400921927719",
    "3400921939255",
    "3400921923346",
    "3400921923285",
    "3400921965919",
    "3400937792486",
    "3400927938849",
    "3400930032824",
    "3400926776237",
    "3400930028919",
    "3400936512931",
    "3400936512870",
    "3400936512702",
    "3400936619203",
    "3400935900258",
    "3400935900487",
    "3400935289773",
    "3400930027356",
    "3400930425367",
    "3400930425138",
    "3400930425077",
    "3400930087190",
    "3400927482052",
    "3400927455667",
    "3400927448393",
    "3400927447914",
    "3400927449574",
    "3400927646706",
    "3400937788236",
    "3400939203799",
    "3400938270426",
    "3400922030616",
    "3400927646874",
    "3400934837371",
    "3400938084931",
    "3400936889422",
    "3400936681293",
    "3400936680463",
    "3400936679054",
    "3400936678682",
    "3400930040416",
    "3400930033234",
    "3400930033074",
    "3400930033005",
    "3400930032664",
    "3400930554968",
    "3400930064672",
    "3400930064665",
    "3400938680706",
    "3400938679755",
    "3400934759109",
    "3400938829921",
    "3400938829631",
    "3400938060348",
    "3400938059397",
    "3400938057386",
    "3400938006810",
    "3400938002157",
    "3400938164985",
    "3400938161793",
    "3400930041369",
    "3400930004227",
    "3400930004104",
    "3400930032602",
    "3400930032534",
    "3400930032381",
    "3400941656378",
    "3400941716904",
    "3400941716782",
    "3400941716614",
    "3400941716492",
    "3400941717093",
    "3400938829570",
    "3400937793209",
    "3400937791137",
    "3400934147135",
    "3400933898663",
    "3400934126482",
    "3400934126253",
    "3400930019320",
    "3400930019306",
    "3400930019283",
    "3400930018248",
    "3400930023853",
    "3400921625929",
    "3400935357472",
    "3400930040140",
    "3400930029039",
    "3400930080078",
    "3400930023846",
    "3400941716324",
    "3400949000180",
    "3400934830747",
    "3400934830686",
    "3400934830518",
    "3400935957030",
    "3400935956897",
    "3400930698259",
    "3400931705246",
    "3400931035459",
    "3400931922766",
    "3400927938610",
    "3400927938559",
    "3400927938498",
    "3400949980246",
    "3400932984312",
    "3400933780647",
    "3400930040386",
    "3400930058794",
    "3400930031896",
    "3400930041475",
    "3400930032916",
    "3400936678514",
    "3400936678224",
    "3400936678163",
    "3400935305145",
    "3400936679863",
    "3400936889880",
    "3400930058886",
    "3400930058824",
    "3400930056899",
    "3400930293003",
    "3400927938320",
    "3400927938269",
    "3400927938030",
    "3400926612085",
    "3400926610364",
    "3400941716843",
    "3400937444453",
    "3400936594265",
    "3400937970518",
    "3400949510405",
    "3400949924585",
    "3400937983464",
    "3400937982863",
    "3400937978781",
    "3400937023818",
    "3400936967816",
    "3400936967175",
    "3400936966925",
    "3400930032343",
    "3400930032244",
    "3400930032190",
    "3400930031971",
    "3400930030035",
    "3400934759277",
    "3400926852559",
    "3400926777128",
    "3400926776985",
    "3400937788984",
    "3400939018218",
    "3400939016955",
    "3400939003597",
    "3400938270884",
    "3400938264913",
    "3400938271256",
    "3400938176292",
    "3400938176124",
    "3400930023839",
    "3400930023815",
    "3400930023792",
    "3400930023785",
    "3400930060391",
    "3400930030028",
    "3400930029077",
    "3400930029015",
    "3400941701474",
    "3400941627224",
    "3400941626104",
    "3400949026043",
    "3400938979305",
    "3400938810998",
    "3400938810301",
    "3400932956395",
    "3400933148744",
    "3400933133085",
    "3400932507955",
    "3400930060346",
    "3400930060308",
    "3400930022849",
    "3400949500062",
    "3400949508914",
    "3400949510863",
    "3400949510634",
    "3400926610883",
    "3400932711888",
    "3400949013906",
    "3400931539681",
    "3400936619142",
    "3400939139135",
    "3400935357533",
    "3400935357304",
    "3400935357243",
    "3400935357014",
    "3400936423541",
    "3400930019276",
    "3400930019269",
    "3400930019252",
    "3400927997570",
    "3400927803215",
    "3400927802492",
    "3400922325941",
    "3400922198149",
    "3400922275123",
    "3400922366005",
    "3400937441902",
    "3400936968417",
    "3400936965973",
    "3400937628228",
    "3400935035172",
    "3400930076439",
    "3400949500147",
    "3400937984355",
    "3400936966345",
    "3400926803124",
    "3400926710002",
    "3400939000695",
    "3400937983815",
    "3400937927864",
    "3400949024780",
    "3400938110357",
    "3400936680982",
    "3400936680753",
    "3400937927116",
    "3400937926515",
    "3400936678804",
    "3400936678453",
    "3400937970976",
    "3400941814648",
    "3400937464963",
    "3400936107274",
    "3400938463125",
    "3400937861366",
    "3400938830750",
    "3400938830002",
    "3400938922004",
    "3400938811421",
    "3400938811070",
    "3400938829860",
    "3400938081749",
    "3400937940931",
    "3400937812160",
    "3400937804646",
    "3400938810479",
    "3400938510812",
    "3400938509922",
    "3400949499595",
    "3400938982084",
    "3400938999983",
    "3400938979244",
    "3400938830811",
    "3400938978292",
    "3400937813570",
    "3400934848896",
    "3400934848728",
    "3400936107564",
    "3400936942790",
    "3400932822690",
    "3400932822522",
    "3400936941502",
    "3400936966055",
    "3400937984874",
    "3400938269994",
    "3400938269475",
    "3400926803063",
    "3400926710750",
    "3400926613266",
    "3400922276014",
    "3400926711931",
    "3400926938529",
    "3400926937348",
    "3400922326603",
    "3400936681415",
    "3400922211947",
    "3400921923407",
    "3400926803292",
    "3400935034113",
    "3400926844745",
    "3400930085202",
    "3400930085189",
    "3400930085233",
    "3400930085219",
    "3400931155195",
    "3400930085158",
    "3400941812408",
    "3400930085141",
    "3400930076385",
    "3400930087268",
    "3400930087251",
    "3400930087213",
    "3400930087206",
    "3400926614218",
    "3400922135854",
    "3400938827569",
    "3400941808845",
    "3400936551442",
    "3400936966284",
    "3400938811360",
    "3400937448826",
    "3400937443791",
    "3400930022634",
    "3400930087183",
    "3400936551213",
    "3400936678743",
    "3400921626001",
    "3400927998980",
    "3400936613577",
    "3400930032954",
    "3400930022627",
    "3400936677913",
    "3400936942103",
    "3400921626179",
    "3400949510573",
    "3400936966406",
    "3400935602251",
    "3400930028117",
    "3400937411455",
    "3400941626913",
    "3400930040119",
    "3400930031902",
    "3400949505081",
    "3400936920330",
    "3400930087244",
    "3400941716553",
    "3400935956958",
    "3400937805995",
    "3400949026623",
    "3400935210760",
    "3400926776008",
    "3400939863900",
    "3400934122460",
    "3400926692919",
    "3400927482342",
    "3400939848532",
    "3400938681826",
    "3400934252662",
    "3400934436895",
    "3400927455087",
    "3400927646645",
    "3400930292921",
    "3400938830170",
    "3400934126024",
    "3400927938788",
    "3400938981483",
    "3400937912266",
    "3400933018023",
    "3400922325422",
    "3400922368016",
    "3400939283005",
    "3400937833271",
    "3400930018149",
    "3400922032108",
    "3400921967920",
    "3400927498190",
    "3400937789875",
    "3400937925853",
    "3400938080339",
    "3400930041406",
    "3400921925647",
    "3400933790172",
    "3400938978704",
    "3400933801441",
    "3400938058109",
    "3400930019313",
    "3400938662207",
    "3400937627795",
    "3400938284614",
    "3400930023778",
    "3400938830231",
    "3400937940290",
    "3400935034571",
    "3400939000527",
    "3400938983494",
    "3400938082869",
    "3400938469097",
    "3400937861595",
    "3400937465106",
    "3400949605767",
    "3400930076507",
    "3400926613785",
    "3400941810565",
    "3400930087220",
    "3400949500819",
    "3400938086072",
    "3400930085196",
    "3400930085172",
    "3400922274461"
  )
  override val pharmacologicalClasses: List[PharmacologicalClassConfig] = List.empty
}
