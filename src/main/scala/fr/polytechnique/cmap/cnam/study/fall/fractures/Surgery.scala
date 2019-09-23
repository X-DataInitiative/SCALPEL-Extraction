package fr.polytechnique.cmap.cnam.study.fall.fractures

trait Surgery {
  val codes = Set(
    "QAGA004",
    "QZGA003",
    "EEGA002",
    "EEGA001",
    "BKGA003",
    "BKGA005",
    "GAGD002",
    "MJGA001",
    "BBGA001",
    "PAGA002",
    "LAGA007",
    "BGGA003",
    "LBGD001",
    "QAGA001",
    "QZGA006",
    "QAGA002",
    "QZGA007",
    "LAGA001",
    "DGGA003",
    "BGGA002",
    "GGNC001",
    "GGNA001",
    "MEMA017",
    "MEMC005",
    "MEMA006",
    "MEMC003",
    "NJAA003",
    "NJAA001",
    "MJAA001",
    "MJAA002",
    "NJAB001",
    "NJAA004",
    "NJAA002",
    "MCAA001",
    "NDAA001",
    "MDAA001",
    "LBAA001",
    "PDAB001",
    "MZFA001",
    "MZFA003",
    "MZFA007",
    "NZFA009",
    "NZFA005",
    "MZFA013",
    "NZFA010",
    "NZFA004",
    "NZFA013",
    "NZFA007",
    "MZFA002",
    "MZFA005",
    "NZFA002",
    "EZQH004",
    "NJPA015",
    "NJPA006",
    "PCPA002",
    "NJPA014",
    "NJPA007",
    "LABA004",
    "LABA001",
    "LABA003",
    "LHFA028",
    "LHFA001",
    "LHFA029",
    "LHFA025",
    "LHFA027",
    "LHFA013",
    "LDDA001",
    "MHDA004",
    "NHDA008",
    "NHDA006",
    "LFDA010",
    "LFDA008",
    "LFDA013",
    "MHDA005",
    "NHDA007",
    "NHDA002",
    "NHDA001",
    "NHDA004",
    "MHDA001",
    "NHDA005",
    "MFDA001",
    "NHDA003",
    "NHDA010",
    "NHDA009",
    "NFDA002",
    "NFDA003",
    "LFDA002",
    "LFDA009",
    "LHDA002",
    "LFDA004",
    "LFDA012",
    "MHDA002",
    "MGDA001",
    "LFDA006",
    "LFDA001",
    "LFDA003",
    "LFDA007",
    "LFDA005",
    "LHDA001",
    "MGDA002",
    "NEDA003",
    "MEDA001",
    "NHDA011",
    "NGDA001",
    "NGDC001",
    "NGDA002",
    "MHDA003",
    "NGDA003",
    "MEMC001",
    "MEMA011",
    "NEMA018",
    "MHMA002",
    "MHMA001",
    "MFMA005",
    "MGMA003",
    "NHMA007",
    "NHMA001",
    "NHMA003",
    "NHMA006",
    "NHMA002",
    "MEMA015",
    "MEMA001",
    "MHMA005",
    "MFDA002",
    "MHDB001",
    "NGDA004",
    "MDEA001",
    "MDEA002",
    "MDEA003",
    "QZEA008",
    "ADEA002",
    "ADEA001",
    "AHEA004",
    "AHEA008",
    "AHEA014",
    "AHEA006",
    "AHEA009",
    "AHEA016",
    "QZPA008",
    "NJMA003",
    "GFFA018",
    "LAHA002",
    "MDHA001",
    "NZHA001",
    "MZHA001",
    "NEHA001",
    "LHHA006",
    "NEHA002",
    "NAHA002",
    "NAHA001",
    "LAHA001",
    "QEHA002",
    "LJHA001",
    "LDHA002",
    "LEHA002",
    "LFHA001",
    "LEHC001",
    "LBDD001",
    "NFMA005",
    "MEMA008",
    "MEMA016",
    "MEMA014",
    "MEMA012",
    "MEMC002",
    "MGMA004",
    "BFPP001",
    "PAKB002",
    "ENFA005",
    "PAMH001",
    "LAMA012",
    "LBBA006",
    "LBBA004",
    "MADP001",
    "HBDD008",
    "HBDD007",
    "HBDD014",
    "EQBP001",
    "LFFA008",
    "LEFA008",
    "LEFA014",
    "LDFA009",
    "LFFA009",
    "LEFA007",
    "LEFA010",
    "LDFA012",
    "LFFA013",
    "LEFA004",
    "LEFA006",
    "LDPA007",
    "LDPA006",
    "LHMA015",
    "LHMA006",
    "LFMA001",
    "LEMA002",
    "LHMA013",
    "LHMA003",
    "LHMA004",
    "LEMA004",
    "LHMA011",
    "NJMA007",
    "NJMA005",
    "QZMP001",
    "LAFA900",
    "LAMA009",
    "MGMA005",
    "GGJA003",
    "LAPA012",
    "LAPA003",
    "EGPA001",
    "ADPA011",
    "ADPA008",
    "ADPA021",
    "ADPA016",
    "ADPA020",
    "ADPA001",
    "CCPA001",
    "LDPA001",
    "LDPA003",
    "MCPA013",
    "MCPA014",
    "MBPA001",
    "MBPA006",
    "NCPA008",
    "NCPA010",
    "MCPA011",
    "MCPA012",
    "NBPA005",
    "NBPA016",
    "GGPA001",
    "AHPA026",
    "AFCA004",
    "ABCB001",
    "AFCA002",
    "ABCA003",
    "ABCA004",
    "ABCA002",
    "NZFA001",
    "MZFA011",
    "NZFA003",
    "MZFA004",
    "NZFA006",
    "NZFA008",
    "MZFA010",
    "NBGA015",
    "MDGA005",
    "MJPA004",
    "MJPA006",
    "MJPA001",
    "PCPA003",
    "MJPA012",
    "DCJA001",
    "DCJB001",
    "BHGA003",
    "BHGA001",
    "AAJA003",
    "AAJA004",
    "AAJH004",
    "AAJA002",
    "AFJA003",
    "AEJA003",
    "JLJA001",
    "AFJA004",
    "AFJA001",
    "ABJA008",
    "GGJB002",
    "HPJB001",
    "ABJA004",
    "ABJA007",
    "ABJA005",
    "QZJB001",
    "ABJA002",
    "ABJA006",
    "ABJA003",
    "GGJC002",
    "QAJA003",
    "MJJA002",
    "MJJA004",
    "MJJA001",
    "MHJA001",
    "NZJB001",
    "MZJB001",
    "NEJA002",
    "NEJB001",
    "NFJC002",
    "NFJA002",
    "NGJC002",
    "NGJA002",
    "BKJA001",
    "GGJA004",
    "GGJA002",
    "BEJB002",
    "LCJA004",
    "LCJA002",
    "GHJA001",
    "GHJA002",
    "AAJA001",
    "AAJH001",
    "AAJA005",
    "NEJA003",
    "JLJA002",
    "LCJA003",
    "NEJA004",
    "QZJA011",
    "GGJC001",
    "GGJA001",
    "QZJA009",
    "QZJB002",
    "LJJA002",
    "LJJA001",
    "AFJB002",
    "MJJA003",
    "MDGA004",
    "MDGA003",
    "MDGA001",
    "MZGA004",
    "MZGA003",
    "NDGA002",
    "NDGA001",
    "PAGA003",
    "PAGA006",
    "PAGA005",
    "MBGA001",
    "MBGA002",
    "NBGA001",
    "NBGA003",
    "NAGA002",
    "NAGA003",
    "LBGA005",
    "NBGA006",
    "NBGA002",
    "NCGA002",
    "NCGA001",
    "BHGA006",
    "BHGA002",
    "MJFA003",
    "MJFA009",
    "QZFA023",
    "QZFA038",
    "QZFA027",
    "NDFA007",
    "LFFA007",
    "LFFA004",
    "QZFA032",
    "QZFA009",
    "LBFA023",
    "CAFA009",
    "GFFA019",
    "LGFA001",
    "LGFA005",
    "NDFA002",
    "MDFA002",
    "MZFA006",
    "LJFA010",
    "QZFA039",
    "NAFA002",
    "MAFA006",
    "LLFA013",
    "LLFA003",
    "NBFA001",
    "NCFA006",
    "NCFA002",
    "GFFC002",
    "GFFA017",
    "QZFA020",
    "QZFA029",
    "LHFA031",
    "GFFA021",
    "EZQA001",
    "NEQC001",
    "MEQC002",
    "NGQC001",
    "MFQC001",
    "NFQC001",
    "MGQC001",
    "MEQC001",
    "MJFA010",
    "MJFA006",
    "PCPA004",
    "PCPB001",
    "MJPA005",
    "MJPB001",
    "ABSA004",
    "ABSA006",
    "ABSA002",
    "ABSA001",
    "ABSA003",
    "ABSA011",
    "ABSA005",
    "ABSA007",
    "ABSA012",
    "HASA018",
    "NDDC001",
    "NDDA001",
    "NFDC001",
    "NFDA009",
    "PADA003",
    "LGDA001",
    "NCEA001",
    "MEFA004",
    "NJPA016",
    "NJPA032",
    "LMBA001",
    "LLBA002",
    "GGBA001",
    "PCEA004",
    "LDFA003",
    "LDFA004",
    "LDFA005",
    "LFFA001",
    "LFFA005",
    "LFFA006",
    "LHFA019",
    "LHFA024",
    "LHFA016",
    "LHMA016",
    "LHMA007",
    "LHPA006",
    "LHPA010",
    "LHPA003",
    "AHPA001",
    "AHPA028",
    "AHPA024",
    "MJPA011",
    "MJPA002",
    "NJPA002",
    "NJPA019",
    "NJPA009",
    "MJPA003",
    "MJPA008",
    "MJPA007",
    "AHPA016",
    "AHPA017",
    "AHPA010",
    "AHPA009",
    "AHPC001",
    "AHPA013",
    "AHPA027",
    "AHPA023",
    "AHPA012",
    "AHPA008",
    "AHPA022",
    "AHPA021",
    "AHPA005",
    "AHPA004",
    "AHPA020",
    "AHPA006",
    "AHPA002",
    "GGPA002",
    "MJPA009",
    "MHPA004",
    "MHPA002",
    "NHPA001",
    "NEPA001",
    "MFPA003",
    "MFPA001",
    "MFPA002",
    "MFPC001",
    "NFPA003",
    "NFPC002",
    "NFPA001",
    "NHPA006",
    "NHPA002",
    "NHPA003",
    "MEPC001",
    "MEPA001",
    "NGPA003",
    "NGPA001",
    "NGPC001",
    "NGPA002",
    "MHPA003",
    "MHPA001",
    "MGPA001",
    "NHPA005",
    "NHPA004",
    "PCPA001",
    "AHPA011",
    "NJPA018",
    "EESA001",
    "ECSA003",
    "EJSA003",
    "DHSA002",
    "EJSA001",
    "EFSA001",
    "EDSA003",
    "EBSA010",
    "EBSA008",
    "GFFA022",
    "GFFA004",
    "GFFA013",
    "GFFA009",
    "NFFC004",
    "NFFA003",
    "NFFC003",
    "NFFA001",
    "LHPA004",
    "FCPA001",
    "LAPA001",
    "AFPA001",
    "LAPA015",
    "NHRP002",
    "MGRP001",
    "NGRP001",
    "MFRP001",
    "NFRP001",
    "MERP001",
    "LARA004",
    "HDPA001",
    "NEJC001",
    "NEJA001",
    "MFJC001",
    "MFJA001",
    "NFJC001",
    "NFJA001",
    "MEJC001",
    "MEJA001",
    "NGJC001",
    "NGJA001",
    "MGJC001",
    "MGJA001",
    "EASF012",
    "LJCA002",
    "LACA012",
    "MBCB003",
    "MBCA010",
    "MBCB004",
    "MBCB001",
    "MBCA003",
    "MBCA006",
    "MBCA008",
    "MDCA005",
    "LBCA010",
    "NCCA018",
    "MDCB005",
    "MDCA003",
    "MDCA014",
    "NCCA007",
    "LACA014",
    "LACA015",
    "NCCA017",
    "NCCA016",
    "MBCA005",
    "LBCA008",
    "MCCA011",
    "NBCA001",
    "NDCA004",
    "NCCA013",
    "NCCA004",
    "NBCA003",
    "LACA020",
    "LACA016",
    "LACA017",
    "NBCA013",
    "NDCB003",
    "NDCA002",
    "MDCA013",
    "NACA003",
    "NACA005",
    "NCCC001",
    "NCCA003",
    "MCCA005",
    "MCCB002",
    "NCCA012",
    "MCCA010",
    "MCCB001",
    "MCCB005",
    "MCCA009",
    "MDCA001",
    "LACA003",
    "LACB002",
    "LACA002",
    "MDCA012",
    "MACB001",
    "MACA004",
    "MACA001",
    "MCCB008",
    "MCCA003",
    "MCCB003",
    "MCCA004",
    "MBCA007",
    "MBCB002",
    "MBCA011",
    "NCCA014",
    "MCCA007",
    "MCCB007",
    "MCCA008",
    "NBCB001",
    "NBCB004",
    "NBCB002",
    "NBCA007",
    "NCCA010",
    "NCCB006",
    "NCCB004",
    "NCCA002",
    "MACB002",
    "MACA003",
    "NACA004",
    "NDCA006",
    "LACA001",
    "NDCB004",
    "MACA002",
    "LBCA007",
    "LBCA004",
    "NDCA005",
    "NBCA008",
    "LGCA001",
    "LJCA001",
    "NDCB001",
    "MBCA004",
    "MDCB003",
    "MDCA010",
    "MDCA011",
    "NCCA006",
    "MBCA001",
    "NBCA010",
    "LBCA001",
    "LBCB001",
    "LBCA002",
    "LBCB002",
    "LBCA006",
    "LACA018",
    "LACA009",
    "LACA011",
    "LACA013",
    "LACA006",
    "LACA010",
    "NBCA006",
    "MBCA012",
    "NBCA005",
    "LACB001",
    "LACA005",
    "MCCB004",
    "NCCB005",
    "NBCB006",
    "NCCB001",
    "NCCA001",
    "NCCB007",
    "NCCB002",
    "NCCA005",
    "NCCA019",
    "NCCA011",
    "NACB001",
    "NBCA012",
    "NBCA002",
    "NCCA008",
    "MCCA001",
    "NDCA001",
    "NBCA015",
    "NBCA014",
    "NCCA015",
    "LACA004",
    "LACA019",
    "MBCA009",
    "MDCB002",
    "MDCA008",
    "MDCA004",
    "NDCB002",
    "NDCA003",
    "MDCA006",
    "MDCB004",
    "MDCA009",
    "MDCA007",
    "NBCA009",
    "LACA008",
    "LACA007",
    "LDCA012",
    "LDCA013",
    "LFCA003",
    "LFCA004",
    "LECA001",
    "LECA004",
    "LECA005",
    "LECA002",
    "LDCA007",
    "LHCA011",
    "LHCA001",
    "LDCA008",
    "LDCA004",
    "LFCA001",
    "LFCA002",
    "NBCA004",
    "LDCA011",
    "LFCA005",
    "LECA006",
    "LFCC001",
    "LECA003",
    "LECC001",
    "LDCA002",
    "LBCA003",
    "LBCA009",
    "LBCA005",
    "LDCA005",
    "LDCA010",
    "NACA002",
    "LHCA010",
    "LHCA016",
    "LHCA002",
    "LDCA003",
    "MDCB001",
    "MDCA002",
    "MCCB009",
    "MCCA002",
    "NCCB003",
    "NCCA009",
    "MBCB005",
    "MBCA002",
    "MCCB006",
    "MCCA006",
    "NBCB005",
    "NBCA011",
    "LDCA001",
    "LDCA006",
    "LDCA009",
    "NACA001",
    "LEPA009",
    "LFPA003",
    "LDPA009",
    "LDPA008",
    "LFPA001",
    "LEPA008",
    "LEPA003",
    "LFPA002",
    "NBPA010",
    "NCPA014",
    "MCPA006",
    "MCPA007",
    "NBPA011",
    "MCPA004",
    "NCPA016",
    "MBPA003",
    "LAPA013",
    "NDPA004",
    "NDPA014",
    "MDPA002",
    "MDPA004",
    "MAPA003",
    "MCPA003",
    "MCPA002",
    "MCPA009",
    "NDPA007",
    "NDPA012",
    "NDPA005",
    "LAPA002",
    "MCPA005",
    "MAPA002",
    "NCPA007",
    "NDPA009",
    "NCPA002",
    "NCPA003",
    "NCPA001",
    "MDPA003",
    "MDPA001",
    "MCPA001",
    "NDPA006",
    "NDPA003",
    "LAPA004",
    "NBPA014",
    "NDPA011",
    "NDPA002",
    "NDPA013",
    "MDPA005",
    "NDPA001",
    "NDPA008",
    "NAPA007",
    "MBPA005",
    "NBPA020",
    "MBPA002",
    "NBPA019",
    "MAPA001",
    "LBPA028",
    "LBPA012",
    "LBPA022",
    "LBPA015",
    "LBPA006",
    "LBPA038",
    "LBPA010",
    "LBPA002",
    "NCPA012",
    "NCPA015",
    "MCPA010",
    "MBPA004",
    "NBPA003",
    "NBPA004",
    "NCPA013",
    "NCPA009",
    "NCPA006",
    "NDPA010",
    "PAPA003",
    "QZJA023",
    "AFJA005",
    "AFJA002",
    "AAJA006",
    "AEJA002",
    "AEJA005",
    "AEJA004",
    "HAJA010",
    "QZJA022",
    "HAJA003",
    "CAJA002",
    "HAJA009",
    "GAJA002",
    "HAJA008",
    "QZJA012",
    "QAJA006",
    "QAJA004",
    "QAJA012",
    "QCJA001",
    "QZJA016",
    "QZJA001",
    "QZJA017",
    "QAJA005",
    "QAJA013",
    "QAJA002",
    "QZJA002",
    "QZJA015",
    "HAJA006",
    "HAJA007",
    "QZJA021",
    "QAJA009",
    "QZJA013",
    "NBFA005",
    "NBFA009",
    "NBMA002",
    "JFFA016",
    "HAFA024",
    "PACB001",
    "PACC001",
    "PACA001",
    "NBCB003",
    "DCFA001",
    "LAFA005",
    "PCMA001",
    "LLMA008",
    "LLMC003",
    "LLMA003",
    "LJMA002",
    "NFMA002",
    "HBMA001",
    "LLMA004",
    "GGCA001",
    "GFFA025",
    "GFFA024",
    "GGHB001",
    "DGCA007",
    "DGCA012",
    "DGCA009",
    "DGCA005",
    "EDCA003",
    "EECA002",
    "EECA003",
    "EECA001",
    "EECA010",
    "EECA008",
    "EDCA005",
    "EDCA004",
    "EZCA005",
    "EECA007",
    "EZCA003",
    "DGCA020",
    "DGCA004",
    "DGCA022",
    "ECCA002",
    "ECCA003",
    "EECA005",
    "ECCA007",
    "EECA012",
    "EZCA004",
    "EPCA002",
    "DGCA014",
    "EJCA002",
    "EZCA001",
    "DGCA011",
    "CBLD001",
    "DGLF002",
    "DGLF001",
    "DGLF003",
    "EDLF004",
    "EDLF005",
    "EDLF013",
    "ECLF004",
    "EDLF006",
    "EELF002",
    "ECLF003",
    "EFLF001",
    "DGLF005",
    "LBLD002",
    "LBLD016",
    "GGLB006",
    "LALA002",
    "LBLD024",
    "DBLA004",
    "DBLF001",
    "GGJB005",
    "PAFA009",
    "PAFA003",
    "PAFA004",
    "PAFA010",
    "LAFA008",
    "AHFA009",
    "EPFA006",
    "NJBA002",
    "NJBA001",
    "BABA001",
    "LDAA001",
    "LFAA002",
    "LDAA002",
    "LFAA001",
    "MZMA003",
    "MZMA002",
    "MDMA001",
    "EAMA001",
    "MJMA013",
    "MJMA009",
    "MHMA003",
    "MHMA004",
    "MCMA001",
    "MCMA002",
    "MBMA002",
    "MBMA001",
    "NHMA008",
    "NJMA001",
    "NJMA002",
    "MEMA009",
    "NGMA001",
    "MFMA003",
    "MGMA006",
    "NEMA019",
    "LHMA008",
    "NAMA002",
    "MJMA012",
    "NFMC001",
    "NFMA011",
    "NBMA001",
    "NBMA003",
    "NFMC003",
    "NFMA004",
    "NFMC002",
    "NFMA010",
    "CAMA016",
    "GAMA021",
    "NJMA004",
    "NCMA001",
    "NCMA002",
    "NFMC005",
    "NFMA008",
    "NEMA013",
    "MFMA001",
    "NFMA013",
    "NDMA001",
    "MGMA002",
    "BKMA003",
    "BKMA001",
    "MEEA004",
    "MHEA003",
    "NGEA001",
    "NEEA002",
    "MFEA003",
    "MFEA001",
    "MGEA001",
    "MGEA002",
    "MHEA004",
    "MHEA002",
    "MEEA002",
    "MEEA003",
    "LBEA001",
    "NEEA003",
    "NEEA001",
    "HBED010",
    "HBED020",
    "HBED019",
    "LAEA007",
    "LAEA008",
    "LAEA003",
    "LAEA001",
    "LAEB001",
    "HBED011",
    "MHEA001",
    "HBED016",
    "HPBA001",
    "EZBA001",
    "EZBA002",
    "JHEP001",
    "PZMA002",
    "PZMA003",
    "PZMA001",
    "LAMA005",
    "LAMA003",
    "LAMA008",
    "QZMA010",
    "LAMA007",
    "MZEA010",
    "MZEA001",
    "MZEA012",
    "MZEA003",
    "MZEA002",
    "MZEA011",
    "NZEA002",
    "MZEA007",
    "NZEA007",
    "NDEA002",
    "MJEA019",
    "NJEA003",
    "MJEA006",
    "MJEC002",
    "NJEA007",
    "NJEA002",
    "NFEC002",
    "NFEA002",
    "MJEA010",
    "MJEC001",
    "NFEC001",
    "NFEA001",
    "BJEA002",
    "MJEA004",
    "PCEA002",
    "QZEA034",
    "QZEA009",
    "LAMA006",
    "LAMA010",
    "LAMA004",
    "LDKA900",
    "LFKA001",
    "EDKA003",
    "DGKA015",
    "DGKA011",
    "DGKA001",
    "DGKA003",
    "DGKA025",
    "DGKA009",
    "EBKA001",
    "EDKA002",
    "NEKA018",
    "NEKA011",
    "NEKA020",
    "NEKA017",
    "NEKA021",
    "NEKA016",
    "NEKA012",
    "NEKA014",
    "NEKA010",
    "MFKA003",
    "NFKA009",
    "NFKA007",
    "NFKA008",
    "NFKA006",
    "MGKA003",
    "NHKA001",
    "MGKA002",
    "MEKA005",
    "MEKA010",
    "MEKA009",
    "MEKA006",
    "MEKA007",
    "MEKA008",
    "NGKA001",
    "DGKA002",
    "MCKA002",
    "EEKA001",
    "BJMA002",
    "BJMA005",
    "MZMA001",
    "NJMB001",
    "GDMA003",
    "MJMA003",
    "BAMA004",
    "QAMA005",
    "QAMA002",
    "QAMA013",
    "QAMA008",
    "BAMA010",
    "BAMA013",
    "HAMA002",
    "HAMA027",
    "HAMA005",
    "HAMA023",
    "HAMA029",
    "QAMA015",
    "GAMA018",
    "GAMA012",
    "HAMA003",
    "AGMA001",
    "QZMA003",
    "QZMA009",
    "QZMA004",
    "QZMA005",
    "QZMA001",
    "QZMA007",
    "CAMA022",
    "LMMA005",
    "EAMA002",
    "MJMA016",
    "MJMA002",
    "MJMA015",
    "MJMA010",
    "MJCA012",
    "BHMA001",
    "BHMA002",
    "MEMC004",
    "PZMA004",
    "PZMA005",
    "ACQP002",
    "GAEA001",
    "DEEF001",
    "CAEA002",
    "BAEA002",
    "BAEA001",
    "NZEA001",
    "MDFA005",
    "NDFA006",
    "MCFA006",
    "MBFA001",
    "NCFA009",
    "NBFA007",
    "NCFA008",
    "NGFA001",
    "NCFA001",
    "NAFA001",
    "MFFA001",
    "HLFA019",
    "BAFA008",
    "HJFA008",
    "BAFA015",
    "NJFA009",
    "NJFA001",
    "DAFA006",
    "MAFA004",
    "LJFA006",
    "MCFA005",
    "MBFA002",
    "HAFA028",
    "NEFA001",
    "MCFA003",
    "LBFA004",
    "LJFA008",
    "LJFA004",
    "LJFA002",
    "MDFA004",
    "MCFA004",
    "NBFA008",
    "NBFA004",
    "EGFA009",
    "HMFA010",
    "MAFA002",
    "AAFA008",
    "AAFA006",
    "NDFA004",
    "MCFA001",
    "MJFA011",
    "NJFA003",
    "MEFC001",
    "MEFA001",
    "LJFA009",
    "LAFA006",
    "LBFA020",
    "LBFA018",
    "EFFA001",
    "MDFA003",
    "NBFA003",
    "NCFA005",
    "NAFA004",
    "LBFA003",
    "MGFA006",
    "MCFA002",
    "PAFA005",
    "MDFA001",
    "MAFA005",
    "MDFA006",
    "GBFA004",
    "ECFA005",
    "EDFA010",
    "EEFA006",
    "DGFA015",
    "QZMA002",
    "JNMD002",
    "ABMA002",
    "ABMA003",
    "MAFA001",
    "MAFA003",
    "AHPA003",
    "AHPA018",
    "NFPA004",
    "NFPC001",
    "NFPA002",
    "NJPA025",
    "NJPA029",
    "NJPA035",
    "NJPA034",
    "MJPA013",
    "PCPA006",
    "NJPA022",
    "NJPA030",
    "NJPA017",
    "NJPA005",
    "AHPA019",
    "GAMA007",
    "LEFA001",
    "LDFA010",
    "LFFA012",
    "LEFA009",
    "LEFA005",
    "LHMH800",
    "LHMH801",
    "LHMH006",
    "LHMH003",
    "LHMH004",
    "LHMH001",
    "LHMH802",
    "LHMH002",
    "LHMH005",
    "EPCA003",
    "NJCA001",
    "BACA005",
    "BACA002",
    "BDCA003",
    "BDCA004",
    "BDCA001",
    "DGCA006",
    "DGCA002",
    "JJCA002",
    "EDCA001",
    "EECA009",
    "DFCA001",
    "AHCA023",
    "MJCA001",
    "ECCA004",
    "ECCA005",
    "HKCA003",
    "HKCA004",
    "DGCA001",
    "EBCA009",
    "EDCA002",
    "ECCA010",
    "JECA002",
    "BCCA001",
    "BGCA002",
    "DHCA003",
    "DHCA001",
    "EHCA008",
    "JMCA006",
    "AHCA003",
    "AHCA016",
    "MZMA004",
    "EJCA001",
    "EFCA001",
    "EFCA002",
    "BJCA001",
    "DACA001",
    "GDCA001",
    "AHCA019",
    "ADCA002",
    "AHCA004",
    "AHCA021",
    "AHCA012",
    "AHCA018",
    "AHCA005",
    "AHCA009",
    "AHCA010",
    "AHCA011",
    "HDCA002",
    "HJCD001",
    "HJCD002",
    "BACA008",
    "EGCA002",
    "JLCA008",
    "HFCC001",
    "HFCA003",
    "HGCA002",
    "HECA001",
    "HECA004",
    "HECA002",
    "HHCC001",
    "HHCA001",
    "HJCC001",
    "HJCA001",
    "GECA001",
    "JCCA003",
    "GECA003",
    "PCCA002",
    "JDCC016",
    "JDCA003",
    "LLCC001",
    "LLCA003",
    "LLCC003",
    "LLCA005",
    "GFCC001",
    "GFCA001",
    "BACA001",
    "BACA006",
    "BACA007",
    "AHCA015",
    "MJCA006",
    "MJCA005",
    "ECCA001",
    "AHCA002",
    "AHCA013",
    "MJCA003",
    "MJCA010",
    "MJCA008",
    "MJCA007",
    "MJCA002",
    "AHCA008",
    "AHCA017",
    "AHCA006",
    "BDCA002",
    "PCCA001",
    "JHCA004",
    "JHCA006",
    "MGCC001",
    "MGCA001",
    "JMCA002",
    "DGCA025",
    "MFCA001",
    "MHCA003",
    "MHCA002",
    "MHCA001",
    "NGCA001",
    "NFCA002",
    "NFCA003",
    "NFCC002",
    "NFCA001",
    "NFCA004",
    "NFCC001",
    "NFCA006",
    "NFCA005",
    "BACA003",
    "AHCA022",
    "NFFC002",
    "NFFA004",
    "NHFA001",
    "NEFC001",
    "NEFA004",
    "MHFA001",
    "MHFA003",
    "NFFC001",
    "NFFA002",
    "MGFC001",
    "MGFA002",
    "MGFC002",
    "MGFA005",
    "MEFC002",
    "MEFA003",
    "MGFC003",
    "MGFA003",
    "MFFC001",
    "MFFA002",
    "LAPA008",
    "LAPA016",
    "LAPA005",
    "NDFA001",
    "NDFA003",
    "EHBD001",
    "GABD002",
    "GABD001",
    "NDFA010",
    "NDFA008",
    "MJDA001",
    "MJDC001",
    "PCDA001",
    "NJFA005",
    "MJFA016",
    "MJFA002",
    "MJFA014",
    "MJFA004",
    "MJFA018",
    "MJFA012",
    "MJFA015",
    "PCPA005",
    "LJMA003",
    "EDFA002",
    "EEFA004",
    "EEFA002",
    "ECFA002",
    "DGFA004",
    "EHFA001",
    "ENFA004",
    "ENFA001",
    "ENFA006",
    "EGFA004",
    "EEJF001",
    "EZJF002",
    "EFJF001",
    "DGFA003",
    "EBFA003",
    "EBFA009",
    "EEFA001",
    "EEFA003",
    "EDFA001",
    "EBFA002",
    "EBFA016",
    "EBFA015",
    "EBFA008",
    "EBFA006",
    "EBFA012",
    "DFFA003",
    "EDFA007",
    "DFNF002",
    "KCFA005",
    "NJEA012",
    "NJEA001",
    "NJEA004",
    "QZEA028",
    "MJEA005",
    "MJEA008",
    "MJEA002",
    "MJEA001",
    "MJEA007",
    "MJEA018",
    "PCEA003",
    "MJEA021",
    "MJEA012",
    "MJEA003",
    "MJEA013",
    "MJEA011",
    "MJEA017",
    "NJEA006",
    "NJEA009",
    "NJEA011",
    "NJEA008",
    "NJEA010",
    "JCMA003",
    "MZEA005",
    "MJEA016",
    "LDFA002"
  )
}