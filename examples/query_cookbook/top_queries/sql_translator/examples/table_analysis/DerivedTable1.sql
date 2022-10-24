CREATE OR REPLACE TABLE testdataset.DerivedTable1 AS
WITH
  Table1 AS (
    SELECT
      IF(
        ENDS_WITH(SourceD.FieldE,'Country-iUpR'),
        IF('CCY-6P1E' <> SourceC.FieldI,'Fee-YKlb','Fee-FxYj'),
        IF(
          ENDS_WITH(SourceC.FieldA,'Customer-4RDv'), 'Address-oLRv',
          SourceE.FieldF))
        AS CCYQDlu,
      CASE
        WHEN ENDS_WITH(SourceA.FieldC,'Address-10Ve') THEN
          IF(
            STARTS_WITH(SourceC.FieldG,SourceC.FieldJ), 'Account-7YZF',
            'Country-G28J')
        WHEN SourceE.FieldF LIKE 'Amt-CIWw' THEN
          IF(SourceC.FieldD NOT LIKE 'CCY-7ZwN',SourceD.FieldD,'Code-0Sxc')
        ELSE
          IF(
            ENDS_WITH('Customer-6j8j','Account-9oPD'), SourceE.FieldC,
            'Fee-xenZ')
      END
        AS Customer2RaL,
      IF(
        SourceD.FieldF <> 'Fee-3hOP',
        IF(STARTS_WITH('Code-qDb1','Country-JvYq'),'Fee-QcnB',SourceD.FieldJ),
        IF(STARTS_WITH('Amt-ueiX','Country-ymsz'),'CCY-JNFA',SourceD.FieldF))
        AS Amt3Ib2,
      IF(
        ENDS_WITH(SourceD.FieldH,SourceE.FieldA),
        CASE
          WHEN SourceC.FieldB != 'Code-9VHl' THEN
            'Code-segC'
          WHEN SourceE.FieldG NOT LIKE 'CCY-ACdd' THEN
            SourceC.FieldH
          WHEN ENDS_WITH(SourceC.FieldJ,'CCY-mZAl') THEN
            'CCY-DyaJ'
          ELSE
            SourceE.FieldG
        END,
        IF(ENDS_WITH('Customer-oxRb',SourceA.FieldA),SourceE.FieldG,'Amt-MAhd'))
        AS Account4mSm,
      IF(
        'Country-60xE' != SourceC.FieldG,
        CASE
          WHEN SourceD.FieldJ != SourceC.FieldD THEN
            SourceD.FieldJ
          WHEN STARTS_WITH(SourceA.FieldD,'Account-VOad') THEN
            SourceD.FieldJ
          WHEN ENDS_WITH('Customer-20e4',SourceE.FieldH) THEN
            'CCY-O2LC'
          WHEN 'Account-VdiW' = SourceE.FieldB THEN
            'Address-LzAz'
          ELSE
            SourceA.FieldF
        END, IF(
               STARTS_WITH(SourceC.FieldB,SourceA.FieldB), SourceC.FieldJ,
               SourceC.FieldH))
        AS CCYDFbw,
      IF(
        STARTS_WITH(SourceA.FieldG,'CCY-PAn7'),
        IF(
          ENDS_WITH('Customer-T27z',SourceA.FieldI), 'Account-sqim',
          SourceA.FieldF), IF(
                             ENDS_WITH(SourceD.FieldI,SourceD.FieldF),
                             SourceD.FieldJ, SourceD.FieldF))
        AS CCYjmUx,
      IF(
        'Amt-ditN' <> SourceE.FieldF,
        IF(ENDS_WITH(SourceE.FieldB,'Code-9IHD'),SourceE.FieldA,'Amt-vNwi'),
        IF(SourceC.FieldF <> SourceA.FieldF,SourceE.FieldJ,'CCY-MNmE'))
        AS CountryQVL3,
      IF(
        'Customer-1YXs' LIKE SourceA.FieldI,
        IF(ENDS_WITH('Amt-4IrC','Fee-MC3W'),'Account-5qGu','Code-mcKl'),
        IF(ENDS_WITH('Address-OwTm','Amt-5O8y'),SourceE.FieldC,'Address-CHcK'))
        AS FeeNRHE,
      IF(
        SourceE.FieldA != 'CCY-hT1G',
        IF(ENDS_WITH('Code-EcrO','Address-0a7D'),'CCY-yMH2','Address-5Xo1'),
        IF(STARTS_WITH('Customer-fwMi','CCY-064W'),SourceA.FieldA,'Code-ByKb'))
        AS Feex0t9,
      IF(
        'Customer-an4x' = 'Account-R5U9',
        CASE
          WHEN STARTS_WITH(SourceE.FieldF,SourceC.FieldD) THEN
            SourceA.FieldC
          WHEN STARTS_WITH(SourceA.FieldC,'Code-a0ok') THEN
            SourceC.FieldI
          WHEN ENDS_WITH('Country-LeYf','Country-jQiU') THEN
            SourceA.FieldC
          WHEN SourceE.FieldJ <> SourceC.FieldF THEN
            SourceA.FieldB
          WHEN ENDS_WITH(SourceA.FieldH,'Account-ujak') THEN
            SourceA.FieldD
          ELSE
            SourceE.FieldE
        END, IF('Address-l6YV' <> 'Country-Uxtd','Address-THQH','Country-vlpa'))
        AS Customer3oUD,
      IF(
        ENDS_WITH(SourceA.FieldE,'Amt-t8Qv'),
        IF('Address-oxYA' LIKE SourceC.FieldA,'Country-gor9',SourceE.FieldD),
        IF('Account-IYuf' LIKE SourceA.FieldI,'Customer-65Ix','Customer-Xn1n'))
        AS Code6Ols,
      CASE
        WHEN SourceA.FieldE != SourceE.FieldI THEN
          CASE
            WHEN ENDS_WITH(SourceA.FieldE,'Fee-yKHc') THEN
              'Address-F1na'
            WHEN 'CCY-cehF' NOT LIKE SourceD.FieldC THEN
              'Customer-dtv5'
            WHEN SourceA.FieldA LIKE 'CCY-YJhc' THEN
              SourceC.FieldE
            ELSE
              SourceE.FieldA
          END
        WHEN STARTS_WITH('CCY-eAl5','Address-FbuO') THEN
          IF('Customer-EisE' <> 'Address-axeZ',SourceD.FieldE,'Code-G0za')
        WHEN 'Country-K3ZQ' NOT LIKE 'CCY-Yt4v' THEN
          IF(SourceE.FieldF = SourceD.FieldA,'Country-Lgqb',SourceC.FieldJ)
        ELSE
          IF(
            ENDS_WITH(SourceD.FieldF,'Country-z13n'), 'Address-hF1R',
            'Customer-daCO')
      END
        AS CountrywfBt,
      IF(
        SourceC.FieldI = SourceD.FieldB,
        IF(SourceC.FieldE <> 'Amt-XDSu',SourceC.FieldE,SourceE.FieldA),
        IF('CCY-U3vO' != SourceE.FieldJ,'Account-Ni2M','Fee-jqo3'))
        AS AccounttHRt,
      CASE
        WHEN STARTS_WITH(SourceC.FieldA,'Customer-xf3q') THEN
          IF('Code-sqe6' LIKE SourceA.FieldE,SourceC.FieldJ,'Customer-kuCy')
        WHEN STARTS_WITH(SourceE.FieldI,'Account-UhD6') THEN
          IF(
            STARTS_WITH(SourceE.FieldC,'Amt-0Oql'), SourceC.FieldJ,
            SourceC.FieldF)
        WHEN STARTS_WITH(SourceD.FieldA,SourceA.FieldC) THEN
          CASE
            WHEN SourceC.FieldF <> 'Address-bBdh' THEN
              'Fee-PcZe'
            WHEN SourceA.FieldH != SourceC.FieldH THEN
              'Address-DFhP'
            ELSE
              'Code-Vlil'
          END
        WHEN STARTS_WITH('Fee-3tvo',SourceA.FieldE) THEN
          CASE
            WHEN 'Amt-Caox' NOT LIKE 'Address-zZQJ' THEN
              'Customer-Kwrl'
            WHEN 'Country-RKiv' NOT LIKE SourceA.FieldC THEN
              'Amt-6cqF'
            WHEN 'Customer-v6FA' NOT LIKE SourceA.FieldC THEN
              'Account-jR8d'
            WHEN SourceA.FieldE LIKE 'Customer-cGhX' THEN
              SourceE.FieldA
            ELSE
              SourceD.FieldD
          END
        ELSE
          IF(SourceA.FieldC <> 'Code-Sp0H','Address-KTnJ','Amt-1c7U')
      END
        AS AccountSd1m,
      IF(
        'CCY-BxuJ' != 'Country-Uobk',
        IF('Code-I3kJ' != 'Address-zJXt','Fee-kjsf',SourceA.FieldE),
        IF(STARTS_WITH('Address-j8t0','CCY-eTvJ'),'Amt-F2rT','Amt-uihr'))
        AS CustomerybpW
    FROM
      testdataset.SourceA AS SourceA
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH(SourceC.FieldA,SourceA.FieldD) AND
         SourceA.FieldD <> 'Code-qOT7' OR
         ENDS_WITH(SourceA.FieldA,SourceA.FieldI) AND
         SourceC.FieldE != SourceA.FieldB)
      INNER JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH(SourceA.FieldD,SourceA.FieldE) OR
         ENDS_WITH(SourceD.FieldB,SourceA.FieldD) AND
         SourceC.FieldG NOT LIKE 'Country-CaxZ' AND
         SourceC.FieldE = SourceA.FieldH)
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (SourceC.FieldF LIKE 'Code-ZwDz' OR
         STARTS_WITH('Fee-s2f7',SourceD.FieldC) AND
         STARTS_WITH(SourceD.FieldF,'Customer-c48Z') OR
         STARTS_WITH(SourceE.FieldG,'Fee-Bgg1'))
    WHERE
      ENDS_WITH(SourceE.FieldG,'Address-qtpf') OR
      'Country-Xrvk' != SourceC.FieldB OR
      SourceA.FieldF LIKE SourceA.FieldD AND
      ENDS_WITH('Customer-wbWu',SourceE.FieldF) AND
      ENDS_WITH(SourceA.FieldJ,SourceC.FieldD) AND
      SourceD.FieldF = 'Country-cZX4' AND
      STARTS_WITH(SourceA.FieldI,SourceE.FieldB) AND
      STARTS_WITH(SourceC.FieldG,SourceE.FieldE)
  ),
  Table2 AS (
    SELECT
      IF(
        ENDS_WITH(SourceA.FieldC,SourceA.FieldB),
        IF(SourceE.FieldB = SourceB.FieldF,'Customer-k1b1','Amt-0Fcw'),
        IF('Code-ntXf' = SourceB.FieldD,SourceD.FieldJ,SourceE.FieldA))
        AS Amtm3NE,
      IF(
        STARTS_WITH(SourceD.FieldI,'Amt-uxRI'),
        IF('CCY-iLfY' NOT LIKE 'Fee-xVGg','Amt-TJtY','Account-8MxS'),
        IF(
          ENDS_WITH(SourceB.FieldB,SourceD.FieldI), 'Account-xCOv',
          SourceD.FieldC))
        AS CodewHdY,
      CASE
        WHEN SourceB.FieldD NOT LIKE SourceA.FieldC THEN
          IF(ENDS_WITH('Account-6UYv',SourceE.FieldH),'Amt-gRBx',SourceE.FieldD)
        WHEN SourceB.FieldJ <> SourceA.FieldE THEN
          IF(
            STARTS_WITH(SourceB.FieldF,'Account-AEWZ'), SourceA.FieldF,
            SourceD.FieldG)
        WHEN 'Address-eDBK' NOT LIKE SourceA.FieldA THEN
          IF(ENDS_WITH(SourceD.FieldI,'Amt-fS70'),'Amt-WKLQ','Country-0bTJ')
        ELSE
          CASE
            WHEN ENDS_WITH(SourceB.FieldF,'CCY-RziL') THEN
              SourceD.FieldI
            WHEN SourceD.FieldG LIKE 'Address-n103' THEN
              SourceE.FieldG
            WHEN 'Account-s7Dm' NOT LIKE 'Country-FIhN' THEN
              SourceD.FieldF
            WHEN ENDS_WITH('Account-jCCZ',SourceA.FieldF) THEN
              'Country-uG0g'
            ELSE
              'Code-nTqM'
          END
      END
        AS Fee67Xh,
      CASE
        WHEN SourceA.FieldE <> 'Fee-m7QI' THEN
          IF('Fee-t4Pa' != 'Address-32S2',SourceB.FieldA,'Country-tP4a')
        WHEN 'Customer-UmRO' NOT LIKE 'Code-fcLF' THEN
          IF(SourceA.FieldB != SourceE.FieldB,SourceB.FieldJ,SourceD.FieldF)
        WHEN STARTS_WITH(SourceE.FieldI,SourceD.FieldA) THEN
          IF(SourceB.FieldD != SourceB.FieldE,SourceE.FieldI,'CCY-sAyS')
        ELSE
          IF(SourceB.FieldB LIKE 'Customer-WbUh',SourceD.FieldB,'Account-LHwL')
      END
        AS AccountmDS1,
      IF(
        SourceE.FieldE <> SourceB.FieldG,
        IF(
          'Country-Fy0Y' NOT LIKE 'Address-ojVo', SourceE.FieldC,
          'Address-KHLt'),
        CASE
          WHEN ENDS_WITH(SourceD.FieldJ,SourceB.FieldD) THEN
            'Account-N2Zm'
          WHEN ENDS_WITH('Amt-iPlx',SourceE.FieldF) THEN
            SourceB.FieldD
          ELSE
            'Account-tQZS'
        END)
        AS CustomerqZGi,
      IF(
        ENDS_WITH(SourceA.FieldE,'Country-2D8j'),
        IF(SourceB.FieldJ = SourceE.FieldD,SourceA.FieldC,SourceD.FieldE),
        IF(SourceB.FieldB != 'Country-B0mC',SourceE.FieldE,SourceA.FieldI))
        AS AmtM3tT,
      IF(
        STARTS_WITH('Customer-1IIO','Fee-Uvcs'),
        IF(
          STARTS_WITH(SourceB.FieldB,'Customer-66jw'), 'Amt-KGnD',
          'Country-3d6J'),
        IF('Code-hdX0' != SourceD.FieldA,'Fee-uhK7','Account-ny73'))
        AS Amt6PIN,
      CASE
        WHEN SourceA.FieldE != SourceE.FieldJ THEN
          CASE
            WHEN ENDS_WITH(SourceA.FieldH,SourceA.FieldA) THEN
              'CCY-PSpK'
            WHEN STARTS_WITH(SourceE.FieldF,SourceA.FieldH) THEN
              SourceD.FieldB
            ELSE
              SourceE.FieldD
          END
        WHEN 'Fee-CfFo' NOT LIKE 'Country-ERc6' THEN
          IF(ENDS_WITH('Country-DRSP','Code-p0bs'),'Account-4JLS','Code-JqUo')
        WHEN STARTS_WITH('Country-6WYm','Fee-GYpg') THEN
          IF('Customer-M3gQ' LIKE SourceD.FieldD,SourceB.FieldH,'CCY-SKd3')
        WHEN 'CCY-NRZI' <> SourceE.FieldI THEN
          IF('Address-BF5I' NOT LIKE 'Country-bEaF','Customer-I2Qs','Fee-MJa4')
        WHEN ENDS_WITH('Customer-cIT7','Amt-2h63') THEN
          IF('Fee-jxGX' != SourceE.FieldA,'Fee-Lwj8','Customer-zexf')
        ELSE
          IF('Amt-fqv0' <> SourceA.FieldB,'CCY-Pmtb',SourceE.FieldB)
      END
        AS AccountuV6c,
      IF(
        STARTS_WITH('Account-lJ1V','Amt-3l69'),
        IF('Account-Fsbh' LIKE SourceE.FieldG,'CCY-Z5lA','CCY-egk7'),
        IF(SourceD.FieldJ = SourceD.FieldE,SourceD.FieldG,'Amt-7v1l'))
        AS AddressSaXB,
      CASE
        WHEN ENDS_WITH('Address-e4UH','CCY-OqTO') THEN
          CASE
            WHEN 'Code-iDxp' LIKE SourceD.FieldG THEN
              'Address-i87l'
            WHEN SourceD.FieldF != 'Amt-IxTY' THEN
              SourceB.FieldC
            ELSE
              'CCY-M6Nw'
          END
        WHEN STARTS_WITH(SourceD.FieldC,SourceB.FieldF) THEN
          IF(SourceA.FieldH <> SourceA.FieldC,SourceB.FieldB,'CCY-NumO')
        WHEN 'Amt-1IEC' NOT LIKE SourceB.FieldE THEN
          IF(SourceB.FieldG LIKE 'Country-eiOo','Code-9z2V','Customer-hM4L')
        WHEN ENDS_WITH('Fee-PEGM',SourceB.FieldD) THEN
          IF(SourceA.FieldH = 'Address-EAwK','Country-Ys7A',SourceE.FieldF)
        ELSE
          IF(ENDS_WITH('Amt-KMZ1',SourceE.FieldF),SourceB.FieldC,SourceA.FieldH)
      END
        AS AddressxJst,
      IF(
        SourceE.FieldC != SourceD.FieldH,
        IF('Amt-imHv' <> SourceE.FieldC,SourceE.FieldI,'Customer-b0eC'),
        IF(ENDS_WITH(SourceB.FieldB,SourceE.FieldF),SourceE.FieldD,'CCY-uu6d'))
        AS Customerouuc,
      IF(
        SourceE.FieldB != SourceD.FieldC,
        IF(
          STARTS_WITH(SourceA.FieldC,'Address-Vu2x'), SourceE.FieldJ,
          SourceA.FieldF), IF(
                             STARTS_WITH('Address-G8V8',SourceD.FieldG),
                             'Code-V65H', SourceB.FieldE))
        AS AddressztAA,
      IF(
        STARTS_WITH(SourceE.FieldE,SourceB.FieldB),
        IF(ENDS_WITH(SourceB.FieldF,SourceA.FieldB),SourceE.FieldG,'Amt-ifRN'),
        IF(STARTS_WITH('CCY-XGy4','Code-Rfaf'),'Code-72xa','Code-3rWD'))
        AS CCYp1y0,
      IF(
        SourceA.FieldB = SourceD.FieldI,
        IF(SourceD.FieldF <> SourceB.FieldC,'Country-a8X7',SourceB.FieldI),
        IF(SourceA.FieldA LIKE SourceB.FieldE,SourceB.FieldC,'Country-Keh2'))
        AS CCYryw5,
      IF(
        'Country-1x7b' NOT LIKE SourceD.FieldH,
        IF(ENDS_WITH('CCY-GTTy','Customer-GZoR'),SourceB.FieldI,SourceE.FieldB),
        IF(
          STARTS_WITH('Country-iUqF',SourceD.FieldD), SourceD.FieldF,
          SourceD.FieldI))
        AS FeeJgkn,
      IF(
        SourceB.FieldH != 'Code-aODX',
        IF(SourceD.FieldH = SourceB.FieldI,SourceE.FieldC,SourceB.FieldH),
        IF(
          ENDS_WITH(SourceD.FieldI,SourceA.FieldD), SourceB.FieldE,
          SourceB.FieldC))
        AS CCYDE77,
      IF(
        STARTS_WITH('Amt-k1m6','Country-O4px'),
        IF(SourceE.FieldB <> SourceE.FieldG,SourceE.FieldA,SourceA.FieldD),
        IF(SourceB.FieldD LIKE SourceB.FieldA,'Code-xR58','Amt-v7Z2'))
        AS CountryGiw3,
      IF(
        'Address-ygKh' NOT LIKE SourceE.FieldA,
        IF(STARTS_WITH(SourceE.FieldD,'Amt-0WSx'),'Code-vCR3',SourceA.FieldA),
        IF(
          STARTS_WITH('Country-MuoK','Customer-MO2B'), SourceD.FieldB,
          'CCY-Awdc'))
        AS CustomerRv8H
    FROM
      testdataset.SourceE AS SourceE
      INNER JOIN testdataset.SourceA AS SourceA ON
        (SourceE.FieldJ <> 'Code-g9nl' AND
         STARTS_WITH(SourceE.FieldD,'Fee-JuPr') AND
         'Code-MJh8' <> SourceA.FieldJ OR
         ENDS_WITH(SourceE.FieldH,'Address-bQh4'))
      INNER JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Country-ESZt',SourceA.FieldJ) AND
         STARTS_WITH(SourceB.FieldE,'Customer-CWAn') OR
         'Fee-r1FP' != SourceE.FieldF AND
         STARTS_WITH('Country-w1bl',SourceB.FieldG))
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH(SourceA.FieldF,SourceD.FieldD) AND
         SourceE.FieldG LIKE 'Amt-e19k' OR
         ENDS_WITH('Account-4jYB','CCY-PFCy') AND
         ENDS_WITH(SourceB.FieldF,'Account-qZjp'))
    WHERE
      STARTS_WITH(SourceD.FieldD,SourceB.FieldE) OR
      'Fee-FNoj' LIKE SourceD.FieldB AND
      SourceD.FieldA <> 'Address-ZBwf' OR SourceE.FieldC LIKE 'Country-KyWj' AND
      STARTS_WITH(SourceB.FieldB,'CCY-Pow6') OR
      SourceE.FieldF NOT LIKE SourceD.FieldA AND
      SourceA.FieldC <> SourceA.FieldI OR
      ENDS_WITH(SourceA.FieldF,SourceA.FieldH)
  ),
  Table3 AS (
    SELECT
      IF(
        STARTS_WITH('Code-wlFb',Table2.AmtM3tT),
        IF('Account-pxKL' = 'Fee-rY7T','Fee-6NRT',SourceE.FieldI),
        IF(
          STARTS_WITH(Table2.Fee67Xh,'Address-vz4Y'), SourceE.FieldC,
          Table2.AccountuV6c))
        AS Customerah3l,
      IF(
        STARTS_WITH(SourceB.FieldG,SourceA.FieldB),
        CASE
          WHEN STARTS_WITH('Country-iZdi',SourceC.FieldI) THEN
            'Customer-QnTD'
          WHEN STARTS_WITH(Table1.Account4mSm,Table1.CountryQVL3) THEN
            Table2.CCYDE77
          ELSE
            'Country-NYPU'
        END, IF(
               ENDS_WITH('Fee-9Z9O',Table1.Customer3oUD), 'Account-DDwE',
               Table2.AmtM3tT))
        AS CodeZX2t,
      CASE
        WHEN 'Address-npuW' != 'Account-x8TL' THEN
          IF(
            SourceD.FieldI = Table2.CustomerqZGi, Table1.CustomerybpW,
            SourceA.FieldJ)
        WHEN ENDS_WITH(SourceD.FieldE,SourceB.FieldF) THEN
          IF(
            ENDS_WITH(SourceD.FieldD,'Customer-X8PA'), Table1.Feex0t9,
            'Account-FEQF')
        ELSE
          IF(
            Table2.Fee67Xh LIKE Table2.CodewHdY, Table2.CustomerqZGi,
            SourceE.FieldI)
      END
        AS Amt3ZH8,
      IF(
        STARTS_WITH(Table1.CustomerybpW,Table1.CountryQVL3),
        IF(STARTS_WITH('Amt-y1Pw','Amt-DTZL'),SourceB.FieldJ,'Code-C3Oz'),
        IF('Account-Y9GQ' LIKE 'Account-F7Bu',Table1.CCYDFbw,'Customer-LHe8'))
        AS AddressItuV,
      IF(
        STARTS_WITH('Country-ExtX',SourceD.FieldA),
        IF(SourceC.FieldD != Table1.CCYQDlu,'Amt-nQ92','Fee-rjdU'),
        IF('Account-bgyo' = SourceB.FieldD,'Address-p0cF','Customer-TMta'))
        AS AddressReUj,
      IF(
        'CCY-W427' LIKE SourceD.FieldG,
        IF(Table1.AccounttHRt LIKE SourceE.FieldE,'Amt-XWNB',SourceD.FieldC),
        IF(Table2.CCYDE77 <> Table1.CCYQDlu,'Customer-k9Wk',SourceC.FieldB))
        AS Amt0UkG,
      IF(
        ENDS_WITH('Code-1rXh',Table2.CustomerqZGi),
        CASE
          WHEN SourceE.FieldA = SourceA.FieldD THEN
            SourceA.FieldJ
          WHEN SourceD.FieldH LIKE SourceB.FieldG THEN
            'CCY-Pvy4'
          WHEN SourceE.FieldI != SourceC.FieldJ THEN
            SourceA.FieldD
          WHEN STARTS_WITH('Customer-04ep','CCY-Aftw') THEN
            'Fee-YKT2'
          WHEN STARTS_WITH('Country-dpHl','Fee-bqVe') THEN
            'Address-D67q'
          ELSE
            'Country-T2QT'
        END, IF(
               STARTS_WITH('Customer-Y0z4',SourceB.FieldH), 'Code-g1WJ',
               SourceA.FieldG))
        AS CountrycRy2,
      IF(
        'Account-rtfv' NOT LIKE Table1.CountrywfBt,
        CASE
          WHEN ENDS_WITH('Address-Xbxq','Account-VXk2') THEN
            'CCY-HkK5'
          WHEN 'Country-pAfX' LIKE SourceE.FieldE THEN
            'Amt-955S'
          ELSE
            SourceD.FieldI
        END, IF(
               'Account-0uzL' <> 'Country-LkLi', Table1.Customer2RaL,
               Table2.CustomerqZGi))
        AS Addressc4Sp,
      IF(
        ENDS_WITH(SourceA.FieldI,'Address-SAvS'),
        IF(ENDS_WITH('Code-Pgis','Code-YX5A'),SourceC.FieldF,'Account-F7eE'),
        CASE
          WHEN 'Customer-OkHo' NOT LIKE 'Address-gLY3' THEN
            'Fee-62RA'
          WHEN ENDS_WITH('Customer-1uiD','Amt-ZT5v') THEN
            'Code-DWjw'
          WHEN 'Amt-nUhT' NOT LIKE SourceD.FieldG THEN
            'Customer-J5pG'
          WHEN STARTS_WITH(SourceA.FieldG,'CCY-ABWT') THEN
            'Country-g9wX'
          ELSE
            'Address-nOi7'
        END)
        AS CountryEVqL,
      IF(
        ENDS_WITH(SourceC.FieldF,'Customer-aO0b'),
        IF('CCY-JzcM' = SourceD.FieldF,'Fee-ismc',Table2.CustomerRv8H),
        CASE
          WHEN SourceD.FieldF <> 'Country-Ewjn' THEN
            'Customer-4Oiz'
          WHEN SourceA.FieldF <> Table1.AccounttHRt THEN
            'Code-UtRY'
          WHEN ENDS_WITH(SourceA.FieldD,'Amt-iHIo') THEN
            SourceC.FieldC
          ELSE
            SourceB.FieldB
        END)
        AS CountryLVJe,
      IF(
        STARTS_WITH(SourceB.FieldB,'Country-6viK'),
        IF(
          STARTS_WITH(SourceB.FieldA,'Country-tGRN'), SourceA.FieldD,
          'Fee-XwpJ'), IF(
                         STARTS_WITH(SourceD.FieldB,SourceB.FieldJ),
                         'Address-TSfh', 'Address-2Gez'))
        AS AccountvVoJ,
      IF(
        STARTS_WITH('Code-LiIe',SourceE.FieldI),
        IF('CCY-kiYz' LIKE Table1.CustomerybpW,Table2.CodewHdY,SourceA.FieldI),
        IF(ENDS_WITH(Table1.Code6Ols,'CCY-EDqN'),'Code-dq3z','Country-KyFJ'))
        AS AccountYmqE,
      IF(
        ENDS_WITH(SourceE.FieldH,Table2.CCYDE77),
        CASE
          WHEN 'Address-znqs' != 'Amt-xE17' THEN
            'Code-mjjN'
          WHEN ENDS_WITH(SourceC.FieldC,Table2.AddressxJst) THEN
            'Account-MITY'
          WHEN STARTS_WITH(Table2.CustomerqZGi,'Account-Op9x') THEN
            'Account-iiIh'
          ELSE
            'Code-9BT1'
        END,
        CASE
          WHEN ENDS_WITH(Table2.Fee67Xh,SourceE.FieldI) THEN
            'Account-lp5m'
          WHEN ENDS_WITH('Fee-tsrV','Customer-Y6oI') THEN
            Table2.CodewHdY
          WHEN 'Fee-NXkp' LIKE 'Code-ym9Y' THEN
            SourceD.FieldI
          WHEN STARTS_WITH(SourceC.FieldH,'Fee-WsRf') THEN
            Table2.AddressxJst
          ELSE
            Table2.AddressSaXB
        END)
        AS AddressKget,
      IF(
        'Address-GUQH' NOT LIKE 'Country-lKYP',
        IF('Account-YetP' = Table1.CCYjmUx,SourceD.FieldH,SourceA.FieldE),
        IF(
          ENDS_WITH(Table2.FeeJgkn,SourceB.FieldB), SourceB.FieldG,
          'Country-mWEc'))
        AS Codez1OQ
    FROM
      testdataset.SourceE AS SourceE
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (SourceA.FieldB LIKE SourceE.FieldG OR
         STARTS_WITH(SourceA.FieldI,'CCY-g1eD') AND
         'Account-moci' NOT LIKE SourceE.FieldE OR
         SourceA.FieldJ LIKE SourceA.FieldF)
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        ('Amt-iU4i' <> 'Address-q4sy' AND SourceB.FieldE = SourceE.FieldA AND
         'Amt-vRjp' NOT LIKE SourceB.FieldA OR
         STARTS_WITH('Account-QMBG','Fee-4E9j'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (SourceB.FieldE = 'Address-7AhO' AND
         STARTS_WITH('Country-N6CK',SourceE.FieldF) AND
         ENDS_WITH(SourceC.FieldC,SourceC.FieldJ) AND
         'Account-k6Z4' = SourceC.FieldA)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (SourceC.FieldG != SourceE.FieldE OR SourceA.FieldG = 'Amt-MD77' AND
         ENDS_WITH(SourceC.FieldD,SourceD.FieldA) AND
         SourceC.FieldE = SourceD.FieldB)
      RIGHT JOIN Table1 AS Table1 ON
        (ENDS_WITH(SourceC.FieldC,'Customer-ATyg') AND
         STARTS_WITH(SourceA.FieldF,Table1.AccounttHRt) OR
         SourceC.FieldH NOT LIKE SourceC.FieldC AND
         ENDS_WITH('Code-5jdZ','Fee-wgmY'))
      RIGHT JOIN Table2 AS Table2 ON
        (ENDS_WITH(Table2.CustomerRv8H,'Customer-2NA3') OR
         'Code-vcFE' != 'Fee-wIxv' AND
         'Customer-J8SZ' NOT LIKE SourceE.FieldG AND
         'Code-hhCH' LIKE 'Account-0VrD')
    WHERE
      STARTS_WITH('Amt-myE6',SourceD.FieldA) AND
      STARTS_WITH('Code-GQ8R','Customer-SeCH') OR
      Table2.Amt6PIN <> 'Fee-nZ9S' AND 'Code-NknB' = 'Fee-lWxJ' AND
      ENDS_WITH('Country-5lWX','Customer-BA1Z') OR
      STARTS_WITH('Code-X0PD','Fee-d1J4') OR
      STARTS_WITH(Table1.CustomerybpW,'Country-wEAV') AND
      ENDS_WITH(SourceC.FieldJ,SourceE.FieldI)
  ),
  Table4 AS (
    SELECT
      IF(
        ENDS_WITH(SourceC.FieldC,Table2.CustomerRv8H),
        IF('Amt-5jnl' <> 'Amt-X3pz','Address-ksod','Amt-X7OK'),
        CASE
          WHEN SourceA.FieldF = 'Account-id86' THEN
            'Account-caDG'
          WHEN 'Amt-c6jI' != SourceC.FieldD THEN
            Table1.CountryQVL3
          ELSE
            SourceE.FieldA
        END)
        AS CountryAv9y,
      IF(
        Table3.AddressItuV <> SourceA.FieldE,
        CASE
          WHEN SourceD.FieldE = 'CCY-DulB' THEN
            SourceA.FieldJ
          WHEN STARTS_WITH(SourceD.FieldI,Table3.AddressReUj) THEN
            Table2.AddressSaXB
          WHEN ENDS_WITH('Amt-XvLh','Fee-Hjgg') THEN
            SourceA.FieldI
          WHEN Table3.Addressc4Sp = Table1.Account4mSm THEN
            'Code-T7i2'
          WHEN SourceD.FieldC LIKE Table3.AddressItuV THEN
            Table3.Amt0UkG
          ELSE
            'Country-vyBr'
        END,
        IF(ENDS_WITH('Code-zEV4','Fee-o7N5'),SourceC.FieldC,'Account-3cm4'))
        AS Fee8X1x,
      IF(
        'Customer-QS5e' = SourceE.FieldF,
        IF('Customer-mjDm' != 'Address-LaiS',SourceD.FieldC,Table2.AddressxJst),
        IF(
          STARTS_WITH('Address-JMqe',SourceE.FieldD), Table3.Amt3ZH8,
          Table3.CountrycRy2))
        AS FeeD7R4,
      IF(
        SourceE.FieldJ NOT LIKE SourceD.FieldE,
        CASE
          WHEN Table1.CustomerybpW NOT LIKE 'CCY-EruD' THEN
            'Address-xRzI'
          WHEN 'Customer-T9oA' = 'Address-B3dw' THEN
            SourceE.FieldF
          ELSE
            'Amt-pko9'
        END, IF(
               ENDS_WITH(Table1.Code6Ols,'Customer-IlZb'), Table2.AddressxJst,
               SourceA.FieldB))
        AS CCYw7wt,
      IF(
        STARTS_WITH(SourceE.FieldC,SourceD.FieldG),
        IF(
          STARTS_WITH('CCY-ABZH','Address-HXWl'), SourceC.FieldJ,
          SourceA.FieldF),
        IF(ENDS_WITH(Table1.Account4mSm,'Address-EJaV'),'Code-ykP7','Fee-eayz'))
        AS Amt69Sr,
      IF(
        'Account-hZu4' LIKE 'Fee-W8Rt',
        IF('Address-3Clj' = 'Customer-sni8',SourceA.FieldI,'Country-NvH0'),
        IF(STARTS_WITH(SourceA.FieldD,'Amt-eNYS'),'Code-TEdF','Country-0n5t'))
        AS CountryEZt1,
      CASE
        WHEN ENDS_WITH('Code-QU0R','CCY-htQb') THEN
          IF(Table1.Feex0t9 <> Table2.CCYp1y0,'Country-Of2H',SourceA.FieldH)
        WHEN ENDS_WITH(Table1.AccountSd1m,'Fee-LMYg') THEN
          IF(
            STARTS_WITH(SourceE.FieldG,SourceE.FieldD), Table2.Fee67Xh,
            'Country-2err')
        WHEN STARTS_WITH(SourceC.FieldC,Table1.CountrywfBt) THEN
          IF(ENDS_WITH(SourceC.FieldI,SourceA.FieldJ),'CCY-YdiF',SourceC.FieldB)
        WHEN STARTS_WITH('Address-KQF5',Table1.CCYjmUx) THEN
          IF(
            STARTS_WITH('Code-cBDn',Table1.Code6Ols), 'Account-oaBn',
            Table2.Fee67Xh)
        WHEN 'Address-ngCK' != 'Address-XwNZ' THEN
          CASE
            WHEN 'CCY-yfJj' <> SourceD.FieldF THEN
              Table3.Amt3ZH8
            WHEN 'Code-MIxn' LIKE 'Code-5fHp' THEN
              'Amt-zd6u'
            WHEN 'Account-CGto' NOT LIKE 'Address-xIJR' THEN
              Table1.Customer3oUD
            WHEN ENDS_WITH('Amt-jePQ','Amt-ulid') THEN
              SourceD.FieldH
            ELSE
              'Country-4wVe'
          END
        ELSE
          IF(
            ENDS_WITH('Country-bKel','Account-H6pB'), 'Customer-ONZ6',
            SourceE.FieldF)
      END
        AS CCYwhT3,
      IF(
        STARTS_WITH(SourceA.FieldC,SourceE.FieldG),
        IF(
          Table2.CustomerqZGi <> 'Customer-8mgm', Table2.CustomerRv8H,
          Table1.Amt3Ib2), IF(
                             ENDS_WITH(Table2.Amt6PIN,SourceD.FieldI),
                             'Customer-xdlo', Table1.Code6Ols))
        AS AmtYzGk,
      IF(
        SourceA.FieldJ NOT LIKE 'Code-a3Z8',
        CASE
          WHEN 'Fee-ZTa2' NOT LIKE SourceE.FieldH THEN
            SourceD.FieldE
          WHEN STARTS_WITH(Table2.CustomerRv8H,SourceA.FieldD) THEN
            'Amt-ASdC'
          WHEN SourceE.FieldJ NOT LIKE 'Fee-7LdV' THEN
            'Customer-X2Sp'
          WHEN SourceA.FieldB NOT LIKE SourceA.FieldI THEN
            'Account-OzEB'
          ELSE
            'Address-evvZ'
        END, IF(
               ENDS_WITH(SourceA.FieldD,'Address-CDuE'), Table3.AccountYmqE,
               'Address-2dMC'))
        AS CodeCN1H,
      IF(
        SourceC.FieldI != SourceE.FieldC,
        CASE
          WHEN Table1.CCYjmUx <> SourceA.FieldF THEN
            'Account-NKka'
          WHEN STARTS_WITH(SourceD.FieldJ,'Country-Vse3') THEN
            'Address-hCJ3'
          ELSE
            'Code-hhqd'
        END,
        IF(STARTS_WITH('Code-ddkd','CCY-YOx1'),Table1.CCYQDlu,'Address-ckdh'))
        AS CustomeriAlh,
      IF(
        STARTS_WITH(Table2.CustomerRv8H,'Country-2jjt'),
        IF(
          STARTS_WITH('Address-yy5c','Customer-hLwL'), SourceA.FieldG,
          SourceE.FieldJ), IF(
                             SourceA.FieldE LIKE Table1.CountryQVL3, 'Amt-hnYp',
                             Table1.Account4mSm))
        AS CodeSPJh,
      CASE
        WHEN STARTS_WITH(Table1.Code6Ols,'Fee-GocL') THEN
          IF('Amt-1s7T' = SourceE.FieldD,SourceC.FieldH,'Address-Rcjb')
        WHEN 'Code-mD4e' = SourceC.FieldJ THEN
          IF('Amt-B3Bv' = 'Code-Fp7e',Table2.CountryGiw3,SourceA.FieldC)
        WHEN 'Code-ewUy' <> Table3.CodeZX2t THEN
          IF(SourceA.FieldI = 'CCY-7VhL','CCY-NhLw',SourceE.FieldB)
        WHEN STARTS_WITH('Code-99Xs',SourceC.FieldE) THEN
          IF(SourceC.FieldC != SourceD.FieldG,SourceE.FieldB,SourceE.FieldI)
        ELSE
          CASE
            WHEN STARTS_WITH(Table1.Customer2RaL,Table3.AddressKget) THEN
              SourceD.FieldB
            WHEN ENDS_WITH('Fee-hf1P',Table3.CountrycRy2) THEN
              'Account-dA6G'
            ELSE
              Table2.AddressSaXB
          END
      END
        AS AmtXstb,
      CASE
        WHEN STARTS_WITH('CCY-6WIb',SourceE.FieldD) THEN
          IF('Address-EiOn' = SourceA.FieldG,Table3.AddressReUj,SourceE.FieldJ)
        WHEN ENDS_WITH(SourceC.FieldD,Table2.CCYDE77) THEN
          CASE
            WHEN SourceD.FieldB = 'Code-864l' THEN
              'Code-PnKP'
            WHEN ENDS_WITH('Account-o1VF',Table2.AddressxJst) THEN
              Table1.Amt3Ib2
            ELSE
              SourceD.FieldA
          END
        ELSE
          IF(
            Table3.AddressItuV NOT LIKE 'Account-ufSP', Table1.FeeNRHE,
            'Account-g4DD')
      END
        AS CountryNePa,
      IF(
        Table3.AccountYmqE != 'Address-nLAr',
        IF(
          SourceE.FieldG NOT LIKE 'Address-nTEi', Table2.AmtM3tT,
          SourceC.FieldI), IF(
                             STARTS_WITH(SourceC.FieldA,Table1.CCYjmUx),
                             SourceC.FieldJ, 'Account-8ZCH'))
        AS CountrywojM,
      IF(
        'Country-1tp9' != Table1.CountryQVL3,
        IF(
          STARTS_WITH(SourceE.FieldI,SourceE.FieldF), 'Account-T9rf',
          'Customer-6R6a'),
        IF(SourceA.FieldA != Table3.Addressc4Sp,SourceD.FieldH,SourceE.FieldG))
        AS AccountwzYY,
      CASE
        WHEN 'Account-Wc7B' != 'Customer-TyyS' THEN
          IF(SourceA.FieldF <> 'Account-YQxa','CCY-ALq4','Customer-S7hH')
        WHEN SourceE.FieldI = SourceD.FieldJ THEN
          IF(SourceC.FieldC = Table3.AddressItuV,Table3.CodeZX2t,'Amt-IK7q')
        WHEN 'Customer-4aXB' NOT LIKE Table2.Customerouuc THEN
          IF(ENDS_WITH('Address-tWlE','CCY-lh6U'),'Country-yhl7',SourceE.FieldD)
        WHEN SourceD.FieldB LIKE SourceA.FieldH THEN
          IF(
            'Code-8sKD' != SourceE.FieldC, Table2.CustomerqZGi,
            Table2.AddressSaXB)
        ELSE
          IF(
            Table2.CustomerqZGi NOT LIKE Table2.CountryGiw3, SourceC.FieldE,
            Table1.CCYDFbw)
      END
        AS AccountvOrS,
      IF(
        'CCY-y1Vi' = 'Fee-5RLq',
        IF(
          ENDS_WITH(Table1.AccountSd1m,'Customer-5rMH'), Table3.AddressItuV,
          Table2.CustomerqZGi),
        IF(
          Table2.CountryGiw3 != Table3.CountryLVJe, 'Fee-FRxR',
          Table2.CustomerRv8H))
        AS FeeWpYv,
      IF(
        SourceD.FieldB <> SourceE.FieldE,
        IF('Fee-pciE' NOT LIKE 'Customer-8rXU','Country-XcAp','Account-v5za'),
        IF(STARTS_WITH(SourceC.FieldI,'CCY-SzFw'),'CCY-HlSP','Code-Erci'))
        AS CCYp1KW
    FROM
      Table2 AS Table2
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH(SourceA.FieldD,Table2.CustomerqZGi) OR
         Table2.Amt6PIN <> 'Account-bRjM' AND
         SourceA.FieldF LIKE Table2.AccountuV6c OR
         ENDS_WITH('Customer-Y3xa','Fee-htes'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH('Country-SYzf','CCY-Bs6F') OR
         'Customer-LzCi' LIKE 'CCY-YAUL' OR
         'Customer-Gm6J' = SourceA.FieldG AND 'Amt-yNI8' = Table2.Amt6PIN)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (SourceD.FieldJ NOT LIKE 'Country-hFvv' OR
         STARTS_WITH('CCY-l3Ss','CCY-RTfo') OR
         'Customer-mgRE' NOT LIKE 'Code-jEDY' AND
         'CCY-GmXE' != Table2.CustomerqZGi)
      INNER JOIN testdataset.SourceE AS SourceE ON
        ('Customer-CDO6' <> 'Customer-Frgo' OR
         'Country-34QX' <> SourceE.FieldE AND
         SourceD.FieldB <> SourceA.FieldA AND
         STARTS_WITH(SourceE.FieldH,SourceD.FieldA))
      LEFT JOIN Table1 AS Table1 ON
        (Table1.Customer3oUD != Table2.AmtM3tT OR
         ENDS_WITH('Account-lJVW',Table2.AccountuV6c) OR
         STARTS_WITH('Customer-JRuf','Fee-Ft5Z') OR
         STARTS_WITH(Table2.Customerouuc,SourceD.FieldA))
      INNER JOIN Table3 AS Table3 ON
        (SourceD.FieldE != 'Account-3DVj' AND
         ENDS_WITH('Address-KXkJ',Table3.Customerah3l) AND
         SourceD.FieldJ != Table2.AddressztAA OR
         STARTS_WITH(Table1.Feex0t9,'Address-GEki'))
    WHERE
      'Customer-aIU1' <> 'CCY-SnK9' OR
      STARTS_WITH(SourceD.FieldA,Table1.FeeNRHE) AND
      'Account-RYyl' <> Table1.CCYDFbw AND
      Table3.CountryLVJe NOT LIKE Table1.Customer2RaL OR
      ENDS_WITH(Table2.CodewHdY,SourceD.FieldH) AND
      Table1.CountrywfBt = SourceD.FieldG OR
      'CCY-opxt' = 'Fee-zxgt' AND STARTS_WITH(Table1.Amt3Ib2,SourceE.FieldE)
  ),
  Table5 AS (
    SELECT
      IF(
        SourceA.FieldC != 'CCY-Y3M2',
        IF(SourceE.FieldG <> SourceC.FieldA,'Amt-JEaa','Code-p5UU'),
        IF(ENDS_WITH('CCY-Whni','CCY-6RsI'),'Fee-L1LY',SourceD.FieldJ))
        AS Addressji9M,
      IF(
        STARTS_WITH(SourceE.FieldB,'Country-b0lu'),
        IF(SourceE.FieldE = SourceD.FieldF,'CCY-DdYK',Table4.CountryEZt1),
        IF(SourceD.FieldG <> 'Code-zEAR','Address-DMil','CCY-Tgqg'))
        AS Customerng2S,
      CASE
        WHEN STARTS_WITH('CCY-Nu3A','Address-VoOr') THEN
          IF(ENDS_WITH(Table4.CCYp1KW,Table2.CCYryw5),'Amt-AtLI','Code-cXvN')
        WHEN STARTS_WITH(SourceC.FieldI,'Country-Q5WS') THEN
          IF(
            ENDS_WITH('Country-8v1r',SourceE.FieldD), Table4.CodeCN1H,
            'Account-3t9v')
        ELSE
          IF('Fee-MIAk' <> 'Country-wf44','Amt-zqc6','Code-bJdQ')
      END
        AS CountryMAW7,
      IF(
        'Fee-OIy4' <> 'Account-QeB7',
        IF(Table1.CCYQDlu <> Table4.CCYwhT3,'Amt-khp0','Fee-Bowj'),
        CASE
          WHEN 'Code-slkD' != 'CCY-LAfU' THEN
            'Customer-2N5C'
          WHEN ENDS_WITH('Account-ERDm','CCY-jg3x') THEN
            SourceA.FieldH
          WHEN 'Address-raA9' = Table2.CodewHdY THEN
            'Customer-vjaq'
          ELSE
            'Fee-FunL'
        END)
        AS CCYCfIe,
      IF(
        ENDS_WITH('Amt-fHVb','Code-HLSb'),
        IF('Fee-zYfb' NOT LIKE SourceE.FieldB,'CCY-TvyQ','Country-zFPR'),
        IF(Table4.AmtYzGk LIKE 'Country-gxz3','CCY-MqDA',SourceC.FieldF))
        AS Address82qt,
      CASE
        WHEN 'Address-eRmq' LIKE 'Customer-XkIt' THEN
          CASE
            WHEN ENDS_WITH('Country-5xVN',Table1.Account4mSm) THEN
              SourceE.FieldH
            WHEN Table2.Customerouuc != SourceC.FieldE THEN
              Table3.Amt0UkG
            WHEN ENDS_WITH('Code-iO47',Table2.AmtM3tT) THEN
              SourceC.FieldG
            WHEN 'CCY-r8aC' <> SourceA.FieldJ THEN
              'Address-e7O6'
            ELSE
              Table4.Amt69Sr
          END
        WHEN 'Code-Z9F7' != Table1.AccounttHRt THEN
          IF(
            STARTS_WITH(Table1.Amt3Ib2,SourceC.FieldI), 'Account-7GnL',
            Table2.AddressztAA)
        WHEN ENDS_WITH(SourceC.FieldB,SourceE.FieldD) THEN
          IF(SourceE.FieldJ = 'Amt-ZQIT',SourceA.FieldD,Table4.CCYwhT3)
        WHEN ENDS_WITH('Code-UZmf','Fee-x3iI') THEN
          CASE
            WHEN STARTS_WITH('Country-kSlA','Account-iIAV') THEN
              SourceD.FieldD
            WHEN STARTS_WITH(SourceD.FieldG,SourceD.FieldB) THEN
              Table3.CountryLVJe
            WHEN ENDS_WITH('Code-dmTK',Table1.Customer3oUD) THEN
              'Amt-Ioo2'
            WHEN STARTS_WITH('Amt-w2dJ',SourceD.FieldA) THEN
              'Amt-pcvU'
            WHEN ENDS_WITH(Table2.Fee67Xh,'Fee-8MSh') THEN
              'Account-sqWv'
            ELSE
              'Account-HJre'
          END
        WHEN STARTS_WITH('Fee-Xe3j','Country-HgbY') THEN
          IF(
            ENDS_WITH('Address-hKny','Code-HjXa'), Table2.Amt6PIN,
            'Address-btGv')
        ELSE
          CASE
            WHEN ENDS_WITH('Code-1ceJ','Code-htbj') THEN
              'Amt-uko4'
            WHEN STARTS_WITH(Table4.CCYw7wt,SourceB.FieldA) THEN
              'Country-hgGT'
            WHEN 'Address-QLul' LIKE SourceA.FieldC THEN
              SourceE.FieldB
            ELSE
              SourceC.FieldI
          END
      END
        AS CodeE4do,
      IF(
        STARTS_WITH(Table1.Amt3Ib2,'Customer-FBeU'),
        IF(
          ENDS_WITH(Table1.Code6Ols,'Amt-XnUP'), Table3.CountrycRy2,
          SourceA.FieldC), IF(
                             'Account-j87k' NOT LIKE Table3.Codez1OQ,
                             'Address-RBGq', Table4.CodeSPJh))
        AS AccountZZVy,
      IF(
        STARTS_WITH('Customer-MxYj',SourceE.FieldA),
        IF(
          'Code-MDng' <> Table3.Customerah3l, 'Address-BGVF',
          Table2.AddressxJst),
        CASE
          WHEN 'Country-q3Bs' NOT LIKE 'Account-gPUP' THEN
            'Customer-XqYn'
          WHEN Table3.Amt3ZH8 <> 'Country-RAK3' THEN
            Table4.CountryEZt1
          WHEN ENDS_WITH(SourceC.FieldD,'CCY-cv5a') THEN
            Table3.CodeZX2t
          WHEN ENDS_WITH(SourceA.FieldC,'Country-ETjT') THEN
            Table4.CountryNePa
          WHEN ENDS_WITH(SourceC.FieldE,'CCY-1xtF') THEN
            Table4.CountryAv9y
          ELSE
            'Code-oB8j'
        END)
        AS Fee2LBQ,
      IF(
        STARTS_WITH(Table1.CCYDFbw,Table4.CountrywojM),
        IF('Fee-MTe8' <> SourceD.FieldB,SourceC.FieldG,SourceD.FieldF),
        IF('Account-Tnm2' = SourceE.FieldI,'Address-vAkN','CCY-a9u1'))
        AS FeeX4QK,
      IF(
        STARTS_WITH(Table2.AddressztAA,'Customer-rVkw'),
        IF(ENDS_WITH(Table2.CCYDE77,'CCY-IVxa'),'CCY-FLP3','Fee-fZ66'),
        IF(
          STARTS_WITH(SourceA.FieldJ,'Country-lPnY'), SourceA.FieldI,
          Table2.AddressxJst))
        AS CustomerNZoH,
      CASE
        WHEN 'Code-z7UQ' NOT LIKE SourceA.FieldC THEN
          IF(Table3.CodeZX2t != Table2.Amt6PIN,Table4.AmtXstb,'Address-9HxZ')
        WHEN 'CCY-fjf7' = 'Account-QpMg' THEN
          IF(
            ENDS_WITH('Customer-jy1X','CCY-RaVx'), Table3.AccountYmqE,
            SourceD.FieldA)
        WHEN 'Account-OdPq' LIKE SourceD.FieldA THEN
          IF('Code-x5Ga' != 'Customer-ARbC','Address-CbBx',Table4.AmtXstb)
        ELSE
          IF('Address-cVJm' = Table4.AmtXstb,'Customer-shtw',Table1.CCYDFbw)
      END
        AS CCYwP2S,
      IF(
        Table3.Amt3ZH8 NOT LIKE 'Amt-Mmv6',
        CASE
          WHEN SourceA.FieldH = Table4.CCYw7wt THEN
            'Fee-gvsJ'
          WHEN STARTS_WITH(SourceE.FieldB,'Account-RtYx') THEN
            SourceB.FieldJ
          WHEN STARTS_WITH(SourceB.FieldD,SourceB.FieldG) THEN
            'Address-mvh0'
          WHEN ENDS_WITH(Table1.FeeNRHE,Table3.AddressReUj) THEN
            'Amt-dS9W'
          ELSE
            'Country-76c3'
        END,
        IF(Table2.CustomerqZGi != Table1.CCYjmUx,'Code-qU4i','Country-cxAC'))
        AS AccountkCVD,
      CASE
        WHEN Table2.AddressSaXB != 'Code-05X0' THEN
          IF(
            'CCY-ghlX' NOT LIKE 'Address-OlOi', Table2.CodewHdY,
            Table4.CountryEZt1)
        WHEN Table2.CustomerqZGi <> Table1.Account4mSm THEN
          IF('Customer-SAz4' LIKE 'Account-Z7rc','Customer-dgfl','Amt-I8es')
        WHEN ENDS_WITH(Table1.CCYQDlu,'CCY-nD4p') THEN
          CASE
            WHEN 'Address-S6qh' != Table4.FeeWpYv THEN
              SourceA.FieldH
            WHEN SourceC.FieldF <> SourceE.FieldA THEN
              SourceD.FieldF
            WHEN ENDS_WITH('Amt-JsP0',Table1.Customer3oUD) THEN
              'Fee-H6kC'
            ELSE
              'Country-S90e'
          END
        WHEN Table3.AddressReUj LIKE 'Amt-dMqc' THEN
          IF(
            STARTS_WITH(Table2.CCYryw5,Table1.AccounttHRt), Table1.Code6Ols,
            'Amt-5y0V')
        WHEN 'Country-UYoX' = Table3.CountryEVqL THEN
          IF(ENDS_WITH('Code-68QX','CCY-MgRf'),Table1.AccounttHRt,'Code-DM8K')
        ELSE
          IF(
            ENDS_WITH('Country-bZpw','Code-qsHR'), 'CCY-D2Pp',
            Table3.Customerah3l)
      END
        AS CCYDKu9,
      IF(
        'Account-g43F' = 'Address-9rnf',
        IF(
          Table2.AddressxJst <> SourceB.FieldD, Table4.AccountwzYY,
          Table3.CountryLVJe),
        IF(SourceD.FieldH != Table4.FeeWpYv,Table2.AccountmDS1,'Fee-FhBF'))
        AS Accountnuao,
      IF(
        STARTS_WITH('Address-uGGf',Table4.Amt69Sr),
        IF(SourceE.FieldF != Table2.AddressSaXB,'Customer-j2Ib','Address-RswD'),
        IF(
          ENDS_WITH(SourceB.FieldF,SourceC.FieldD), Table3.Codez1OQ,
          'Country-WwBV'))
        AS CCY38xb,
      IF(
        'Account-WaSb' LIKE 'Address-q2je',
        IF('Customer-4ugL' LIKE 'CCY-wF50','Customer-iAmR','Address-qYne'),
        IF('Code-HkDZ' = 'Address-tWQ1',SourceB.FieldH,'Customer-SHJy'))
        AS CCY7Hoo,
      IF(
        Table1.CountrywfBt <> Table4.AccountwzYY,
        CASE
          WHEN Table4.AccountvOrS <> 'Fee-upN0' THEN
            'Code-V9v2'
          WHEN ENDS_WITH(Table3.AddressItuV,Table4.AmtXstb) THEN
            'Customer-Ofb8'
          WHEN 'Code-G27O' LIKE 'Country-bLPE' THEN
            'Address-EX87'
          WHEN SourceA.FieldB = Table3.CountrycRy2 THEN
            'Account-iidd'
          WHEN ENDS_WITH(Table2.CustomerqZGi,SourceE.FieldA) THEN
            'Country-n3lK'
          ELSE
            'Code-v3v1'
        END,
        IF('Amt-SwUo' LIKE Table4.AccountwzYY,SourceC.FieldC,'Account-XQXq'))
        AS Amtdd0Z,
      CASE
        WHEN ENDS_WITH('Amt-YZIX','Account-76JC') THEN
          IF(
            SourceA.FieldA NOT LIKE 'Account-EF6p', Table3.CountryLVJe,
            'Amt-zyTQ')
        WHEN 'Fee-9H9I' <> 'Address-QXsa' THEN
          IF(
            STARTS_WITH('Country-J8S9','Amt-fnjO'), Table3.Amt0UkG,
            Table2.AddressSaXB)
        ELSE
          IF('Country-9uj4' LIKE SourceE.FieldG,'Account-cSiO','Address-irKg')
      END
        AS AddressdMjQ,
      IF(
        STARTS_WITH('CCY-Fs9x',Table4.CountryNePa),
        CASE
          WHEN Table4.AmtXstb != SourceC.FieldB THEN
            Table3.AddressItuV
          WHEN 'Amt-BKdB' <> 'Account-g5wH' THEN
            Table3.Codez1OQ
          WHEN 'Customer-1SNj' LIKE 'Address-2mGP' THEN
            Table1.AccountSd1m
          ELSE
            'Country-1EFC'
        END, IF(
               STARTS_WITH(SourceC.FieldA,'Amt-5Z2S'), 'Account-jTGk',
               'Address-Qt37'))
        AS Addressjdme
    FROM
      testdataset.SourceA AS SourceA
      INNER JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceA.FieldD,SourceA.FieldC) AND
         ENDS_WITH(SourceA.FieldJ,SourceA.FieldF) AND
         STARTS_WITH('Code-AIgh','Customer-GQjT') OR
         STARTS_WITH(SourceA.FieldF,SourceB.FieldF))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH(SourceB.FieldB,SourceC.FieldA) AND
         SourceC.FieldI = SourceB.FieldH OR
         'Amt-xMbJ' NOT LIKE 'Country-QLkY' OR SourceA.FieldG != SourceA.FieldI)
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (SourceA.FieldD != SourceC.FieldH AND
         STARTS_WITH(SourceC.FieldB,SourceD.FieldD) OR
         STARTS_WITH('Fee-3Kea','CCY-nBjE') OR
         STARTS_WITH(SourceB.FieldH,'Account-CZXx'))
      INNER JOIN testdataset.SourceE AS SourceE ON
        (SourceA.FieldB = SourceB.FieldF AND
         SourceB.FieldB <> 'Address-E9Ht' AND
         ENDS_WITH(SourceB.FieldJ,'Country-33HF') OR
         ENDS_WITH(SourceC.FieldJ,SourceC.FieldD))
      RIGHT JOIN Table1 AS Table1 ON
        (SourceE.FieldG <> 'Code-hY4t' AND
         SourceC.FieldE NOT LIKE SourceC.FieldJ AND
         STARTS_WITH(SourceC.FieldH,SourceC.FieldB) AND
         'Code-wVHU' = SourceD.FieldD)
      RIGHT JOIN Table2 AS Table2 ON
        ('Customer-mdJo' != SourceC.FieldB AND
         ENDS_WITH(SourceD.FieldJ,SourceD.FieldD) OR
         ENDS_WITH('Amt-NWKM',SourceD.FieldI) AND
         SourceB.FieldF LIKE Table2.CustomerqZGi)
      INNER JOIN Table3 AS Table3 ON
        (STARTS_WITH(Table2.AccountmDS1,'Country-w2kP') OR
         STARTS_WITH('Code-JWNA','CCY-UdFe') AND
         'Country-ZRVC' = SourceA.FieldE AND Table2.Fee67Xh LIKE SourceC.FieldB)
      RIGHT JOIN Table4 AS Table4 ON
        ('Address-eRky' <> 'CCY-DMzo' AND 'Fee-Wjqk' != Table1.FeeNRHE OR
         ENDS_WITH(Table2.CountryGiw3,'CCY-sEw0') OR
         'Account-Frw2' != 'Fee-enJu')
    WHERE
      'Address-nTc3' NOT LIKE 'CCY-Y8K0' OR
      Table4.CountrywojM != SourceE.FieldI AND
      ENDS_WITH('Account-MMT3',Table4.Amt69Sr) OR
      'Fee-eBEk' = Table4.CountryAv9y AND
      'CCY-YIJf' NOT LIKE Table1.Feex0t9 AND ENDS_WITH('Amt-pfUG','CCY-KXd5') OR
      Table1.Customer3oUD NOT LIKE Table4.FeeD7R4 OR
      'Customer-rTK1' NOT LIKE 'Code-v5tC'
  )
SELECT
  IF(
    ENDS_WITH(Table4.CustomeriAlh,Table4.FeeWpYv),
    IF(
      STARTS_WITH(SourceD.FieldH,Table4.Fee8X1x), SourceC.FieldE,
      Table2.CustomerRv8H),
    IF('Address-KGN3' = 'Account-A97A',Table4.CountryNePa,SourceE.FieldD))
    AS AmtR86V,
  IF(
    STARTS_WITH(Table2.AddressSaXB,SourceA.FieldI),
    CASE
      WHEN STARTS_WITH(Table5.AccountkCVD,'Customer-d7pc') THEN
        'Code-QDzw'
      WHEN Table2.CodewHdY NOT LIKE SourceC.FieldB THEN
        Table3.Codez1OQ
      WHEN STARTS_WITH('Code-WOmj',Table4.CountryAv9y) THEN
        SourceE.FieldE
      WHEN 'Fee-XEAQ' LIKE SourceC.FieldB THEN
        SourceB.FieldA
      ELSE
        'Country-mwbe'
    END,
    CASE
      WHEN 'Fee-e05m' NOT LIKE 'Country-xmEl' THEN
        Table4.CCYwhT3
      WHEN Table1.CountrywfBt NOT LIKE Table1.CountrywfBt THEN
        'Address-XSIV'
      WHEN STARTS_WITH(SourceA.FieldG,'Customer-aXnK') THEN
        'Code-SgYZ'
      WHEN ENDS_WITH(SourceD.FieldI,'Country-TfZn') THEN
        SourceE.FieldJ
      WHEN STARTS_WITH(Table2.Amt6PIN,SourceC.FieldB) THEN
        Table3.CountrycRy2
      ELSE
        SourceA.FieldA
    END)
    AS AccountmNQo,
  IF(
    Table5.CCY38xb LIKE Table3.AccountvVoJ,
    IF(ENDS_WITH(Table4.CodeSPJh,Table3.Codez1OQ),'CCY-9q8E','CCY-EnZY'),
    IF(ENDS_WITH(Table3.AccountvVoJ,'Country-dUPq'),'Country-oWbS','CCY-ZajQ'))
    AS CCYXSkS,
  CASE
    WHEN ENDS_WITH(SourceD.FieldD,SourceD.FieldE) THEN
      IF(Table5.Address82qt != 'Fee-qQL8','Address-Nzeq',Table2.AddressxJst)
    WHEN 'Customer-VprX' NOT LIKE Table2.CCYp1y0 THEN
      CASE
        WHEN ENDS_WITH(SourceD.FieldH,'Account-uLu6') THEN
          Table5.CCY38xb
        WHEN SourceB.FieldI != 'Code-Shci' THEN
          'Country-YW2l'
        WHEN ENDS_WITH(SourceE.FieldD,Table3.Customerah3l) THEN
          SourceD.FieldB
        WHEN 'Country-dvQq' <> 'Address-feqZ' THEN
          Table2.AmtM3tT
        WHEN Table5.CCY7Hoo <> Table2.CustomerqZGi THEN
          SourceB.FieldI
        ELSE
          'Address-OxaF'
      END
    WHEN STARTS_WITH('Country-4BbJ','Customer-2Au0') THEN
      IF(SourceE.FieldH <> SourceD.FieldF,'Country-ymD6','Fee-sswE')
    ELSE
      IF(
        STARTS_WITH('Address-ETvk','Code-DDyy'), Table2.AddressSaXB,
        SourceC.FieldF)
  END
    AS CustomerWrlz,
  IF(
    ENDS_WITH('Code-tY5h','Amt-6Vsj'),
    IF('Account-DHj0' <> Table3.Amt0UkG,Table5.CCYDKu9,Table2.FeeJgkn),
    IF('Code-gqq1' NOT LIKE 'Fee-WEoE',Table5.FeeX4QK,'Country-gnz8'))
    AS CustomerKrwR,
  IF(
    ENDS_WITH(SourceB.FieldE,'Code-TY6m'),
    CASE
      WHEN 'Code-p5zo' LIKE SourceC.FieldB THEN
        'Customer-vyuY'
      WHEN 'Account-maPB' LIKE Table2.CCYDE77 THEN
        Table5.Fee2LBQ
      ELSE
        Table3.Amt0UkG
    END,
    IF(STARTS_WITH('Code-eFMD','CCY-69th'),SourceB.FieldI,Table4.AccountvOrS))
    AS FeerBIS,
  IF(
    ENDS_WITH(Table1.Customer3oUD,Table1.CCYDFbw),
    IF(SourceB.FieldG != 'Amt-w8tO','Fee-LtCk','Amt-je3p'),
    CASE
      WHEN STARTS_WITH(Table1.Amt3Ib2,'Country-CQAM') THEN
        Table5.CountryMAW7
      WHEN 'Fee-83LX' = 'CCY-0pb1' THEN
        'Code-RXyM'
      WHEN ENDS_WITH(Table2.AddressSaXB,'Fee-KGIc') THEN
        Table1.Customer2RaL
      WHEN 'Address-nEFa' = 'Account-ylsG' THEN
        'Customer-3wGx'
      ELSE
        SourceD.FieldJ
    END)
    AS AddressoeLk,
  IF(
    Table4.Fee8X1x NOT LIKE 'Code-s9jq',
    IF(STARTS_WITH('Country-GjNY','Fee-3gsQ'),'Customer-Rs8Z','Country-uOly'),
    IF(ENDS_WITH(Table4.CCYwhT3,'Customer-n32I'),Table3.Amt3ZH8,'Country-hi3o'))
    AS AmtzEYq,
  IF(
    STARTS_WITH(Table1.Code6Ols,Table3.CountryEVqL),
    IF('Customer-YesA' <> Table4.FeeD7R4,Table5.Amtdd0Z,Table1.Customer3oUD),
    IF('Account-DjeN' NOT LIKE 'Amt-wdOT','Fee-LCop','Amt-d2Xt'))
    AS AccountfEJK,
  IF(
    STARTS_WITH('CCY-XFCF','Amt-AwGp'),
    IF(
      STARTS_WITH(Table5.AccountkCVD,Table4.CodeCN1H), 'CCY-W0LT',
      Table1.CountrywfBt),
    IF(ENDS_WITH('Account-zPa7','Address-wpBv'),'Code-0d8f',SourceB.FieldF))
    AS AccountqcVc,
  IF(
    'Account-IdfC' <> 'Code-rjHx',
    IF(
      STARTS_WITH('Address-oghU',Table4.CustomeriAlh), 'Code-2vTt',
      SourceA.FieldA),
    CASE
      WHEN ENDS_WITH(SourceD.FieldI,Table3.AccountYmqE) THEN
        'Customer-Zcdk'
      WHEN STARTS_WITH('Country-IlQU','Customer-5rce') THEN
        'Amt-RR54'
      WHEN ENDS_WITH('Code-yLdc',Table4.CCYp1KW) THEN
        'Country-NnS3'
      WHEN STARTS_WITH(Table1.Amt3Ib2,'Customer-IE5v') THEN
        'Address-yuz0'
      ELSE
        Table4.FeeD7R4
    END)
    AS CountryV1io,
  CASE
    WHEN STARTS_WITH('Address-Dm3i','Account-KsdC') THEN
      IF(STARTS_WITH('Amt-UKnx',SourceB.FieldD),'Amt-SJsI',Table3.AccountYmqE)
    WHEN STARTS_WITH(SourceD.FieldI,SourceD.FieldB) THEN
      IF(Table5.CountryMAW7 LIKE 'Code-7bSX','Code-pEt9','Code-f0sA')
    ELSE
      CASE
        WHEN 'Country-vEZ8' = 'Fee-J3Cr' THEN
          SourceC.FieldF
        WHEN STARTS_WITH('CCY-swym','Account-b8yp') THEN
          Table4.FeeWpYv
        WHEN ENDS_WITH('Code-bHUC','Address-28Da') THEN
          SourceA.FieldE
        WHEN Table4.CCYw7wt <> 'Customer-oJ7G' THEN
          'Amt-LTKS'
        ELSE
          'Amt-zD4D'
      END
  END
    AS CustomerWpfX,
  IF(
    Table1.Amt3Ib2 NOT LIKE Table5.Addressji9M,
    CASE
      WHEN 'Code-QggD' <> 'Code-aife' THEN
        'CCY-FGh3'
      WHEN 'CCY-zvVn' = 'Account-2a9j' THEN
        'Code-HbTO'
      WHEN Table3.AddressReUj != 'Address-bU5l' THEN
        SourceD.FieldB
      ELSE
        Table4.AmtYzGk
    END, IF(ENDS_WITH('CCY-6DyA','Code-sl0a'),Table4.CCYp1KW,SourceB.FieldI))
    AS AccountKwSP,
  IF(
    'Code-0i5a' LIKE 'Fee-5QRU',
    IF(SourceA.FieldB LIKE 'Amt-N3Br','Country-sagi',SourceC.FieldD),
    IF(Table3.AccountvVoJ <> 'Code-ERDK',SourceD.FieldG,Table4.Fee8X1x))
    AS Codeuhr9,
  CASE
    WHEN ENDS_WITH('Customer-TstR',Table4.AccountwzYY) THEN
      IF(
        'Account-yNGH' NOT LIKE Table1.CustomerybpW, Table5.AccountZZVy,
        'Fee-Glmt')
    WHEN 'Account-i9B1' NOT LIKE 'Code-QEAu' THEN
      IF(
        Table3.AccountvVoJ NOT LIKE 'Address-RZTB', Table3.AccountYmqE,
        'Country-cDCK')
    WHEN STARTS_WITH(Table2.CodewHdY,'Address-FpXy') THEN
      CASE
        WHEN STARTS_WITH('Address-d4AM',Table3.AccountYmqE) THEN
          Table2.AddressxJst
        WHEN ENDS_WITH(Table3.Amt3ZH8,Table5.CCYwP2S) THEN
          SourceA.FieldE
        WHEN STARTS_WITH(Table4.CountryAv9y,Table5.Amtdd0Z) THEN
          SourceE.FieldG
        WHEN ENDS_WITH(Table1.AccountSd1m,'Customer-HFF0') THEN
          Table1.CCYDFbw
        ELSE
          Table4.AccountvOrS
      END
    ELSE
      IF(ENDS_WITH('Code-YkEw',Table3.CodeZX2t),'Address-FJGV','Country-7OPI')
  END
    AS Addressrgjd
FROM
  Table3 AS Table3
  INNER JOIN testdataset.SourceA AS SourceA ON
    (SourceA.FieldH = Table3.AccountvVoJ OR
     Table3.CodeZX2t LIKE 'Customer-UsLN' AND
     ENDS_WITH('Code-5omJ','Account-mSZs') OR
     ENDS_WITH(Table3.CodeZX2t,'Account-RoVf'))
  INNER JOIN testdataset.SourceB AS SourceB ON
    (STARTS_WITH('CCY-eJ0X',SourceA.FieldI) AND
     SourceA.FieldB <> Table3.Customerah3l OR
     Table3.AccountYmqE NOT LIKE SourceB.FieldA OR
     'Customer-tqC2' LIKE 'CCY-UYlj')
  INNER JOIN testdataset.SourceC AS SourceC ON
    (STARTS_WITH('Address-wAJr','Account-kjIq') AND
     ENDS_WITH(Table3.CountrycRy2,'Account-0Upb') AND
     STARTS_WITH('Country-ceBX','Address-BN0n') AND
     'Account-w6gl' = SourceA.FieldI)
  RIGHT JOIN testdataset.SourceD AS SourceD ON
    (STARTS_WITH(SourceA.FieldB,SourceB.FieldF) OR
     'Fee-RHdA' != Table3.AccountvVoJ OR
     'Fee-XXOJ' <> SourceD.FieldG OR ENDS_WITH('Address-3zEG','Customer-aVh3'))
  INNER JOIN testdataset.SourceE AS SourceE ON
    (SourceC.FieldJ <> 'Account-eQa2' AND SourceC.FieldF = Table3.Addressc4Sp OR
     STARTS_WITH(SourceA.FieldC,'Account-xgmE') OR
     STARTS_WITH('Account-LnA2','Code-0fyd'))
  RIGHT JOIN Table1 AS Table1 ON
    ('Customer-JWPc' <> 'Customer-pzrC' AND
     ENDS_WITH('Country-zSIq','Amt-GrWm') AND
     'Account-3CHk' = SourceE.FieldG AND ENDS_WITH('Code-2Q4G','Fee-LrPa'))
  INNER JOIN Table2 AS Table2 ON
    ('Fee-NuZ6' NOT LIKE 'Customer-nF7Q' OR 'Address-IRRa' != 'Fee-Xaab' OR
     ENDS_WITH(SourceC.FieldE,SourceD.FieldF) OR
     STARTS_WITH(SourceD.FieldF,'Address-tofG'))
  LEFT JOIN Table4 AS Table4 ON
    (Table3.AddressItuV NOT LIKE Table4.CountrywojM OR
     ENDS_WITH('Address-6Val',SourceB.FieldI) OR
     Table4.FeeD7R4 NOT LIKE Table3.CountryEVqL AND
     Table4.AmtYzGk LIKE 'Fee-U0g4')
  LEFT JOIN Table5 AS Table5 ON
    (Table3.Addressc4Sp LIKE Table4.CountryAv9y OR
     STARTS_WITH('Amt-LjY4','Fee-Iwhp') OR
     SourceC.FieldE = 'Country-UchZ' AND
     Table1.CustomerybpW LIKE 'Account-XnDa')
WHERE
  Table2.CCYDE77 != Table4.FeeWpYv OR STARTS_WITH(SourceA.FieldC,'CCY-24Jc') OR
  'Customer-6EtB' = 'Address-S2LR' OR
  STARTS_WITH('Address-jtOg',SourceE.FieldE) AND
  ENDS_WITH(SourceC.FieldJ,Table2.AmtM3tT) AND
  'Address-aPYR' NOT LIKE Table3.Amt0UkG OR
  'CCY-YFOZ' <> 'CCY-LgtQ' OR Table5.AccountkCVD = 'Country-M1QT'