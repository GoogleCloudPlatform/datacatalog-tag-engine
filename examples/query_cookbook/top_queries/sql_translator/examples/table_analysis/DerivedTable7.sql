CREATE OR REPLACE TABLE testdataset.DerivedTable7 AS
WITH
  Table31 AS (
    SELECT
      IF(
        ENDS_WITH('Amt-AOAO','CCY-sund'),
        IF(
          'CCY-z9lZ' NOT LIKE 'Customer-ixZq', DerivedTable1.FeerBIS,
          'CCY-fVnt'),
        IF(
          STARTS_WITH(DerivedTable3.CountryoFFA,DerivedTable2.AmtmorU),
          'Country-Bnp7', 'Code-oCsM'))
        AS CCY4op0,
      IF(
        ENDS_WITH(DerivedTable3.CodeVHWz,DerivedTable4.FeesrBh),
        IF(
          'Code-AFcS' NOT LIKE DerivedTable2.CodeDmva,
          DerivedTable3.CustomerLP0M, DerivedTable3.CodeUeLq),
        CASE
          WHEN STARTS_WITH('CCY-SPeV','Customer-nkJ7') THEN
            'Customer-9pQM'
          WHEN 'Code-DmEb' != SourceB.FieldJ THEN
            DerivedTable3.FeeqkjX
          WHEN STARTS_WITH(SourceB.FieldJ,SourceE.FieldE) THEN
            'CCY-0k9r'
          WHEN 'Amt-IS2u' != 'Amt-bjOX' THEN
            SourceA.FieldF
          ELSE
            DerivedTable4.Account1SJF
        END)
        AS AccountOIqn,
      IF(
        'Customer-DYQQ' LIKE SourceE.FieldC,
        IF('Fee-Mke7' NOT LIKE 'Customer-1rYR','Address-w97n','Code-FyeO'),
        IF(
          STARTS_WITH(SourceC.FieldG,'Fee-PG3G'), SourceE.FieldB,
          'Country-frr4'))
        AS AddressFu86,
      IF(
        STARTS_WITH('Country-zlk7',SourceA.FieldG),
        IF(
          'Code-zZ6Q' != 'Code-rIcu', DerivedTable2.AccountctCy,
          DerivedTable3.CountryoFFA),
        IF(
          STARTS_WITH(DerivedTable4.AddressWvnP,SourceB.FieldF), 'Amt-73cv',
          DerivedTable6.AddressPIbO))
        AS CCYB8lD,
      IF(
        STARTS_WITH('Fee-nsGK','Code-ZY41'),
        IF(
          STARTS_WITH(DerivedTable6.Fee8TJM,'Customer-ufxs'),
          DerivedTable2.Address7oYI, 'Code-uwys'),
        IF(
          'Address-C6IV' != DerivedTable2.AccountrJZf, 'Fee-wIDS',
          'Account-6dqZ'))
        AS Address9GCS,
      IF(
        DerivedTable2.CodeDmva = 'CCY-nMyF',
        CASE
          WHEN STARTS_WITH(DerivedTable4.CodeXHEv,'Fee-ZQ3E') THEN
            DerivedTable6.CustomerCZg3
          WHEN STARTS_WITH(DerivedTable6.FeeN99q,DerivedTable6.AddressYFBr) THEN
            'Fee-W2Xk'
          ELSE
            SourceA.FieldD
        END, IF(
               STARTS_WITH(DerivedTable1.AmtR86V,DerivedTable2.Account61kv),
               'Address-rM3x', 'Code-QxhJ'))
        AS CodexXjy,
      CASE
        WHEN STARTS_WITH('Amt-dspu',SourceC.FieldC) THEN
          IF(
            ENDS_WITH('Code-fS8H',DerivedTable3.Feeg8kl),
            DerivedTable4.Addressm87D, SourceC.FieldD)
        WHEN SourceB.FieldE LIKE DerivedTable6.CustomerxLhn THEN
          IF(
            ENDS_WITH(DerivedTable3.CountryoFFA,SourceC.FieldJ),
            DerivedTable3.CustomerLP0M, 'Code-bb76')
        WHEN STARTS_WITH('Amt-cSAf','Address-U3o0') THEN
          IF(
            ENDS_WITH(DerivedTable3.Feeg8kl,DerivedTable4.Account1SJF),
            'Customer-gg0p', 'Customer-n0zD')
        WHEN STARTS_WITH(SourceA.FieldH,SourceB.FieldD) THEN
          IF(
            DerivedTable1.AccountKwSP NOT LIKE DerivedTable3.AccountViga,
            DerivedTable6.CustomerCZg3, 'CCY-ylDK')
        WHEN SourceB.FieldC LIKE SourceD.FieldI THEN
          CASE
            WHEN ENDS_WITH(SourceB.FieldI,'Fee-DIeQ') THEN
              SourceE.FieldH
            WHEN 'Customer-IsNw' <> 'Amt-jrYl' THEN
              'Customer-mstI'
            WHEN ENDS_WITH('CCY-dLoG','Country-2dlF') THEN
              DerivedTable1.CountryV1io
            ELSE
              'Customer-QRSV'
          END
        ELSE
          IF('Address-gV6n' = 'Code-Loaj','Country-2k3n',SourceA.FieldG)
      END
        AS Code9KZI,
      IF(
        STARTS_WITH(DerivedTable3.Code3Xip,SourceC.FieldD),
        CASE
          WHEN STARTS_WITH('Amt-lV5w',SourceB.FieldB) THEN
            'Fee-Tikt'
          WHEN ENDS_WITH('Amt-1wq5','Customer-5Zsn') THEN
            DerivedTable4.AccounteutQ
          ELSE
            'Country-ome9'
        END, IF(
               STARTS_WITH('Address-oBpT','Account-VSMz'), 'Address-6eHT',
               SourceE.FieldE))
        AS CountrymFgA,
      IF(
        SourceB.FieldI != 'Country-2NbX',
        CASE
          WHEN DerivedTable3.CustomereBLI != 'Account-crBK' THEN
            'Fee-prb8'
          WHEN ENDS_WITH('Customer-IB6X','Code-Ny5c') THEN
            SourceB.FieldG
          WHEN SourceA.FieldG LIKE 'Account-oEp3' THEN
            SourceA.FieldF
          ELSE
            'Fee-CHry'
        END, IF(
               STARTS_WITH('Code-jsfI','Customer-60lA'),
               DerivedTable1.CustomerWpfX, 'Account-Nkd9'))
        AS Amtm9SX,
      IF(
        ENDS_WITH(DerivedTable3.CountrygPvi,DerivedTable6.AddressPIbO),
        IF(
          ENDS_WITH('Customer-wUrJ','CCY-uJY7'), DerivedTable1.Codeuhr9,
          'CCY-zt5o'), IF(
                         ENDS_WITH(DerivedTable6.AmtW2w8,'CCY-BSPL'),
                         'Country-8e6s', 'Account-Wgah'))
        AS AddressQZsG,
      IF(
        STARTS_WITH(DerivedTable1.Addressrgjd,SourceA.FieldB),
        IF(SourceB.FieldH LIKE DerivedTable6.FeeFMCK,'Fee-wqdp','CCY-byXn'),
        IF(ENDS_WITH('Amt-DANl','Country-lwDp'),'Fee-vPX5','Customer-X8jF'))
        AS CustomerpCuq,
      IF(
        STARTS_WITH(SourceA.FieldG,'Code-4kTk'),
        CASE
          WHEN STARTS_WITH(DerivedTable4.Addressm87D,SourceC.FieldB) THEN
            'Customer-GkiR'
          WHEN 'Account-v42R' <> DerivedTable2.Address7oYI THEN
            'CCY-rwTv'
          ELSE
            SourceD.FieldD
        END,
        CASE
          WHEN 'Code-RcgQ' = 'Fee-YRad' THEN
            DerivedTable6.Address54VO
          WHEN STARTS_WITH(DerivedTable3.CustomerULyd,'Fee-umT0') THEN
            'Code-WaRn'
          WHEN 'Account-jbpy' <> 'Account-TZGt' THEN
            'Account-mSPs'
          WHEN 'CCY-x836' = SourceA.FieldB THEN
            SourceC.FieldH
          ELSE
            DerivedTable3.CountryoFFA
        END)
        AS Countryd8ig
    FROM
      testdataset.DerivedTable1 AS DerivedTable1
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH('Address-15ND',DerivedTable1.CCYXSkS) AND
         STARTS_WITH(SourceA.FieldG,SourceA.FieldH) AND
         SourceA.FieldF NOT LIKE DerivedTable1.AddressoeLk OR
         'Account-Vx64' LIKE 'CCY-XcxN')
      INNER JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH(DerivedTable1.AccountfEJK,'Customer-CY9m') AND
         ENDS_WITH('Customer-TNwS','Customer-3ZdT') OR
         'CCY-qRs7' = SourceA.FieldI AND 'Amt-IvYs' = DerivedTable1.CCYXSkS)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        ('Amt-2tM2' NOT LIKE 'Amt-rb9S' OR
         ENDS_WITH('Account-rLUu',SourceC.FieldC) AND
         SourceB.FieldF LIKE DerivedTable1.CustomerKrwR OR
         ENDS_WITH(DerivedTable1.AmtzEYq,SourceC.FieldD))
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        ('Country-tgYl' <> SourceB.FieldI OR
         ENDS_WITH(SourceD.FieldG,SourceA.FieldI) OR
         ENDS_WITH(SourceA.FieldG,'Customer-2Qua') OR
         STARTS_WITH('Customer-q9t0',SourceD.FieldI))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (SourceB.FieldB NOT LIKE DerivedTable1.Codeuhr9 OR
         'Fee-0GFh' <> 'CCY-t9vA' OR
         'CCY-m785' LIKE 'Customer-P0Ff' AND
         STARTS_WITH('Fee-0oDl',SourceE.FieldJ))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Amt-VKBf' NOT LIKE DerivedTable2.AddresscM17 AND
         ENDS_WITH(SourceB.FieldA,SourceE.FieldJ) AND
         'Amt-Kc45' != SourceB.FieldH AND
         STARTS_WITH('CCY-cAAr','Customer-iENX'))
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH(SourceB.FieldI,SourceC.FieldJ) AND
         'Country-NxAF' <> DerivedTable3.CustomereBLI AND
         STARTS_WITH('Amt-SwAW','Code-2HAP') OR
         STARTS_WITH(SourceE.FieldI,SourceE.FieldC))
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH('CCY-vj4W',SourceE.FieldF) OR
         ENDS_WITH(DerivedTable3.Feeg8kl,'Country-KU4l') AND
         STARTS_WITH('Address-PRRI','Code-Znje') OR
         SourceE.FieldB NOT LIKE SourceB.FieldJ)
      INNER JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (ENDS_WITH('Address-X6oc','Address-9W4k') OR
         STARTS_WITH('Country-jy48',SourceE.FieldG) AND
         STARTS_WITH('Address-JmZq',DerivedTable6.CodeY4Yb) OR
         'CCY-0Uvb' != 'Country-75R5')
    WHERE
      SourceC.FieldE != DerivedTable6.Addressp4Y0 OR
      ENDS_WITH(DerivedTable1.AccountKwSP,DerivedTable3.FeeqkjX) AND
      ENDS_WITH('Account-2zW6','Amt-Swxp') OR
      'Account-EMNy' <> 'Country-9wrV' AND
      ENDS_WITH(SourceA.FieldI,DerivedTable1.CustomerWpfX) OR
      'Country-3OzH' LIKE 'Country-fmL1' AND
      STARTS_WITH(SourceB.FieldB,'Account-n350') AND
      'Amt-fvMM' LIKE DerivedTable4.Addressm87D
  ),
  Table32 AS (
    SELECT
      IF(
        ENDS_WITH('Customer-2zGA',DerivedTable4.Country5f1u),
        IF(
          ENDS_WITH(DerivedTable4.AccounteutQ,'Account-9zSj'),
          DerivedTable5.AddressV4AX, DerivedTable1.AmtzEYq),
        IF(
          ENDS_WITH('Amt-7sEk',DerivedTable2.Amt885g), 'Address-gl0A',
          DerivedTable3.FeepQGh))
        AS Feekyu2,
      CASE
        WHEN Table31.CCYB8lD NOT LIKE DerivedTable6.AddressPIbO THEN
          IF(
            STARTS_WITH(SourceD.FieldJ,'Fee-4wMZ'), SourceC.FieldA,
            'Country-9QO5')
        WHEN STARTS_WITH(DerivedTable2.AccountrJZf,DerivedTable6.CodeY4Yb) THEN
          CASE
            WHEN
              STARTS_WITH(DerivedTable3.Codeaqvh,DerivedTable6.AddressD7PZ)
            THEN
              'Fee-5Cl8'
            WHEN
              'Customer-W1Ah' != 'Customer-xH3K'
            THEN
              DerivedTable1.AccountfEJK
            ELSE
              'Country-hT7V'
          END
        ELSE
          IF(
            'Address-wLeb' = DerivedTable6.AddressWejH, DerivedTable6.Fee8TJM,
            SourceC.FieldG)
      END
        AS CodeCEQT,
      CASE
        WHEN DerivedTable2.AccounttVM1 = 'Amt-qRbE' THEN
          IF(
            DerivedTable1.AccountKwSP <> DerivedTable2.Amt885g, 'Address-3C1L',
            'Customer-fQhi')
        WHEN STARTS_WITH(SourceC.FieldJ,DerivedTable4.AccountMw3l) THEN
          IF(
            STARTS_WITH(DerivedTable6.AddressYFBr,'Amt-U6XN'), 'Amt-i85J',
            'Amt-w81O')
        WHEN Table31.Address9GCS != 'Code-m8Pv' THEN
          IF(DerivedTable4.FeesrBh <> 'Code-IzLY',SourceD.FieldA,'Code-v7cm')
        WHEN ENDS_WITH(SourceA.FieldG,'Address-NnRQ') THEN
          IF(
            'Address-nGbr' NOT LIKE DerivedTable3.Account4sxv, SourceB.FieldJ,
            DerivedTable2.AccounttVM1)
        WHEN 'Country-cBaT' <> 'Customer-plxy' THEN
          CASE
            WHEN 'Account-x7xY' NOT LIKE SourceD.FieldD THEN
              'Address-eEAQ'
            WHEN DerivedTable6.Address54VO = SourceC.FieldJ THEN
              SourceC.FieldC
            WHEN ENDS_WITH('Fee-goSw',DerivedTable2.AccountctCy) THEN
              DerivedTable1.Addressrgjd
            ELSE
              DerivedTable4.Country5f1u
          END
        ELSE
          CASE
            WHEN
              ENDS_WITH(DerivedTable4.Country5f1u,DerivedTable4.CodeXHEv)
            THEN
              'Address-pJey'
            WHEN
              ENDS_WITH(DerivedTable2.CountrywaWi,'Country-e5rZ')
            THEN
              'CCY-FIQD'
            WHEN
              STARTS_WITH(SourceA.FieldF,DerivedTable1.AmtR86V)
            THEN
              'Customer-fZPR'
            WHEN
              STARTS_WITH('Code-XjRM',DerivedTable1.CustomerWrlz)
            THEN
              'Address-T2ZT'
            WHEN
              DerivedTable1.AmtzEYq <> 'Country-ScIS'
            THEN
              DerivedTable2.Account61kv
            ELSE
              'Account-1dLJ'
          END
      END
        AS FeeDStf,
      IF(
        STARTS_WITH('Address-JhGG',DerivedTable4.Country5f1u),
        CASE
          WHEN ENDS_WITH('Customer-rP7V','CCY-VHJN') THEN
            SourceB.FieldF
          WHEN DerivedTable2.AccountctCy != 'Fee-5t8D' THEN
            'Country-72x5'
          WHEN ENDS_WITH('CCY-P1TL',SourceA.FieldG) THEN
            'Customer-wFng'
          WHEN ENDS_WITH('Fee-3NiE',DerivedTable6.AddressPIbO) THEN
            DerivedTable5.Code7INp
          WHEN STARTS_WITH('Customer-6sYr',DerivedTable3.AccountViga) THEN
            SourceD.FieldF
          ELSE
            SourceD.FieldH
        END,
        CASE
          WHEN STARTS_WITH('Country-3ryi',SourceB.FieldB) THEN
            'Customer-fUWV'
          WHEN DerivedTable6.AddressWejH LIKE DerivedTable6.AddressgQNB THEN
            'Code-rZpV'
          WHEN DerivedTable1.CCYXSkS LIKE DerivedTable4.AccountMw3l THEN
            'Address-pm71'
          ELSE
            DerivedTable2.CodeDmva
        END)
        AS AddressdrCR,
      IF(
        STARTS_WITH('Address-AaBC','Code-4utT'),
        IF(
          ENDS_WITH('Country-7MZY',DerivedTable2.AccounttVM1),
          DerivedTable3.FeeD0Po, 'Address-f5wj'),
        IF(
          ENDS_WITH('Code-VZZA','Customer-NnNA'), DerivedTable1.AccountmNQo,
          'Country-c8Lb'))
        AS AddressH6Yq,
      IF(
        STARTS_WITH('CCY-iSvM',DerivedTable2.Address5RWP),
        IF(
          STARTS_WITH(DerivedTable6.CustomerCZg3,'Fee-TyDm'), 'Country-ueWr',
          DerivedTable3.FeeCUde),
        IF('Code-8xTX' = 'Country-Vxqi',SourceB.FieldJ,'Fee-XbqL'))
        AS AccountuH2c,
      IF(
        STARTS_WITH(DerivedTable3.FeepQGh,'Customer-JTzn'),
        IF(
          SourceD.FieldA NOT LIKE DerivedTable5.CCYQK2Q,
          DerivedTable5.CountrynpDj, DerivedTable2.AccountrJZf),
        IF(
          ENDS_WITH('Address-WXGM','Amt-ot4Q'), DerivedTable6.AddressYFBr,
          DerivedTable4.FeezLpC))
        AS Customer6IcH,
      IF(
        STARTS_WITH('CCY-dI7e',DerivedTable3.FeeqkjX),
        IF(
          'Account-wNlB' <> DerivedTable2.CountrywaWi, 'CCY-TIAB',
          SourceD.FieldA),
        CASE
          WHEN STARTS_WITH(DerivedTable3.FeepQGh,'Account-ii4T') THEN
            DerivedTable4.Country5f1u
          WHEN STARTS_WITH(DerivedTable4.FeezLpC,DerivedTable4.FeezLpC) THEN
            'Address-qYfe'
          WHEN 'Amt-gjC6' = DerivedTable5.CodePGRC THEN
            'Fee-lnbA'
          ELSE
            DerivedTable5.CCYVD4h
        END)
        AS Amt91NZ,
      IF(
        DerivedTable6.CCYp8wn != 'Code-MUYM',
        IF(SourceA.FieldJ <> 'Code-QYR1','Amt-IXwA',SourceC.FieldJ),
        IF(SourceC.FieldC NOT LIKE 'Account-7fAY','Customer-WxIk','Fee-Y6al'))
        AS CustomeryLU9,
      IF(
        SourceA.FieldD <> SourceC.FieldC,
        IF(
          ENDS_WITH(DerivedTable4.Addressm87D,'Customer-5EZW'), 'CCY-lWZH',
          SourceB.FieldE),
        IF(
          DerivedTable3.CustomereBLI NOT LIKE DerivedTable2.Address5RWP,
          'Fee-kcDS', SourceB.FieldI))
        AS CustomeriwOk,
      CASE
        WHEN ENDS_WITH('Account-0cFr',SourceC.FieldH) THEN
          IF(ENDS_WITH('Amt-gzOh','Code-k7Xj'),'Account-XJj9','Address-H9xr')
        WHEN 'Code-O4Ka' LIKE DerivedTable2.CodeDmva THEN
          IF(
            ENDS_WITH(DerivedTable6.AddressPpXC,'Amt-zQ3g'), 'CCY-P4Fw',
            'CCY-UCud')
        WHEN 'Amt-kCkj' NOT LIKE DerivedTable1.AccountKwSP THEN
          CASE
            WHEN DerivedTable2.AccounttVM1 NOT LIKE 'Account-n79x' THEN
              'Account-s6kO'
            WHEN ENDS_WITH(DerivedTable1.CountryV1io,'Country-oQhC') THEN
              'CCY-5hzF'
            ELSE
              DerivedTable4.Country5f1u
          END
        WHEN ENDS_WITH('Amt-T7zV','Account-5imf') THEN
          IF(
            'Country-Xceq' LIKE 'CCY-U4w5', SourceB.FieldF,
            DerivedTable6.AddressWejH)
        WHEN ENDS_WITH('Country-19Ng',SourceC.FieldI) THEN
          IF(
            SourceB.FieldG LIKE 'Fee-ZC2O', DerivedTable4.FeesrBh,
            SourceD.FieldH)
        ELSE
          IF(
            'Address-9jkn' = DerivedTable5.AddressV4AX, SourceC.FieldJ,
            'Country-dbP6')
      END
        AS Accountk062,
      CASE
        WHEN SourceC.FieldG <> 'Address-qJ2o' THEN
          IF('Customer-qess' = 'CCY-dGtZ','Account-g1Ci',DerivedTable3.Codeaqvh)
        WHEN STARTS_WITH('Code-y0vQ','Account-xjRo') THEN
          IF(
            ENDS_WITH(DerivedTable2.Address7oYI,'CCY-MwY6'), 'Account-Q25y',
            'Customer-Frny')
        ELSE
          IF(
            DerivedTable3.CodeVHWz = 'CCY-PZ1U', SourceA.FieldH,
            DerivedTable4.CCYY6No)
      END
        AS AccountNiCU,
      IF(
        ENDS_WITH('Account-RXUE',SourceA.FieldA),
        IF(
          DerivedTable5.Code7INp LIKE 'Fee-HSZA', 'Code-yIAL',
          DerivedTable3.AccountViga),
        IF(
          SourceB.FieldH LIKE DerivedTable3.FeeF6P4, 'Fee-n5Jp',
          Table31.AddressQZsG))
        AS FeebSlR,
      IF(
        DerivedTable4.Country5f1u = DerivedTable2.Amtp4ee,
        IF('Account-TW1P' != DerivedTable1.AmtzEYq,'Fee-kDCY','Customer-t6q6'),
        IF('CCY-nyfP' = 'CCY-oycn','Fee-RRyA',DerivedTable3.CustomerULyd))
        AS CCYPZmI,
      IF(
        STARTS_WITH(DerivedTable3.CountrygMB4,'Address-YTNN'),
        CASE
          WHEN STARTS_WITH(SourceB.FieldD,DerivedTable5.CCYVD4h) THEN
            DerivedTable4.Country8VtM
          WHEN SourceC.FieldD <> SourceD.FieldD THEN
            SourceA.FieldE
          WHEN ENDS_WITH(SourceB.FieldB,DerivedTable3.AccountViga) THEN
            SourceB.FieldE
          WHEN 'Amt-iZYM' LIKE DerivedTable4.Country8VtM THEN
            'CCY-nl9a'
          WHEN 'Customer-4PGW' <> DerivedTable3.CustomerULyd THEN
            'Fee-6GxU'
          ELSE
            'Code-aulE'
        END, IF(
               STARTS_WITH('Amt-tC9Y','Address-rbaM'), DerivedTable6.AmtCCH1,
               'Customer-6bRT'))
        AS AccountApPl
    FROM
      testdataset.SourceC AS SourceC
      LEFT JOIN testdataset.SourceA AS SourceA ON
        ('Address-2yZd' <> SourceC.FieldB OR 'Amt-hZa2' LIKE SourceC.FieldB OR
         SourceC.FieldG <> 'Amt-1hUb' OR 'Customer-3fLd' <> 'Country-7MZG')
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Amt-TQPH',SourceC.FieldD) OR
         ENDS_WITH('Customer-gqE3','Address-d4Ly') OR
         ENDS_WITH(SourceB.FieldA,'Country-MbGf') AND
         STARTS_WITH(SourceA.FieldC,SourceC.FieldG))
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (SourceA.FieldA <> 'Amt-HV9f' AND
         STARTS_WITH(SourceC.FieldG,SourceB.FieldC) OR
         STARTS_WITH('Amt-VPm0','Fee-9YSm') OR
         SourceC.FieldI LIKE 'Account-sC7D')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (DerivedTable1.AccountmNQo != SourceA.FieldG OR
         'Fee-ZzI2' = SourceC.FieldC AND
         SourceA.FieldH != 'Customer-hpdd' AND
         DerivedTable1.FeerBIS <> 'Code-gk1D')
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (SourceB.FieldA <> DerivedTable2.AccounttVM1 AND
         'Code-YwFc' NOT LIKE 'Code-Eus7' OR
         STARTS_WITH('Code-Ehfd','CCY-7eMZ') OR
         ENDS_WITH(DerivedTable1.CCYXSkS,DerivedTable1.Codeuhr9))
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (ENDS_WITH('Address-wVcM',SourceC.FieldA) AND
         SourceD.FieldC LIKE SourceB.FieldJ OR
         STARTS_WITH('Customer-XXNf','CCY-buzn') AND
         'Country-lTvd' <> 'Amt-rog8')
      LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH('Address-n95h','Code-arFy') AND
         DerivedTable3.FeeCUde != SourceC.FieldH AND
         ENDS_WITH(DerivedTable1.Codeuhr9,SourceB.FieldC) OR
         SourceA.FieldF LIKE 'Fee-Z2Nb')
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        ('CCY-sFqO' <> 'Customer-qGoZ' OR 'Address-3nvL' <> SourceC.FieldA OR
         ENDS_WITH('Account-CjCz',DerivedTable4.FeexvTE) OR
         DerivedTable2.AmtmorU != 'Fee-FLxN')
      RIGHT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (SourceB.FieldG <> 'Fee-FAyf' AND STARTS_WITH('Fee-Ciw0','CCY-PglU') OR
         'Amt-Zkl8' != DerivedTable4.Addressm87D AND
         STARTS_WITH('Code-3hZ8',SourceD.FieldG))
      RIGHT JOIN Table31 AS Table31 ON
        (DerivedTable1.AddressoeLk != DerivedTable1.Addressrgjd OR
         ENDS_WITH('CCY-VBIU','Code-5ERn') AND
         ENDS_WITH(DerivedTable4.Account1SJF,'Account-L9JW') OR
         'Code-xved' LIKE 'Account-4c8W')
    WHERE
      STARTS_WITH('Customer-Nyr2',SourceA.FieldE) OR
      STARTS_WITH('Code-J56a',SourceA.FieldD) AND
      ENDS_WITH(DerivedTable6.Fee8TJM,DerivedTable1.CountryV1io) AND
      SourceA.FieldD != Table31.AddressFu86 OR
      DerivedTable3.CodeUeLq != 'Address-8T6F' OR
      DerivedTable5.CCYVD4h = 'CCY-uGEl' AND
      'Account-W0Z0' NOT LIKE DerivedTable1.CCYXSkS AND
      ENDS_WITH(DerivedTable3.FeeD0Po,'Amt-GTXM')
  ),
  Table33 AS (
    SELECT
      IF(
        'Account-f6k3' <> Table31.CountrymFgA,
        IF(
          STARTS_WITH(SourceE.FieldE,Table32.AccountNiCU), Table32.CodeCEQT,
          SourceC.FieldI),
        CASE
          WHEN SourceE.FieldH NOT LIKE 'Code-GMBM' THEN
            DerivedTable6.AddressPIbO
          WHEN Table32.CustomeriwOk <> 'Fee-Uy5d' THEN
            'Account-VN3g'
          WHEN DerivedTable3.Account4sxv != DerivedTable1.AmtzEYq THEN
            'Country-QqJN'
          WHEN SourceE.FieldA = 'Account-YzHU' THEN
            'Customer-oc0Q'
          ELSE
            DerivedTable1.CountryV1io
        END)
        AS CustomerOgOO,
      CASE
        WHEN ENDS_WITH(Table31.Countryd8ig,'CCY-B84p') THEN
          IF(
            'CCY-0n6N' NOT LIKE DerivedTable6.CodeY4Yb,
            DerivedTable6.AddressPIbO, 'Fee-qYSt')
        WHEN 'Amt-2hqA' != 'Amt-VXt0' THEN
          IF(ENDS_WITH('Amt-bC2L','Country-W6xg'),SourceE.FieldG,SourceC.FieldH)
        WHEN DerivedTable3.CodeVHWz = 'Address-4g5C' THEN
          CASE
            WHEN 'Address-AZf5' NOT LIKE 'Customer-vwRX' THEN
              'Amt-flAr'
            WHEN Table32.Customer6IcH = 'Address-wY1Z' THEN
              DerivedTable6.AddressD7PZ
            WHEN 'Code-lRZd' != 'Country-aSYf' THEN
              Table32.AccountNiCU
            ELSE
              'Amt-CG7d'
          END
        ELSE
          IF(
            STARTS_WITH(DerivedTable1.Codeuhr9,'Customer-xtNX'),
            'Customer-oYJM', 'Country-U5vY')
      END
        AS CCYbSPF,
      IF(
        STARTS_WITH(SourceE.FieldI,'Customer-Yl6K'),
        IF(
          STARTS_WITH('Country-ZGr6',Table31.Address9GCS), Table32.AccountApPl,
          DerivedTable6.CustomerxLhn),
        IF(
          ENDS_WITH(SourceD.FieldF,'Customer-3fmI'), Table32.CodeCEQT,
          'Address-EQfW'))
        AS Addressz5ST,
      IF(
        'Amt-VVqX' LIKE 'Code-x0Oj',
        IF(
          Table32.Feekyu2 NOT LIKE 'Amt-jAhz', Table31.CustomerpCuq,
          DerivedTable1.CustomerWrlz),
        IF(
          STARTS_WITH('Amt-DVp1','Address-mYyW'), 'Account-ehaZ',
          SourceB.FieldD))
        AS CustomerT5hI,
      IF(
        SourceC.FieldD NOT LIKE 'CCY-Ppkx',
        IF(
          STARTS_WITH(DerivedTable3.CustomerLP0M,'Amt-xoCB'), SourceB.FieldJ,
          Table31.AccountOIqn),
        IF(
          'Customer-8zXs' NOT LIKE 'Account-EXRR', 'Address-4lZg',
          SourceE.FieldG))
        AS AmtqLkt,
      IF(
        Table31.CustomerpCuq = 'CCY-Gs4t',
        CASE
          WHEN DerivedTable3.AccountViga = 'Fee-kVub' THEN
            DerivedTable5.CCYQK2Q
          WHEN STARTS_WITH(SourceC.FieldH,'Account-2cFA') THEN
            'Customer-8IzF'
          WHEN 'Code-FEOu' NOT LIKE 'Account-SwSz' THEN
            DerivedTable1.CountryV1io
          ELSE
            DerivedTable1.AmtzEYq
        END,
        IF(STARTS_WITH('Code-QZWH','Fee-Wi1q'),'Account-nrVi',SourceD.FieldA))
        AS CCYKsWo,
      IF(
        ENDS_WITH('Customer-DU0m',Table31.CodexXjy),
        IF(
          ENDS_WITH('Amt-5anS',Table31.Code9KZI), Table32.Customer6IcH,
          DerivedTable6.CodebnT9),
        IF(
          STARTS_WITH(SourceE.FieldJ,'Amt-5AM7'), 'Account-oVs0',
          Table32.AccountNiCU))
        AS CodeFMbQ,
      IF(
        SourceE.FieldE = SourceB.FieldJ,
        IF(
          STARTS_WITH(Table31.AccountOIqn,DerivedTable3.CodeUeLq), 'Code-EwMB',
          DerivedTable3.FeeF6P4),
        IF(
          ENDS_WITH(SourceC.FieldJ,'Customer-P7ss'), 'CCY-dVRk',
          DerivedTable6.CountryD7gw))
        AS CustomerqHLB,
      IF(
        'Customer-g1J4' LIKE 'Code-toF3',
        IF(SourceC.FieldJ != 'Fee-fIi7',Table32.AddressH6Yq,'Country-vJLj'),
        CASE
          WHEN 'Account-Ee1m' LIKE Table32.Feekyu2 THEN
            'Code-nOhe'
          WHEN ENDS_WITH(Table31.CustomerpCuq,DerivedTable1.AccountqcVc) THEN
            DerivedTable3.CustomereBLI
          ELSE
            'Address-4qxV'
        END)
        AS AmtB6MW,
      CASE
        WHEN STARTS_WITH('Account-mM7K',DerivedTable5.CountrynpDj) THEN
          IF(DerivedTable6.CodeY4Yb = 'Account-lljv','Fee-DczB','Fee-6Rbe')
        WHEN ENDS_WITH(SourceC.FieldB,DerivedTable1.AccountmNQo) THEN
          CASE
            WHEN SourceC.FieldB <> 'Country-LC5l' THEN
              Table31.Countryd8ig
            WHEN 'Customer-LAuG' <> 'Customer-jNL2' THEN
              DerivedTable3.Feeg8kl
            ELSE
              'Country-ldcK'
          END
        WHEN ENDS_WITH('Account-lPtL','Country-Ojx3') THEN
          CASE
            WHEN ENDS_WITH('Customer-d3V3',DerivedTable6.AddressgQNB) THEN
              'Customer-HnF1'
            WHEN ENDS_WITH(Table32.CCYPZmI,'Amt-q1yf') THEN
              DerivedTable6.AddressYFBr
            WHEN ENDS_WITH(Table32.Customer6IcH,'Customer-usau') THEN
              'Address-yIVx'
            WHEN 'Customer-1iPW' LIKE DerivedTable6.AddressgQNB THEN
              'Address-0Lmi'
            WHEN 'Country-j3ZJ' LIKE Table31.CCY4op0 THEN
              DerivedTable1.CustomerWrlz
            ELSE
              'Account-Xvvx'
          END
        ELSE
          CASE
            WHEN 'Fee-LC8f' <> SourceD.FieldI THEN
              'Address-17mM'
            WHEN ENDS_WITH('Code-6CqG',SourceE.FieldA) THEN
              DerivedTable1.CCYXSkS
            WHEN DerivedTable6.AmtW2w8 <> SourceB.FieldE THEN
              SourceC.FieldE
            WHEN STARTS_WITH(DerivedTable6.CodebnT9,SourceE.FieldH) THEN
              DerivedTable6.Address54VO
            ELSE
              DerivedTable6.AmtW2w8
          END
      END
        AS CCYk7B6,
      IF(
        'Fee-cQaI' NOT LIKE Table32.FeeDStf,
        IF(
          'Country-YAjN' <> DerivedTable6.Address54VO, 'Account-5xEe',
          'Account-QziK'),
        CASE
          WHEN STARTS_WITH(Table32.FeebSlR,'Address-oSik') THEN
            DerivedTable1.CustomerKrwR
          WHEN STARTS_WITH(DerivedTable6.AddressgQNB,'Customer-eUdP') THEN
            DerivedTable5.Customera0qh
          WHEN 'Code-ylFW' = DerivedTable6.FeeEU6a THEN
            'Country-iuUj'
          WHEN ENDS_WITH(DerivedTable6.AddressPpXC,'Country-c5oW') THEN
            Table32.Customer6IcH
          ELSE
            'Account-fumT'
        END)
        AS Code6yfJ,
      IF(
        STARTS_WITH(DerivedTable6.CountryD7gw,'Amt-xNrF'),
        CASE
          WHEN 'Country-FwZf' NOT LIKE 'Fee-7fJw' THEN
            'CCY-5LUq'
          WHEN 'Code-09P2' NOT LIKE DerivedTable1.FeerBIS THEN
            SourceD.FieldJ
          WHEN SourceD.FieldI LIKE SourceE.FieldE THEN
            'Customer-Ef64'
          WHEN 'Country-VUSm' NOT LIKE 'Country-tb0u' THEN
            SourceC.FieldB
          WHEN SourceB.FieldA = SourceB.FieldF THEN
            SourceE.FieldH
          ELSE
            DerivedTable1.CCYXSkS
        END, IF(
               ENDS_WITH('Code-ckq3',DerivedTable1.CCYXSkS), 'Fee-ayhG',
               'Account-BsBc'))
        AS CountryDffd,
      IF(
        STARTS_WITH(DerivedTable5.Customera0qh,SourceD.FieldB),
        CASE
          WHEN STARTS_WITH(DerivedTable6.AmtW2w8,DerivedTable3.Codeaqvh) THEN
            'Customer-ZMSR'
          WHEN ENDS_WITH(DerivedTable6.CustomerxLhn,Table32.AccountuH2c) THEN
            DerivedTable1.AccountfEJK
          WHEN ENDS_WITH('Code-HK7z','CCY-cx24') THEN
            Table32.FeeDStf
          ELSE
            SourceD.FieldI
        END, IF('Amt-JYJ4' = 'Amt-PILk','Code-ArfP','Country-xc4u'))
        AS Codep6v4,
      CASE
        WHEN ENDS_WITH('Country-kKlj','Fee-SE5e') THEN
          IF(
            ENDS_WITH('Customer-JEeR',SourceC.FieldF),
            DerivedTable6.Addressp4Y0, SourceE.FieldI)
        WHEN STARTS_WITH(DerivedTable1.AmtR86V,DerivedTable5.AddressV4AX) THEN
          IF(
            'Customer-UUbZ' <> 'Customer-wa5T', DerivedTable6.AddressgQNB,
            'Customer-ByBS')
        WHEN DerivedTable1.CustomerWrlz NOT LIKE 'Code-3Ai7' THEN
          IF(
            DerivedTable1.AccountKwSP LIKE Table31.Amtm9SX, Table31.CCYB8lD,
            SourceB.FieldG)
        ELSE
          IF(
            STARTS_WITH(DerivedTable3.CustomerULyd,SourceB.FieldA),
            'Account-fTlU', Table31.Code9KZI)
      END
        AS Countryl42L,
      IF(
        'CCY-k5bl' NOT LIKE DerivedTable1.AccountfEJK,
        IF(Table31.AddressFu86 NOT LIKE 'Amt-OVge','Fee-KNQc','Account-Ra0M'),
        IF(
          STARTS_WITH('Code-SEsT',DerivedTable1.CustomerKrwR), SourceB.FieldC,
          DerivedTable3.AccountViga))
        AS AmtCvo7,
      IF(
        'Fee-DknW' = DerivedTable5.CodePGRC,
        IF(
          ENDS_WITH('Country-IcI5',SourceC.FieldA), DerivedTable5.CountrynpDj,
          'Country-H29R'), IF(
                             STARTS_WITH(Table31.Address9GCS,'Amt-3afH'),
                             Table32.AddressH6Yq, 'Customer-O0nf'))
        AS AmtMTMQ,
      IF(
        'Fee-VCxe' != SourceE.FieldC,
        CASE
          WHEN SourceB.FieldE != 'Customer-N2Gk' THEN
            DerivedTable3.CodeVHWz
          WHEN STARTS_WITH('Fee-7JmD',DerivedTable6.Address54VO) THEN
            Table32.Accountk062
          WHEN 'Account-pIQD' LIKE SourceB.FieldF THEN
            'Customer-7HXJ'
          WHEN STARTS_WITH(SourceE.FieldI,'Customer-6Ufk') THEN
            DerivedTable6.Addressp4Y0
          ELSE
            Table31.CountrymFgA
        END, IF(
               SourceE.FieldD LIKE SourceD.FieldE, DerivedTable6.AddressWejH,
               DerivedTable3.CustomerLP0M))
        AS AmtFZpL,
      IF(
        STARTS_WITH('Fee-PWa5',SourceD.FieldA),
        IF(
          'CCY-Mo7z' <> DerivedTable6.AddressD7PZ, 'Customer-7AtN',
          'Address-Emjy'), IF(
                             DerivedTable5.Customera0qh LIKE 'Code-mrFR',
                             'Amt-XnMp', 'Country-8UYz'))
        AS AccountA4qk,
      IF(
        STARTS_WITH(DerivedTable3.CustomerLP0M,'Address-Z6ae'),
        IF(
          ENDS_WITH(SourceD.FieldI,'Country-AElD'), 'Country-GxOu',
          DerivedTable3.FeeqkjX),
        IF(
          STARTS_WITH('Account-cLn9','Fee-c66e'), DerivedTable5.Code7INp,
          DerivedTable1.CustomerWrlz))
        AS AmtDK2R,
      IF(
        'Account-n9mF' = 'Amt-zS1I',
        IF(
          STARTS_WITH(DerivedTable6.CodebnT9,SourceD.FieldA), 'Code-hM1p',
          DerivedTable6.FeeN99q),
        CASE
          WHEN STARTS_WITH('Country-VNgu','Amt-JVYl') THEN
            'Amt-iExE'
          WHEN ENDS_WITH(SourceC.FieldF,SourceE.FieldI) THEN
            'Address-Ritp'
          WHEN STARTS_WITH(SourceC.FieldC,'Code-Eamp') THEN
            'Amt-EySd'
          ELSE
            DerivedTable3.FeeqkjX
        END)
        AS AccountYbtB
    FROM
      testdataset.SourceE AS SourceE
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceB.FieldF,'Customer-8KAS') OR
         STARTS_WITH(SourceE.FieldB,SourceB.FieldB) AND
         'Country-B0qU' = 'Customer-qWvi' OR
         'Code-RHv6' NOT LIKE SourceB.FieldD)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH('Address-DR6g',SourceC.FieldG) AND
         SourceC.FieldJ <> 'CCY-dh0J' OR
         'CCY-hmEV' != SourceC.FieldG AND 'CCY-e2e7' LIKE SourceE.FieldD)
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH('Code-u0BT','Customer-o0s6') OR
         ENDS_WITH(SourceE.FieldA,'Country-ShQs') AND
         SourceD.FieldI NOT LIKE SourceD.FieldJ OR
         SourceD.FieldC NOT LIKE SourceC.FieldH)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('Amt-X4t1','Account-h6Me') OR 'Code-3dQ0' <> 'Code-W8Lo' AND
         STARTS_WITH('Amt-aQWW',SourceC.FieldC) OR
         STARTS_WITH('Customer-jO6M','CCY-OICy'))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (DerivedTable1.AccountKwSP NOT LIKE 'Customer-eR6I' AND
         SourceC.FieldH LIKE DerivedTable1.CCYXSkS OR
         'Fee-R1uD' = 'Customer-uRi6' AND SourceD.FieldI NOT LIKE 'Fee-A4Ca')
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        ('CCY-LgKs' LIKE 'CCY-mfS7' AND
         SourceB.FieldD <> DerivedTable1.AmtzEYq OR
         'Code-ODGJ' != DerivedTable5.CCY76AX OR
         STARTS_WITH('Customer-EhJS',DerivedTable5.AddressV4AX))
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (STARTS_WITH(DerivedTable3.Account4sxv,SourceD.FieldB) OR
         'Address-QY9E' NOT LIKE SourceE.FieldD AND
         DerivedTable3.Feeg8kl != 'Customer-usnK' OR
         ENDS_WITH(DerivedTable6.CustomerxLhn,'CCY-vpFl'))
      INNER JOIN Table31 AS Table31 ON
        (ENDS_WITH('Address-JXDR',SourceB.FieldI) OR
         DerivedTable6.CustomerxLhn <> SourceD.FieldD AND
         SourceB.FieldF LIKE 'Account-S10y' AND 'Fee-fgU3' != SourceE.FieldG)
      RIGHT JOIN Table32 AS Table32 ON
        (Table31.Countryd8ig NOT LIKE 'Code-QSp9' AND
         ENDS_WITH(DerivedTable6.AddressD7PZ,'CCY-F49z') AND
         Table31.CountrymFgA LIKE 'Customer-2hHU' AND
         'Code-BUJA' = DerivedTable6.FeeN99q)
    WHERE
      DerivedTable6.AddressPIbO = 'Amt-Fy3m' AND
      STARTS_WITH('Fee-WPAD',DerivedTable3.CountrygPvi) OR
      Table31.Countryd8ig NOT LIKE 'Customer-9VcC' OR
      ENDS_WITH(Table32.AccountNiCU,DerivedTable3.Account4sxv) OR
      ENDS_WITH(SourceB.FieldB,DerivedTable6.CountryD7gw) OR
      DerivedTable5.CCYQK2Q = DerivedTable6.AddressYFBr AND
      STARTS_WITH('Address-1iJx','Address-YdU8') OR
      STARTS_WITH(DerivedTable3.CountryoFFA,DerivedTable6.AddressWejH)
  ),
  Table34 AS (
    SELECT
      IF(
        'Fee-6bsP' = DerivedTable6.AmtCCH1,
        CASE
          WHEN ENDS_WITH(Table31.CCY4op0,'Address-HOSg') THEN
            'Customer-DYbN'
          WHEN 'Code-ChHn' <> DerivedTable3.CustomerLP0M THEN
            Table33.Addressz5ST
          WHEN DerivedTable1.AccountmNQo NOT LIKE SourceA.FieldC THEN
            'Account-i8TK'
          WHEN DerivedTable1.CustomerKrwR LIKE 'Customer-TsNQ' THEN
            'Amt-aN65'
          ELSE
            'Amt-qenC'
        END, IF(
               ENDS_WITH(DerivedTable4.CodeXHEv,DerivedTable3.CustomerULyd),
               'Amt-M8Vz', 'Address-ckEQ'))
        AS Addressniej,
      IF(
        ENDS_WITH('Address-7nX6','CCY-A35r'),
        IF(
          STARTS_WITH('Country-XwSy','Country-FWvm'), Table32.CustomeryLU9,
          'Account-t0CI'),
        CASE
          WHEN STARTS_WITH('Customer-Yftl','Code-6YE6') THEN
            'Country-SBxO'
          WHEN Table32.Feekyu2 = DerivedTable6.AddressPIbO THEN
            'Amt-5Yrs'
          ELSE
            DerivedTable4.CodelvtV
        END)
        AS Customer6xxb,
      IF(
        STARTS_WITH(SourceC.FieldJ,'Fee-F1Q4'),
        IF(
          'Address-WfLa' != 'Address-1FbL', DerivedTable6.CodeY4Yb,
          'Customer-JPLK'),
        IF('CCY-FRd4' = SourceB.FieldF,'Address-6HYq',DerivedTable2.CodeRziT))
        AS AddressDWrB,
      IF(
        STARTS_WITH('CCY-PmYB',DerivedTable3.CountrygPvi),
        IF(
          DerivedTable1.CustomerKrwR <> DerivedTable4.Account1SJF, 'CCY-RotB',
          DerivedTable3.FeeT7G9),
        CASE
          WHEN 'Customer-79Co' NOT LIKE Table33.CustomerqHLB THEN
            'Amt-knYE'
          WHEN 'Address-fkEW' <> Table31.AddressFu86 THEN
            DerivedTable4.FeesrBh
          WHEN Table32.FeeDStf = 'Code-DfUO' THEN
            Table31.Amtm9SX
          WHEN 'Customer-aN5d' != SourceB.FieldG THEN
            'Fee-ztpd'
          ELSE
            'Customer-Gesh'
        END)
        AS CCYUbcx,
      IF(
        'Amt-z8EW' = DerivedTable4.CCYY6No,
        CASE
          WHEN 'Code-pbqS' <> 'Amt-YLFL' THEN
            Table31.CCYB8lD
          WHEN ENDS_WITH('Amt-hzj9',SourceA.FieldB) THEN
            'Customer-W7MM'
          WHEN STARTS_WITH(SourceE.FieldC,'Fee-x748') THEN
            SourceA.FieldC
          ELSE
            'Account-XAKA'
        END, IF(
               SourceA.FieldJ LIKE DerivedTable1.CCYXSkS,
               DerivedTable6.Address54VO, 'Amt-DnBa'))
        AS AmtIxPm,
      IF(
        DerivedTable4.Addressm87D NOT LIKE 'Code-vNGi',
        IF(
          'Address-idKC' LIKE DerivedTable6.Addressp4Y0, Table32.AddressdrCR,
          Table32.CustomeriwOk),
        IF('Code-jRkn' <> 'Amt-6ht8','Account-HA5Y',DerivedTable4.Addressm87D))
        AS Amtar6r,
      IF(
        SourceC.FieldC != DerivedTable2.Amt885g,
        IF(DerivedTable3.CodeUeLq = 'Address-Wl9f',SourceE.FieldB,'Amt-ZQXw'),
        IF(
          ENDS_WITH(SourceE.FieldJ,'Country-Zoxh'), Table31.CustomerpCuq,
          SourceB.FieldG))
        AS AmtLcj2,
      CASE
        WHEN DerivedTable6.CodebnT9 LIKE 'Code-XKNE' THEN
          CASE
            WHEN 'Code-Pwxe' LIKE SourceA.FieldE THEN
              'Address-9naW'
            WHEN 'Code-oskr' LIKE 'Account-S27p' THEN
              DerivedTable1.CustomerWrlz
            WHEN 'Amt-VzpB' LIKE 'Code-cnuW' THEN
              Table32.Customer6IcH
            WHEN ENDS_WITH('Amt-UsOh',Table31.Countryd8ig) THEN
              Table32.AccountNiCU
            ELSE
              SourceA.FieldJ
          END
        WHEN 'Country-rFIt' NOT LIKE 'Account-sF3A' THEN
          IF(
            STARTS_WITH(Table33.CountryDffd,'Fee-drCn'), SourceB.FieldE,
            'Address-6SSw')
        WHEN 'Address-Pbqe' != 'Customer-qHyQ' THEN
          IF('Account-Z3XU' = DerivedTable3.CodeVHWz,'Country-zfwR','Amt-qsEA')
        ELSE
          CASE
            WHEN DerivedTable6.CountryD7gw LIKE 'Customer-j96U' THEN
              DerivedTable3.CodeUeLq
            WHEN STARTS_WITH('Code-SKgz',DerivedTable4.CodelvtV) THEN
              'Amt-TlSZ'
            ELSE
              Table31.Amtm9SX
          END
      END
        AS AmtMxN3,
      CASE
        WHEN 'Country-4AAn' NOT LIKE Table31.Code9KZI THEN
          IF(
            STARTS_WITH(DerivedTable1.CustomerWrlz,DerivedTable3.FeepQGh),
            SourceE.FieldI, 'Country-luz3')
        WHEN ENDS_WITH('Address-m7qs',Table31.Address9GCS) THEN
          IF(
            DerivedTable3.Account4sxv NOT LIKE 'Customer-BOLv', SourceA.FieldB,
            Table32.Feekyu2)
        ELSE
          IF(
            Table31.CCY4op0 NOT LIKE 'Amt-L5H3', DerivedTable4.FeezLpC,
            'Country-Ptci')
      END
        AS CustomerLV5F,
      IF(
        STARTS_WITH('Fee-YmQz','CCY-3PkU'),
        IF(
          STARTS_WITH('Address-wCtC','Amt-9jtk'), 'Code-cnjS',
          DerivedTable1.AmtzEYq),
        IF(
          DerivedTable6.Addressp4Y0 = Table33.AmtCvo7, Table31.AddressQZsG,
          'Address-0rpP'))
        AS Code0Vo3,
      IF(
        'CCY-gsX6' <> 'Amt-Sa0K',
        IF(
          STARTS_WITH(Table33.Codep6v4,DerivedTable4.Country8VtM),
          DerivedTable4.CodelvtV, Table33.AmtB6MW),
        IF(
          'Amt-twN3' LIKE 'Address-UDax', Table33.CodeFMbQ,
          DerivedTable3.CountrygPvi))
        AS Feegyty,
      IF(
        DerivedTable2.Account61kv NOT LIKE 'Fee-ShBJ',
        IF(Table31.AccountOIqn LIKE 'Amt-5sAM','Code-EHfC','Amt-2Z8X'),
        CASE
          WHEN DerivedTable3.CustomerLP0M <> Table31.Amtm9SX THEN
            'Address-7oAq'
          WHEN ENDS_WITH(DerivedTable6.AddressPpXC,'Fee-Gla9') THEN
            'Country-Ia82'
          WHEN 'Country-bCqS' = DerivedTable1.AccountqcVc THEN
            SourceA.FieldA
          WHEN DerivedTable1.AccountmNQo != 'Address-BXzK' THEN
            DerivedTable6.AddressD7PZ
          ELSE
            Table33.AmtDK2R
        END)
        AS AddresspzKC,
      IF(
        'Fee-Xfdi' LIKE DerivedTable4.AddressB6KJ,
        IF(
          STARTS_WITH('Account-QUFy',SourceC.FieldG), 'Country-QBw1',
          Table33.AmtCvo7), IF(
                              'Customer-idKQ' NOT LIKE 'CCY-zMlQ',
                              Table32.Accountk062, DerivedTable2.CountrywaWi))
        AS Feea5sE,
      IF(
        ENDS_WITH(DerivedTable1.CustomerWrlz,'Country-Tgfu'),
        IF(
          ENDS_WITH('CCY-QDdR',DerivedTable6.AmtW2w8), SourceB.FieldB,
          'Fee-CTrJ'), IF(
                         'Amt-n0gq' LIKE 'Customer-PZcb',
                         DerivedTable6.AddressPIbO, Table33.Countryl42L))
        AS AddressNg86,
      IF(
        'Country-Rsz4' NOT LIKE DerivedTable4.CodelvtV,
        IF('Fee-qOHX' != 'Amt-u944',DerivedTable6.FeeEU6a,Table32.CCYPZmI),
        IF('Customer-tmjZ' NOT LIKE 'CCY-E968','Country-UlPo',SourceA.FieldF))
        AS AddressfMAo
    FROM
      testdataset.SourceE AS SourceE
      INNER JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH('Fee-KXLL','Customer-L0ho') OR
         ENDS_WITH('Country-ZLUW',SourceE.FieldJ) OR
         ENDS_WITH('Account-9YRy',SourceA.FieldF) OR
         STARTS_WITH('CCY-vbV3','CCY-QIxM'))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (SourceE.FieldI <> 'Code-2K05' OR 'Address-tEbt' <> SourceE.FieldE AND
         SourceE.FieldC LIKE SourceA.FieldH AND
         STARTS_WITH(SourceB.FieldG,SourceE.FieldI))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Fee-nlgp',SourceC.FieldG) OR
         SourceB.FieldJ != 'Customer-FPsx' AND
         ENDS_WITH(SourceB.FieldH,'CCY-ejQm') OR
         'Country-5A8n' <> 'Address-arYB')
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Account-bPlF','Account-KlnK') OR
         STARTS_WITH('CCY-Jyep',SourceA.FieldB) AND
         SourceB.FieldH <> DerivedTable1.AccountqcVc OR
         ENDS_WITH(DerivedTable1.CountryV1io,SourceB.FieldF))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Address-e9ie' = DerivedTable2.Account61kv AND
         ENDS_WITH('Amt-LTsz','CCY-olWY') OR
         SourceB.FieldI = SourceE.FieldG AND
         'Amt-f4vg' NOT LIKE DerivedTable1.Addressrgjd)
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        ('Amt-rxOb' != 'CCY-dEBh' AND 'CCY-wcWu' <> SourceE.FieldD AND
         DerivedTable3.FeeT7G9 <> SourceB.FieldJ AND
         ENDS_WITH('CCY-bb6t','Amt-WwOw'))
      LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (ENDS_WITH('Amt-4gEn','Code-I0XP') OR
         'Customer-KENY' NOT LIKE 'Code-oZEU' AND
         ENDS_WITH(DerivedTable4.CodeJM19,'Address-32Og') AND
         STARTS_WITH('Amt-fVFj',DerivedTable2.AccountctCy))
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        ('Address-O5sA' != 'Account-wwv6' OR 'Fee-tFiC' != 'CCY-WY5c' AND
         STARTS_WITH(DerivedTable6.Fee8TJM,DerivedTable6.FeeFMCK) AND
         'Country-LI67' LIKE DerivedTable4.Country8VtM)
      LEFT JOIN Table31 AS Table31 ON
        (STARTS_WITH('Code-YPUO','CCY-EcOY') OR
         STARTS_WITH('Customer-TrkW','Fee-mFLH') OR
         ENDS_WITH(DerivedTable3.AccountViga,DerivedTable2.CodeDmva) OR
         ENDS_WITH(DerivedTable2.Amt885g,DerivedTable3.CodeUeLq))
      RIGHT JOIN Table32 AS Table32 ON
        ('Fee-HN1T' <> DerivedTable6.CountryD7gw OR
         'CCY-eYJh' LIKE 'Country-ux6j' OR
         ENDS_WITH(DerivedTable6.CodebnT9,'Fee-wowE') AND
         ENDS_WITH('Customer-Mtbp','Code-6St4'))
      LEFT JOIN Table33 AS Table33 ON
        (STARTS_WITH('Amt-CEUP','CCY-CheQ') AND 'Fee-Q1N7' <> SourceA.FieldH AND
         ENDS_WITH('Account-J7OP',DerivedTable2.Amtp4ee) OR
         'CCY-lb7U' NOT LIKE 'Country-fJ7R')
    WHERE
      ENDS_WITH(SourceB.FieldG,'Fee-4yiO') AND
      Table33.CCYk7B6 != 'Address-REsk' AND
      STARTS_WITH('Customer-O3Q7',Table32.CustomeryLU9) AND
      ENDS_WITH(DerivedTable4.AddressWvnP,'Country-4grY') OR
      'Customer-7YUw' <> DerivedTable1.CustomerKrwR AND
      STARTS_WITH(DerivedTable3.CodeVHWz,'Customer-1Lco') AND
      DerivedTable2.AmtmorU NOT LIKE SourceA.FieldF OR
      'Code-XxNr' NOT LIKE 'Code-HAbm'
  ),
  Table35 AS (
    SELECT
      IF(
        Table34.Addressniej != DerivedTable1.CCYXSkS,
        IF('Fee-7z3G' LIKE 'CCY-NelB','Amt-Coc7','Fee-y1lf'),
        IF('Address-ydmJ' <> DerivedTable3.CustomerLP0M,'Code-ZGoB','Fee-Wx3E'))
        AS Country90qX,
      IF(
        STARTS_WITH('Fee-hzen','CCY-4Rre'),
        IF(
          STARTS_WITH(Table33.CCYk7B6,Table34.Amtar6r), 'Country-JE0t',
          'Code-OPYa'),
        IF(Table33.CCYbSPF != SourceB.FieldG,SourceC.FieldF,'Amt-6JYa'))
        AS AmthGGG,
      IF(
        SourceD.FieldD NOT LIKE DerivedTable2.AccounttVM1,
        IF(
          ENDS_WITH(Table32.FeebSlR,SourceB.FieldI), Table34.Addressniej,
          SourceA.FieldD),
        IF(STARTS_WITH('Country-WYA7','Amt-gYbN'),'Code-LTdV','Code-zXGN'))
        AS FeeYMaJ,
      CASE
        WHEN STARTS_WITH('Customer-D2IG',DerivedTable2.AmtmorU) THEN
          IF(SourceE.FieldH LIKE SourceB.FieldH,'Customer-Xig0','Code-iEN1')
        WHEN ENDS_WITH(DerivedTable6.FeeEU6a,'Fee-KyPB') THEN
          IF(ENDS_WITH('Fee-nspp',SourceA.FieldG),'Code-OWf2',SourceD.FieldG)
        ELSE
          IF(SourceD.FieldJ != SourceC.FieldH,'Country-whoh','Fee-70KD')
      END
        AS AmtPUJI,
      IF(
        STARTS_WITH('CCY-5RTG','Customer-YunK'),
        IF('Code-XOHo' <> DerivedTable6.AddressD7PZ,'Amt-t6fq','CCY-vRlv'),
        IF(
          DerivedTable6.CodebnT9 LIKE Table33.Addressz5ST, SourceE.FieldI,
          Table31.CCY4op0))
        AS Fee7u0X,
      CASE
        WHEN ENDS_WITH('Amt-QxuG',SourceC.FieldC) THEN
          CASE
            WHEN
              ENDS_WITH(Table34.Feegyty,'CCY-Zu4s')
            THEN
              DerivedTable2.AddressHdnu
            WHEN
              STARTS_WITH('Code-rM67','Country-GIFb')
            THEN
              'Customer-djBY'
            WHEN
              STARTS_WITH(Table33.CustomerqHLB,DerivedTable2.Account61kv)
            THEN
              Table32.CodeCEQT
            WHEN
              ENDS_WITH('Country-nh7U','Customer-88Jr')
            THEN
              'Customer-bSX5'
            WHEN
              Table31.CCY4op0 = 'Address-0NWh'
            THEN
              'CCY-nr1f'
            ELSE
              SourceE.FieldG
          END
        WHEN 'Account-syIp' <> Table32.FeeDStf THEN
          IF(
            STARTS_WITH('CCY-m45n',Table31.AddressFu86), 'Country-GpWr',
            'CCY-XUmc')
        WHEN 'Amt-w4Jq' != DerivedTable2.Amt885g THEN
          CASE
            WHEN STARTS_WITH('Amt-ohMh',DerivedTable5.AddressV4AX) THEN
              'Customer-6ggy'
            WHEN STARTS_WITH(SourceC.FieldG,Table33.CodeFMbQ) THEN
              DerivedTable3.FeepQGh
            WHEN 'Amt-UmRx' != SourceC.FieldF THEN
              'Account-KzEF'
            WHEN ENDS_WITH(DerivedTable6.CustomerCZg3,'Address-hVOR') THEN
              'Address-X3tk'
            WHEN ENDS_WITH('Account-on4Z','Code-Gv2P') THEN
              DerivedTable3.CustomerULyd
            ELSE
              DerivedTable1.AccountmNQo
          END
        WHEN ENDS_WITH('Fee-6HpU',Table34.Addressniej) THEN
          IF(
            'Code-fwwW' NOT LIKE 'Account-w860', SourceE.FieldB,
            Table33.CustomerT5hI)
        ELSE
          IF(
            ENDS_WITH(DerivedTable1.AmtR86V,Table33.Code6yfJ),
            DerivedTable1.AccountqcVc, 'Code-lhja')
      END
        AS Codec7bi,
      IF(
        ENDS_WITH('Country-3Bm4',Table33.CodeFMbQ),
        IF(
          STARTS_WITH(SourceE.FieldE,'Account-TlGV'), DerivedTable1.Addressrgjd,
          'Fee-XjIj'),
        CASE
          WHEN ENDS_WITH(DerivedTable3.CustomereBLI,'Code-7Vf0') THEN
            SourceB.FieldI
          WHEN 'Customer-qP4j' LIKE DerivedTable6.AddressWejH THEN
            'CCY-6xD4'
          ELSE
            'Fee-Ya0N'
        END)
        AS CCYguVw,
      CASE
        WHEN STARTS_WITH(Table32.Accountk062,'Address-MzTg') THEN
          IF(
            SourceA.FieldB NOT LIKE DerivedTable2.AmtNncS, 'Fee-Wunt',
            'Customer-f1Wl')
        WHEN 'Code-Y3sG' NOT LIKE 'Fee-wwSl' THEN
          IF(ENDS_WITH('CCY-oimi',SourceE.FieldB),SourceE.FieldI,'Code-5sqS')
        WHEN STARTS_WITH(DerivedTable6.AddressgQNB,SourceA.FieldJ) THEN
          CASE
            WHEN
              STARTS_WITH(DerivedTable6.AddressgQNB,DerivedTable5.Feepmor)
            THEN
              SourceC.FieldE
            WHEN
              ENDS_WITH(Table33.AmtFZpL,'Customer-KfSr')
            THEN
              'CCY-8TwB'
            WHEN
              STARTS_WITH('Code-Ruvd',Table32.AddressH6Yq)
            THEN
              Table31.Countryd8ig
            ELSE
              'Country-JLiY'
          END
        WHEN 'Amt-flaZ' LIKE 'Country-mjTN' THEN
          IF(
            ENDS_WITH(SourceB.FieldC,DerivedTable1.AccountfEJK),
            DerivedTable1.CustomerWrlz, 'Address-65Vq')
        WHEN 'Code-OC9g' = SourceA.FieldF THEN
          IF(
            STARTS_WITH(DerivedTable3.CodeVHWz,DerivedTable6.FeeEU6a),
            DerivedTable3.FeeD0Po, Table33.Codep6v4)
        ELSE
          CASE
            WHEN ENDS_WITH('CCY-hvm6','Amt-DFl2') THEN
              SourceA.FieldJ
            WHEN STARTS_WITH('Fee-kWhK',Table32.Amt91NZ) THEN
              Table33.AccountA4qk
            ELSE
              DerivedTable5.Feepmor
          END
      END
        AS Customerm2u4,
      IF(
        STARTS_WITH('Address-DMkl','Fee-HoLO'),
        IF(
          DerivedTable6.FeeFMCK <> DerivedTable3.FeeqkjX, 'Fee-Puyy',
          'Address-evLR'), IF(
                             ENDS_WITH(DerivedTable6.CodeY4Yb,'Fee-Jewt'),
                             'Code-wNkD', SourceB.FieldB))
        AS Accountwc3T,
      IF(
        STARTS_WITH(DerivedTable1.CustomerWpfX,'Code-QQ9P'),
        IF(
          ENDS_WITH(SourceA.FieldC,Table33.AmtqLkt), 'Country-9yiY',
          Table33.CodeFMbQ), IF(
                               'Fee-KyEe' = 'Account-wgq4',
                               DerivedTable6.AddressPpXC, SourceB.FieldI))
        AS Amt7ivV
    FROM
      testdataset.DerivedTable3 AS DerivedTable3
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (DerivedTable3.Account4sxv != DerivedTable3.FeeT7G9 AND
         'Country-hL8z' != 'CCY-QoNc' AND
         'Country-zClb' NOT LIKE 'CCY-ecgq' OR
         ENDS_WITH(DerivedTable3.Code3Xip,'Code-FWg9'))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(DerivedTable3.FeeF6P4,'Account-erpL') AND
         'Code-I84B' NOT LIKE 'Fee-SGqk' AND
         DerivedTable3.FeeT7G9 <> DerivedTable3.FeeD0Po AND
         STARTS_WITH(DerivedTable3.Account4sxv,'Fee-s8NW'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (SourceC.FieldE != 'Customer-rmPh' AND
         ENDS_WITH('Account-we82',SourceC.FieldJ) AND
         DerivedTable3.FeeF6P4 <> 'Amt-X5I3' OR
         ENDS_WITH(SourceA.FieldG,'Amt-vMtc'))
      INNER JOIN testdataset.SourceD AS SourceD ON
        (SourceB.FieldH LIKE 'Fee-M77C' OR
         ENDS_WITH('Country-gPjj',SourceA.FieldA) AND
         SourceC.FieldE != DerivedTable3.FeeF6P4 AND
         SourceC.FieldA NOT LIKE DerivedTable3.FeeF6P4)
      INNER JOIN testdataset.SourceE AS SourceE ON
        (SourceD.FieldC <> SourceE.FieldF OR
         SourceC.FieldJ NOT LIKE 'Code-Z8WW' OR
         SourceC.FieldJ LIKE 'Account-unt2' OR
         'Account-ch3x' != DerivedTable3.CountryoFFA)
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (SourceD.FieldF != DerivedTable1.AddressoeLk AND
         ENDS_WITH(SourceB.FieldE,'Account-oBma') AND
         STARTS_WITH(SourceD.FieldJ,SourceC.FieldG) OR
         ENDS_WITH(SourceE.FieldB,'Customer-cEfy'))
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (STARTS_WITH(DerivedTable1.CustomerWrlz,'Account-Tr3L') AND
         ENDS_WITH(DerivedTable2.AccounttVM1,SourceA.FieldF) OR
         'Account-7vzh' != 'Country-yrTE' OR
         'Customer-PI19' LIKE SourceA.FieldI)
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (SourceC.FieldE <> 'Code-mhAs' AND
         SourceB.FieldI NOT LIKE 'Code-pOvu' OR
         'CCY-Fbbf' LIKE DerivedTable3.CountryoFFA AND
         DerivedTable2.Account61kv = SourceA.FieldH)
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (SourceB.FieldE != DerivedTable6.CustomerxLhn OR
         ENDS_WITH(SourceB.FieldH,'Fee-WAvn') OR
         'Account-w13y' != 'Customer-pJZ8' OR
         DerivedTable3.FeeD0Po LIKE 'Fee-BTdB')
      INNER JOIN Table31 AS Table31 ON
        ('CCY-AURY' NOT LIKE 'Fee-KRqM' AND
         STARTS_WITH(SourceD.FieldJ,'CCY-KMca') AND
         'Code-Ig6R' != DerivedTable6.CodebnT9 AND
         ENDS_WITH(SourceA.FieldA,SourceE.FieldE))
      RIGHT JOIN Table32 AS Table32 ON
        (STARTS_WITH('CCY-puuM','Country-9Bik') OR
         ENDS_WITH('Address-yU1i',DerivedTable1.Codeuhr9) OR
         ENDS_WITH('CCY-mYKO',DerivedTable3.CCYO742) AND
         'CCY-qs4S' != DerivedTable6.CCYp8wn)
      INNER JOIN Table33 AS Table33 ON
        ('Account-4rXA' LIKE 'Address-asfx' AND
         'Code-CeHw' != Table32.FeebSlR OR
         STARTS_WITH(DerivedTable1.AccountfEJK,'Country-mM1j') OR
         STARTS_WITH('Account-crdU','Address-H0xo'))
      RIGHT JOIN Table34 AS Table34 ON
        (Table33.AccountA4qk != 'Code-p97S' AND
         STARTS_WITH(DerivedTable2.Amtp4ee,'Customer-AATF') OR
         STARTS_WITH(SourceD.FieldH,SourceA.FieldB) OR
         ENDS_WITH(SourceD.FieldH,Table33.Countryl42L))
    WHERE
      SourceC.FieldF LIKE DerivedTable6.AmtCCH1 AND
      STARTS_WITH(SourceE.FieldF,'Code-qIub') AND
      'Code-4tZO' = Table32.AccountApPl AND
      Table34.Feea5sE NOT LIKE 'CCY-tvlR' OR
      ENDS_WITH('Code-CaO0',SourceE.FieldC) AND
      Table33.CustomerT5hI <> 'Address-Qg7T' AND
      ENDS_WITH('CCY-LJMT','Account-co0k') OR
      STARTS_WITH(SourceB.FieldI,'Fee-Z2xX')
  )
SELECT
  CASE
    WHEN 'Account-C9oD' = DerivedTable1.CountryV1io THEN
      IF(
        DerivedTable6.Fee8TJM <> 'Amt-npin', 'Fee-pYEK',
        DerivedTable6.CustomerxLhn)
    WHEN ENDS_WITH(Table33.CountryDffd,'Fee-sgWs') THEN
      IF(
        STARTS_WITH('Amt-Xzcl','Amt-aX7s'), SourceB.FieldF,
        DerivedTable2.AccountrJZf)
    WHEN ENDS_WITH(Table33.AmtMTMQ,'Address-HD3L') THEN
      IF(SourceC.FieldI != 'Amt-S1mE',DerivedTable6.FeeFMCK,SourceE.FieldI)
    ELSE
      IF('Fee-e5UK' = 'Code-g3xn',DerivedTable3.Feeg8kl,Table33.CustomerT5hI)
  END
    AS AddressuSTK,
  IF(
    'Fee-DvKQ' != Table33.CustomerOgOO,
    IF(ENDS_WITH('Account-6eDN','CCY-ub1A'),'CCY-Rz2G',DerivedTable3.CodeVHWz),
    IF('Address-XU9i' LIKE 'Account-i6ap','CCY-UnF8',DerivedTable6.FeeFMCK))
    AS AccountKbFf,
  IF(
    'Account-F3l6' = 'Account-x76P',
    IF(ENDS_WITH(Table33.AccountYbtB,'Address-p2ac'),SourceC.FieldG,'Amt-Nawo'),
    IF(Table32.AddressH6Yq LIKE 'Customer-85TY','Address-2xvN','Fee-q2Kh'))
    AS FeeLG30,
  CASE
    WHEN ENDS_WITH('Country-7RjY','Account-QvK4') THEN
      IF(STARTS_WITH(DerivedTable1.FeerBIS,'CCY-FUTd'),'Code-SQsS','CCY-dPzo')
    WHEN SourceD.FieldI NOT LIKE SourceB.FieldG THEN
      IF('Fee-9kVg' LIKE 'Address-JizE','CCY-OHCo',SourceC.FieldB)
    WHEN ENDS_WITH('Account-qta1',DerivedTable2.AccountrJZf) THEN
      IF(
        DerivedTable3.FeeF6P4 <> 'Account-gMEU', SourceE.FieldD,
        DerivedTable2.AccounttVM1)
    ELSE
      IF(
        STARTS_WITH('Account-VrHP',SourceB.FieldC), DerivedTable2.AccounttVM1,
        'Account-PWhI')
  END
    AS Address3l5L,
  IF(
    ENDS_WITH(Table32.Accountk062,DerivedTable3.CountrygPvi),
    IF(DerivedTable3.FeeCUde LIKE SourceB.FieldA,'Country-SXQu',SourceE.FieldF),
    IF(STARTS_WITH('Amt-PHNW','Account-fl4G'),'Amt-h8ci','Address-lBAh'))
    AS CountryXTJ7,
  IF(
    'Amt-yWux' NOT LIKE 'Code-xDuW',
    IF(
      ENDS_WITH('Address-sUBs','Address-s5E6'), Table33.CustomerOgOO,
      DerivedTable2.Address7oYI),
    IF(
      STARTS_WITH('CCY-1dEm',Table33.Countryl42L), DerivedTable2.AccounttVM1,
      DerivedTable6.CustomerxLhn))
    AS CountrypfA6,
  IF(
    ENDS_WITH(Table32.Accountk062,DerivedTable2.CodeRziT),
    IF(
      STARTS_WITH('Account-GRTx','Fee-TOSL'), DerivedTable2.AddresscM17,
      DerivedTable2.AccounttVM1),
    IF(STARTS_WITH(SourceC.FieldJ,'Customer-wHco'),'Code-5Rfk',SourceE.FieldJ))
    AS CustomertAao,
  IF(
    Table33.AmtDK2R != 'Amt-lYcp',
    IF('Amt-6Rg1' = DerivedTable6.CustomerCZg3,'Address-LQsF','Fee-hBRQ'),
    CASE
      WHEN Table33.Code6yfJ NOT LIKE 'Address-bMs1' THEN
        DerivedTable1.AmtzEYq
      WHEN ENDS_WITH(Table32.Customer6IcH,'Amt-eXi3') THEN
        'Address-MnkE'
      ELSE
        'CCY-MUn0'
    END)
    AS CountryrPg8,
  IF(
    STARTS_WITH(Table35.Amt7ivV,Table33.CustomerOgOO),
    IF(
      DerivedTable2.AmtNncS LIKE 'CCY-z1PQ', Table35.Accountwc3T,
      'Address-Ezoy'),
    IF(DerivedTable6.CCYp8wn LIKE 'Fee-sDYG',DerivedTable1.FeerBIS,'CCY-9hEl'))
    AS Codeuiec,
  IF(
    STARTS_WITH(DerivedTable6.AddressYFBr,'Address-y2Io'),
    IF(
      STARTS_WITH('Account-OfbT',DerivedTable3.CustomerULyd),
      DerivedTable3.Code3Xip, 'Customer-jLW0'),
    CASE
      WHEN Table32.AccountuH2c <> DerivedTable6.CountryD7gw THEN
        SourceE.FieldC
      WHEN 'Fee-mlkp' != 'Code-RkfO' THEN
        'Customer-kGIF'
      WHEN ENDS_WITH('Customer-aeoG','Code-hnRp') THEN
        DerivedTable3.Account4sxv
      ELSE
        Table33.CCYKsWo
    END)
    AS Amt9aae,
  IF(
    Table33.CustomerT5hI LIKE 'Account-3BPn',
    IF('Country-GdkG' <> 'Customer-k2r3','Account-R0ok','Address-Dypl'),
    IF('Amt-owL4' LIKE 'CCY-2DrB',DerivedTable2.AmtNncS,'Account-w3ls'))
    AS Amt3faU,
  IF(
    DerivedTable1.AmtzEYq = 'Account-PqYL',
    IF(DerivedTable2.AccountctCy LIKE SourceC.FieldJ,'Fee-bVzc',SourceE.FieldE),
    IF(
      STARTS_WITH('CCY-8gpN','Code-6H7R'), DerivedTable2.CountrywaWi,
      'Customer-UwH1'))
    AS CCYZD20,
  IF(
    SourceD.FieldF = 'Amt-7uXl',
    CASE
      WHEN STARTS_WITH(SourceD.FieldA,SourceE.FieldF) THEN
        DerivedTable6.CountryD7gw
      WHEN ENDS_WITH(SourceB.FieldG,DerivedTable1.AccountqcVc) THEN
        'Amt-ocQm'
      WHEN STARTS_WITH('Amt-nhGh','Customer-euxT') THEN
        'Country-iMyz'
      WHEN 'Country-Ntoz' = 'Fee-dEux' THEN
        SourceD.FieldD
      ELSE
        DerivedTable3.FeeCUde
    END, CASE
           WHEN ENDS_WITH('Country-FCYX','CCY-ZgEk') THEN
             DerivedTable3.Feeg8kl
           WHEN Table33.AccountA4qk LIKE 'Address-EH2I' THEN
             'Country-lijS'
           ELSE
             'Fee-VEEN'
         END)
    AS AddressgmSt,
  IF(
    ENDS_WITH('Address-PWVB',SourceD.FieldC),
    IF('Country-dlWj' = 'Account-r1Sg','Code-J9SY',SourceB.FieldF),
    IF(
      STARTS_WITH('Amt-Ubzq',SourceD.FieldI), Table33.AmtqLkt,
      Table33.Countryl42L))
    AS FeeBS3r,
  IF(
    STARTS_WITH(Table33.Code6yfJ,'Account-EX53'),
    IF(
      ENDS_WITH(SourceD.FieldG,'Country-aiCY'), DerivedTable2.AccountrJZf,
      Table33.CCYKsWo),
    CASE
      WHEN STARTS_WITH('Code-cpCc',SourceD.FieldJ) THEN
        DerivedTable6.AddressgQNB
      WHEN SourceE.FieldD = Table33.Codep6v4 THEN
        DerivedTable6.AddressgQNB
      ELSE
        'Amt-7twC'
    END)
    AS CodeOp8k
FROM
  Table35 AS Table35
  LEFT JOIN testdataset.SourceB AS SourceB ON
    (STARTS_WITH(SourceB.FieldE,'Address-K9lk') OR
     ENDS_WITH(SourceB.FieldG,'CCY-Fwwo') OR
     ENDS_WITH(SourceB.FieldD,SourceB.FieldD) AND 'CCY-1CNy' <> 'Country-tZ9F')
  INNER JOIN testdataset.SourceC AS SourceC ON
    ('Customer-ho9m' NOT LIKE 'Code-G4uW' AND
     STARTS_WITH('Fee-J1I5','Fee-ERl0') AND
     'Account-iwIg' LIKE Table35.AmthGGG AND
     SourceB.FieldB = Table35.Accountwc3T)
  RIGHT JOIN testdataset.SourceD AS SourceD ON
    (ENDS_WITH(Table35.Codec7bi,'Country-ycpA') AND
     'Code-P6gT' = 'Country-m4Tc' OR
     SourceD.FieldI = 'Code-gQ70' AND ENDS_WITH('Fee-Qwme','Address-DXYe'))
  INNER JOIN testdataset.SourceE AS SourceE ON
    (ENDS_WITH(SourceB.FieldH,'Address-vDN2') OR
     ENDS_WITH(SourceB.FieldE,SourceD.FieldG) AND
     ENDS_WITH(SourceE.FieldH,'Amt-J97u') AND Table35.Fee7u0X = SourceB.FieldI)
  INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
    (STARTS_WITH(SourceC.FieldG,SourceE.FieldH) AND
     SourceC.FieldF = SourceC.FieldC AND
     STARTS_WITH(DerivedTable1.Addressrgjd,'Account-uo2Q') AND
     SourceE.FieldB != SourceC.FieldE)
  INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
    (STARTS_WITH(DerivedTable1.AccountfEJK,'Country-rBAu') AND
     ENDS_WITH(SourceE.FieldJ,SourceB.FieldG) AND
     'Code-OfCK' LIKE DerivedTable2.AmtmorU OR
     ENDS_WITH(DerivedTable1.AmtR86V,'CCY-RCUp'))
  INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
    (ENDS_WITH('Account-KHWm','Country-7Ar9') OR
     'Customer-HouA' != 'Fee-3gDe' OR
     SourceB.FieldI NOT LIKE 'Account-PkbA' OR
     STARTS_WITH('Code-3Ur2','Code-Tcot'))
  INNER JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
    (DerivedTable1.AccountmNQo <> 'Address-geyi' OR
     'Amt-Nk4A' LIKE 'Amt-S9cu' OR
     DerivedTable6.CustomerxLhn <> SourceC.FieldI OR
     DerivedTable2.AddresscM17 LIKE DerivedTable1.AccountmNQo)
  INNER JOIN Table32 AS Table32 ON
    (ENDS_WITH(SourceC.FieldC,DerivedTable6.AddressWejH) AND
     STARTS_WITH('Country-JiNw','Code-bpM3') OR
     Table32.Feekyu2 = DerivedTable6.AmtW2w8 AND
     'Country-mjBX' LIKE Table32.AccountApPl)
  RIGHT JOIN Table33 AS Table33 ON
    (STARTS_WITH(Table33.AmtCvo7,Table35.Codec7bi) AND
     'Country-NRm5' LIKE 'Address-jF2H' AND
     Table32.CustomeryLU9 = SourceC.FieldH OR 'Address-67RU' = SourceE.FieldF)
WHERE
  DerivedTable6.Address54VO LIKE 'Code-mpLA' OR
  Table32.AccountApPl = Table35.Fee7u0X AND
  ENDS_WITH('Address-yql4','Country-eLYp') OR
  ENDS_WITH(Table33.AccountA4qk,'Account-XbIY') AND
  'Customer-6NIK' != Table32.AddressdrCR OR 'Amt-OLsZ' != SourceD.FieldI OR
  SourceD.FieldH = 'CCY-LyOC' AND 'Amt-E3rc' LIKE SourceC.FieldH