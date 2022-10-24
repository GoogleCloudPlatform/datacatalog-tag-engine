CREATE OR REPLACE TABLE testdataset.DerivedTable2 AS
WITH
  Table6 AS (
    SELECT
      CASE
        WHEN SourceE.FieldJ LIKE DerivedTable1.AmtzEYq THEN
          IF(
            STARTS_WITH(SourceD.FieldJ,DerivedTable1.FeerBIS), 'Address-ZCbD',
            'Address-V9LW')
        WHEN ENDS_WITH('Address-aoEF','CCY-gju5') THEN
          IF('Fee-vyh7' NOT LIKE 'Country-AQOL',SourceB.FieldF,'CCY-KVIe')
        ELSE
          IF(
            SourceB.FieldH LIKE DerivedTable1.CCYXSkS, SourceE.FieldI,
            DerivedTable1.CountryV1io)
      END
        AS AddresslEfL,
      IF(
        DerivedTable1.AccountqcVc <> 'Account-mqni',
        IF(
          STARTS_WITH(SourceE.FieldA,DerivedTable1.AmtzEYq), SourceD.FieldC,
          'Country-fFeE'),
        IF(ENDS_WITH('Fee-Zwky','CCY-kqAn'),SourceC.FieldF,'Code-AlrG'))
        AS CountryeNab,
      IF(
        'Fee-Uwgk' <> 'Fee-ChNS',
        CASE
          WHEN ENDS_WITH('Account-pHeA','Fee-tP4m') THEN
            'Customer-XaD0'
          WHEN DerivedTable1.FeerBIS != SourceA.FieldF THEN
            'Account-ssh9'
          WHEN SourceD.FieldF <> SourceC.FieldI THEN
            'CCY-G9CG'
          ELSE
            SourceC.FieldC
        END, IF(
               ENDS_WITH('Address-Z53y','CCY-JRb6'), SourceA.FieldE,
               DerivedTable1.CustomerWpfX))
        AS CCYz1qA,
      IF(
        SourceE.FieldB LIKE SourceA.FieldG,
        IF(
          SourceB.FieldA LIKE SourceB.FieldH, DerivedTable1.Addressrgjd,
          'CCY-BxJh'), IF(
                         STARTS_WITH('Fee-zsbV',SourceE.FieldH),
                         DerivedTable1.CustomerWpfX, 'Customer-3DuH'))
        AS FeeQVk4,
      IF(
        'Code-YC8I' LIKE SourceA.FieldB,
        IF(
          STARTS_WITH(SourceD.FieldI,'Address-1oND'), DerivedTable1.Codeuhr9,
          'CCY-SI6d'),
        IF(SourceE.FieldE != SourceD.FieldF,'CCY-AjZA','CCY-HLy3'))
        AS AccountOHQa,
      CASE
        WHEN 'Address-OFe4' = SourceA.FieldH THEN
          CASE
            WHEN 'CCY-zHe4' = SourceD.FieldF THEN
              'Address-OOv1'
            WHEN SourceD.FieldH <> 'Code-Y9wR' THEN
              SourceB.FieldG
            ELSE
              'Customer-zn1Q'
          END
        WHEN STARTS_WITH('Fee-L8Zt',SourceB.FieldA) THEN
          IF('Country-tsGj' <> 'CCY-rgwv',DerivedTable1.FeerBIS,SourceE.FieldD)
        WHEN STARTS_WITH('Fee-iOio',SourceB.FieldF) THEN
          IF(
            ENDS_WITH(SourceC.FieldH,SourceB.FieldA), 'Amt-k6Pk',
            'Customer-RB2F')
        ELSE
          IF(
            STARTS_WITH(SourceB.FieldH,DerivedTable1.CCYXSkS),
            DerivedTable1.AccountqcVc, SourceC.FieldF)
      END
        AS Codeju07,
      IF(
        'Address-F5lI' <> 'Customer-eqfr',
        IF(
          STARTS_WITH('Amt-8qxO','Country-9wpw'), SourceB.FieldH,
          'Customer-gaRs'),
        IF(ENDS_WITH(SourceB.FieldJ,'Code-jtYN'),'Code-t6vd',SourceC.FieldE))
        AS CodeKYmV,
      IF(
        ENDS_WITH('Fee-DAex',SourceB.FieldH),
        IF(
          STARTS_WITH(SourceA.FieldA,SourceD.FieldB), SourceB.FieldE,
          DerivedTable1.CustomerWpfX),
        IF(ENDS_WITH(SourceB.FieldD,'Amt-uCTh'),SourceD.FieldE,'Address-MVzT'))
        AS CustomerUs4L,
      IF(
        STARTS_WITH('Code-AjBb','Amt-LC7z'),
        IF(SourceA.FieldB LIKE 'Code-cKJy',SourceB.FieldG,'Account-ARCH'),
        IF('Address-ZaH2' = 'Amt-axTr',SourceB.FieldH,SourceB.FieldI))
        AS AddressO6QX,
      IF(
        SourceC.FieldD = 'Fee-DdqD',
        IF(
          'Country-HWw9' NOT LIKE SourceC.FieldF, 'Account-XGer',
          SourceB.FieldF),
        IF(STARTS_WITH('Fee-gF5t','CCY-REN5'),'Address-0aaN',SourceC.FieldI))
        AS CustomerYwXe,
      IF(
        SourceB.FieldC != 'Address-fk0y',
        IF(
          'Country-w3zp' NOT LIKE 'Account-LCml', DerivedTable1.AccountKwSP,
          DerivedTable1.CustomerWpfX),
        CASE
          WHEN 'CCY-LG8k' LIKE 'Code-DJki' THEN
            'Country-L9pR'
          WHEN 'CCY-EgzS' != 'CCY-tiGU' THEN
            SourceD.FieldE
          WHEN STARTS_WITH('Fee-vXud',DerivedTable1.CustomerWrlz) THEN
            'Address-yZNb'
          WHEN SourceA.FieldA LIKE 'Account-jQo0' THEN
            DerivedTable1.CountryV1io
          WHEN ENDS_WITH(SourceD.FieldI,SourceD.FieldG) THEN
            'Code-czAd'
          ELSE
            SourceD.FieldD
        END)
        AS CountryMbVx
    FROM
      testdataset.SourceA AS SourceA
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH(SourceB.FieldF,SourceA.FieldJ) AND
         ENDS_WITH('Fee-e3eu',SourceB.FieldE) AND
         ENDS_WITH(SourceB.FieldI,SourceA.FieldE) AND
         STARTS_WITH(SourceB.FieldI,'Fee-Bnm4'))
      LEFT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH('Code-SUGt',SourceA.FieldB) OR
         'CCY-BPXY' <> SourceB.FieldF OR
         SourceC.FieldG NOT LIKE SourceB.FieldB OR
         'Address-MQLV' = 'Address-uG11')
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(SourceC.FieldB,SourceC.FieldH) OR
         'Country-sMEM' NOT LIKE SourceB.FieldI AND
         SourceC.FieldC != SourceC.FieldB AND SourceB.FieldI = SourceA.FieldG)
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        ('Amt-C8Ku' <> SourceB.FieldC OR
         STARTS_WITH(SourceE.FieldA,'Account-8Bcv') AND
         'Customer-8RcM' <> SourceB.FieldG AND SourceA.FieldB = 'Fee-mvDC')
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Country-0oXh',SourceD.FieldC) OR
         'Address-bfZ2' <> DerivedTable1.CountryV1io OR
         SourceB.FieldA = SourceE.FieldJ OR SourceE.FieldH LIKE 'Customer-3KJ8')
    WHERE
      STARTS_WITH('Customer-Gi9E','Fee-TtWw') OR
      SourceA.FieldE NOT LIKE DerivedTable1.AccountmNQo OR
      SourceC.FieldC != SourceE.FieldE AND
      ENDS_WITH(SourceD.FieldA,SourceB.FieldD) AND
      ENDS_WITH(DerivedTable1.Codeuhr9,'Amt-KXoy') OR
      DerivedTable1.AddressoeLk NOT LIKE 'Fee-9d3f' AND
      SourceA.FieldD NOT LIKE 'Code-Zq7c' OR
      ENDS_WITH('Customer-iuC7',DerivedTable1.Addressrgjd)
  ),
  Table7 AS (
    SELECT
      IF(
        STARTS_WITH(DerivedTable1.AmtR86V,'Account-08CK'),
        IF(
          'Account-gYSf' <> 'Account-z2F4', DerivedTable1.AmtzEYq,
          SourceB.FieldF), IF(
                             STARTS_WITH(SourceD.FieldG,'Address-hfU6'),
                             SourceC.FieldH, 'Account-hXm8'))
        AS Customer7fcU,
      CASE
        WHEN ENDS_WITH('Code-N5Gx','CCY-vzjX') THEN
          IF(
            ENDS_WITH('Address-xZec','Customer-8ZdC'),
            DerivedTable1.Addressrgjd, DerivedTable1.CustomerWpfX)
        WHEN 'Amt-hx1q' != 'Amt-JULI' THEN
          CASE
            WHEN ENDS_WITH(SourceB.FieldJ,SourceD.FieldE) THEN
              SourceD.FieldB
            WHEN 'Address-mU4y' <> 'Country-DLXI' THEN
              'Account-mFJ9'
            WHEN 'Country-0zLC' != 'Customer-e4mM' THEN
              SourceB.FieldA
            WHEN ENDS_WITH('Customer-yhj3',SourceB.FieldE) THEN
              'Address-mTBI'
            WHEN SourceD.FieldH = 'Fee-d2rf' THEN
              'CCY-rqFC'
            ELSE
              'Amt-rZBx'
          END
        WHEN STARTS_WITH('Account-JHt3',SourceA.FieldI) THEN
          IF(
            'Account-EOfR' NOT LIKE SourceA.FieldI, DerivedTable1.CountryV1io,
            SourceC.FieldH)
        WHEN 'Address-GIME' <> 'Customer-6uDj' THEN
          IF(
            ENDS_WITH('Customer-3fM7','Customer-Na7a'), DerivedTable1.CCYXSkS,
            SourceE.FieldC)
        WHEN STARTS_WITH('Amt-TKkC',DerivedTable1.FeerBIS) THEN
          IF(ENDS_WITH('Amt-Huvk','Account-4XQM'),'Amt-hC7B',SourceE.FieldJ)
        ELSE
          IF(
            STARTS_WITH(SourceB.FieldH,SourceE.FieldE), 'Fee-pHFQ',
            DerivedTable1.CustomerWpfX)
      END
        AS Account6ZJO,
      CASE
        WHEN ENDS_WITH(DerivedTable1.AccountqcVc,SourceD.FieldB) THEN
          IF(
            'Country-w3bw' = 'Customer-jxIT', 'Amt-lPDo',
            DerivedTable1.AccountqcVc)
        WHEN STARTS_WITH(SourceB.FieldD,Table6.CCYz1qA) THEN
          IF(SourceD.FieldE NOT LIKE 'Account-hGkC','Amt-9RGr','Account-vR8y')
        WHEN ENDS_WITH('Account-22kV','Address-Pz7d') THEN
          IF('Account-ZEE4' != 'Account-P18l',SourceD.FieldF,'Amt-wHsV')
        WHEN ENDS_WITH('Account-wQ69',SourceD.FieldI) THEN
          IF('Country-Oe4Q' != 'Amt-k41u',SourceD.FieldJ,SourceD.FieldF)
        ELSE
          IF('Account-TfLt' <> 'Amt-im4b','Address-JD21',Table6.CountryeNab)
      END
        AS AddresshO4t,
      IF(
        Table6.FeeQVk4 != SourceC.FieldG,
        IF('CCY-O1pC' <> 'CCY-xcZY','Fee-WJKO','Account-Afn0'),
        CASE
          WHEN DerivedTable1.AmtR86V LIKE 'Address-pHlp' THEN
            SourceD.FieldI
          WHEN STARTS_WITH(DerivedTable1.AccountmNQo,SourceE.FieldE) THEN
            Table6.AccountOHQa
          ELSE
            SourceE.FieldF
        END)
        AS FeeYXiQ,
      CASE
        WHEN STARTS_WITH(SourceB.FieldJ,'Customer-hNZN') THEN
          CASE
            WHEN STARTS_WITH(SourceA.FieldF,SourceB.FieldH) THEN
              'Customer-qbjq'
            WHEN ENDS_WITH('Code-IG42',SourceA.FieldB) THEN
              Table6.FeeQVk4
            WHEN DerivedTable1.CustomerWrlz <> Table6.FeeQVk4 THEN
              'Country-LxgT'
            WHEN 'Code-Oeoy' = 'Address-lHxK' THEN
              'Account-GinR'
            WHEN ENDS_WITH(SourceE.FieldA,'CCY-YJWF') THEN
              SourceB.FieldJ
            ELSE
              'Country-uBnV'
          END
        WHEN STARTS_WITH(SourceB.FieldH,DerivedTable1.AccountfEJK) THEN
          IF(
            ENDS_WITH('Address-CQOZ','Account-OJuG'), 'Code-0k6b',
            SourceA.FieldA)
        WHEN STARTS_WITH(Table6.Codeju07,'Address-kPr0') THEN
          IF(
            STARTS_WITH('CCY-ui2H',DerivedTable1.CCYXSkS), 'Customer-uHAA',
            'Fee-IPpu')
        ELSE
          IF(STARTS_WITH('Fee-haSs','Amt-gM5o'),'Code-b9Ed','Customer-OMK3')
      END
        AS FeeAEBz,
      IF(
        ENDS_WITH('Account-5i5D',SourceE.FieldG),
        IF('Address-CtkS' <> Table6.CountryeNab,'Address-OkX7',SourceC.FieldE),
        IF(
          SourceD.FieldH = 'Country-45OG', DerivedTable1.AddressoeLk,
          'Customer-tRic'))
        AS AddressqiWG,
      CASE
        WHEN STARTS_WITH(SourceB.FieldD,'Country-JJ4e') THEN
          CASE
            WHEN 'Fee-0Z2J' NOT LIKE 'Fee-pESj' THEN
              'Fee-WhXz'
            WHEN ENDS_WITH(SourceD.FieldA,'Code-NBP9') THEN
              'Fee-xs9f'
            ELSE
              Table6.CCYz1qA
          END
        WHEN SourceD.FieldB = 'Account-HBW4' THEN
          IF(
            ENDS_WITH('Customer-pUNX',SourceA.FieldE), SourceA.FieldH,
            SourceD.FieldI)
        WHEN STARTS_WITH('Address-AkAG','Code-sZ4w') THEN
          IF(
            'Customer-gIBb' = 'Address-5v9i', DerivedTable1.CustomerKrwR,
            'Fee-qxn1')
        WHEN SourceD.FieldF <> SourceD.FieldE THEN
          IF(
            ENDS_WITH('Account-m4Sw',SourceC.FieldJ), 'Account-1PJe',
            SourceD.FieldE)
        WHEN SourceA.FieldJ = 'Code-OXlp' THEN
          CASE
            WHEN STARTS_WITH(SourceB.FieldC,SourceD.FieldC) THEN
              'CCY-BZOm'
            WHEN Table6.FeeQVk4 = DerivedTable1.CountryV1io THEN
              DerivedTable1.AddressoeLk
            WHEN STARTS_WITH(SourceE.FieldB,SourceE.FieldJ) THEN
              'Fee-dsVY'
            WHEN ENDS_WITH(DerivedTable1.CustomerKrwR,SourceA.FieldA) THEN
              'Address-FeLW'
            WHEN SourceA.FieldF != Table6.Codeju07 THEN
              'Address-aopo'
            ELSE
              'Country-g5U5'
          END
        ELSE
          IF(ENDS_WITH('Account-AiIC',SourceD.FieldA),'Address-DXBy','Fee-tOvL')
      END
        AS AmtPg6E,
      IF(
        'Customer-jbC9' NOT LIKE DerivedTable1.AccountfEJK,
        CASE
          WHEN 'Fee-4aLh' <> SourceD.FieldD THEN
            'Code-IlTq'
          WHEN STARTS_WITH(SourceE.FieldA,Table6.CCYz1qA) THEN
            'Country-BIqg'
          ELSE
            'CCY-6lHk'
        END, IF(
               STARTS_WITH('Account-T6rI',SourceA.FieldJ), Table6.AccountOHQa,
               DerivedTable1.AccountfEJK))
        AS AmtC0Ga,
      IF(
        SourceC.FieldD <> 'Account-XzTF',
        IF(
          STARTS_WITH(SourceD.FieldJ,'Customer-pVEP'), SourceE.FieldJ,
          'CCY-RHjq'),
        CASE
          WHEN SourceD.FieldG = SourceA.FieldJ THEN
            SourceC.FieldG
          WHEN DerivedTable1.AccountfEJK != SourceC.FieldJ THEN
            'Customer-XU2D'
          WHEN STARTS_WITH(SourceD.FieldG,'Country-WcGB') THEN
            'Address-Xceq'
          WHEN ENDS_WITH(DerivedTable1.CustomerWrlz,Table6.CustomerUs4L) THEN
            SourceC.FieldC
          ELSE
            'Country-GoXe'
        END)
        AS FeeB8Wr,
      IF(
        'Country-8BZn' NOT LIKE 'CCY-ARyR',
        CASE
          WHEN ENDS_WITH('CCY-wVr6',SourceC.FieldA) THEN
            DerivedTable1.CustomerWrlz
          WHEN SourceA.FieldH LIKE 'Customer-fsuk' THEN
            'Customer-Unyt'
          WHEN 'Customer-S46E' NOT LIKE SourceA.FieldD THEN
            SourceB.FieldB
          WHEN ENDS_WITH('Amt-n3zx',SourceE.FieldF) THEN
            'Address-0OUh'
          WHEN 'Fee-zz8K' <> 'Address-rFeK' THEN
            'Account-c3FI'
          ELSE
            'Amt-2019'
        END, IF(
               STARTS_WITH(SourceE.FieldC,'Country-z79I'),
               DerivedTable1.CustomerWpfX, SourceD.FieldF))
        AS CountrybN0v,
      IF(
        DerivedTable1.AmtR86V LIKE Table6.CountryMbVx,
        IF(
          Table6.CountryMbVx <> SourceA.FieldF, SourceC.FieldD,
          DerivedTable1.AmtR86V),
        IF(
          'Account-zlaI' NOT LIKE SourceD.FieldF, 'Code-mMbh',
          Table6.AddressO6QX))
        AS CCY3lvo,
      IF(
        SourceD.FieldJ != 'Account-xtEg',
        IF(
          DerivedTable1.AccountqcVc = DerivedTable1.FeerBIS,
          DerivedTable1.CustomerWrlz, 'Address-Vs5t'),
        IF(ENDS_WITH(SourceE.FieldF,'Account-LW5A'),'Fee-q93e',SourceA.FieldI))
        AS AddressWs4z,
      IF(
        STARTS_WITH(SourceB.FieldE,SourceE.FieldA),
        IF(Table6.CustomerYwXe != 'Code-7h9a',SourceE.FieldF,'Amt-IJbV'),
        IF(
          SourceE.FieldJ LIKE 'Customer-9qce', SourceE.FieldA,
          DerivedTable1.AmtR86V))
        AS AccountNi89,
      IF(
        STARTS_WITH(SourceC.FieldA,'Code-PB2j'),
        IF(SourceE.FieldA <> SourceC.FieldE,SourceE.FieldB,'Address-ftzn'),
        IF(
          ENDS_WITH('CCY-wcKU',SourceC.FieldD), 'Amt-jZpU',
          DerivedTable1.CustomerWpfX))
        AS Accounth5Rx
    FROM
      testdataset.SourceD AS SourceD
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Code-lpNQ' = 'Address-YqxB' AND SourceD.FieldC LIKE SourceA.FieldH AND
         ENDS_WITH('Code-J1hw',SourceD.FieldG) AND
         STARTS_WITH('Amt-P0sh',SourceA.FieldE))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('CCY-Z8rz','Amt-mXTF') OR
         ENDS_WITH(SourceD.FieldC,'Customer-E1IE') AND
         ENDS_WITH('Address-qk2K',SourceD.FieldE) OR
         STARTS_WITH(SourceB.FieldG,'Amt-ftSD'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        ('CCY-8gBO' != SourceA.FieldC AND 'Code-jiXs' <> SourceA.FieldC AND
         STARTS_WITH('Account-EVpJ',SourceA.FieldC) OR
         'Amt-xfdQ' NOT LIKE SourceC.FieldD)
      LEFT JOIN testdataset.SourceE AS SourceE ON
        ('Customer-jke0' NOT LIKE SourceA.FieldI AND
         ENDS_WITH(SourceC.FieldD,SourceB.FieldI) AND
         STARTS_WITH(SourceA.FieldA,'Amt-YwaT') OR
         ENDS_WITH(SourceD.FieldA,'Fee-Uvmc'))
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Country-KpgW','CCY-Sy09') AND
         STARTS_WITH('Account-RRYa',SourceA.FieldD) AND
         ENDS_WITH(SourceB.FieldJ,'CCY-6naw') OR
         STARTS_WITH(DerivedTable1.AccountmNQo,'Fee-4KeU'))
      RIGHT JOIN Table6 AS Table6 ON
        (SourceA.FieldF != 'Country-G7FJ' AND 'Customer-LpLM' = 'Amt-t5e2' OR
         'CCY-Qpuw' <> SourceE.FieldA OR
         ENDS_WITH('Address-QbTu',SourceB.FieldE))
    WHERE
      STARTS_WITH(SourceD.FieldF,SourceD.FieldG) AND
      STARTS_WITH(DerivedTable1.Codeuhr9,'Address-oMa4') OR
      ENDS_WITH(SourceC.FieldD,SourceC.FieldD) OR
      'Address-gFnt' LIKE DerivedTable1.CustomerKrwR AND
      ENDS_WITH(DerivedTable1.CustomerKrwR,'Account-Y3k2') OR
      ENDS_WITH('Amt-I6Ft',Table6.FeeQVk4) AND
      STARTS_WITH(SourceD.FieldD,Table6.FeeQVk4) AND
      SourceC.FieldC = SourceA.FieldE
  ),
  Table8 AS (
    SELECT
      CASE
        WHEN ENDS_WITH('Amt-T2Kq',SourceA.FieldI) THEN
          IF(
            ENDS_WITH(SourceE.FieldB,DerivedTable1.CCYXSkS), SourceC.FieldB,
            'Customer-oM6N')
        WHEN 'Address-8DgN' <> 'Account-kldv' THEN
          IF(
            ENDS_WITH(SourceC.FieldC,SourceA.FieldJ), 'Address-GBpn',
            'Country-4thC')
        WHEN 'Customer-PTJN' LIKE 'Fee-pyUZ' THEN
          CASE
            WHEN 'Account-1uqa' != 'Account-r8q1' THEN
              'Country-yqiW'
            WHEN SourceC.FieldD NOT LIKE 'CCY-6tWD' THEN
              'Country-3BnY'
            WHEN ENDS_WITH(SourceD.FieldD,'CCY-Dk5u') THEN
              SourceE.FieldH
            WHEN ENDS_WITH(SourceE.FieldG,'CCY-APUn') THEN
              'CCY-zhvl'
            ELSE
              'Code-SpVD'
          END
        ELSE
          IF(ENDS_WITH('Code-iPTR','Code-D8YQ'),'Country-MCIw',SourceD.FieldD)
      END
        AS Accountwhr5,
      IF(
        SourceA.FieldA != 'CCY-LN0U',
        IF(ENDS_WITH(DerivedTable1.Codeuhr9,'Code-QlJc'),'Amt-RVnw','Fee-Tqkt'),
        CASE
          WHEN STARTS_WITH(DerivedTable1.AccountmNQo,'Code-clvH') THEN
            SourceE.FieldD
          WHEN ENDS_WITH(DerivedTable1.AddressoeLk,'Address-Qfxu') THEN
            DerivedTable1.FeerBIS
          ELSE
            SourceD.FieldG
        END)
        AS Country1eap,
      IF(
        ENDS_WITH(SourceC.FieldC,SourceC.FieldF),
        IF(
          'CCY-jiq9' != 'Address-z0ni', DerivedTable1.AccountqcVc,
          'Account-X49d'), IF(
                             'Account-1HIQ' != DerivedTable1.AccountKwSP,
                             SourceC.FieldJ, 'Address-69xd'))
        AS FeevX4G,
      IF(
        SourceA.FieldC NOT LIKE SourceA.FieldH,
        IF('Code-8iCd' != SourceE.FieldF,DerivedTable1.AccountqcVc,'Code-2uIK'),
        IF(
          SourceC.FieldD != 'CCY-JM1d', SourceC.FieldG,
          DerivedTable1.AccountqcVc))
        AS Country2Con,
      IF(
        ENDS_WITH('Code-FUlZ','Fee-Hrgc'),
        IF(SourceC.FieldI = 'Amt-VgEC','Amt-bMbS','Fee-XpBI'),
        IF(
          'Account-CjNI' NOT LIKE 'Address-YsZA', SourceA.FieldF,
          'Address-v3be'))
        AS AccountN1xg,
      IF(
        SourceE.FieldH LIKE SourceC.FieldE,
        IF('Account-UtbW' LIKE 'Account-inrr',SourceC.FieldD,'Fee-SRz2'),
        IF(
          STARTS_WITH(SourceA.FieldE,'Fee-eMX0'), DerivedTable1.AmtzEYq,
          SourceC.FieldA))
        AS AddressGB06,
      IF(
        SourceA.FieldG <> 'Account-ZOgq',
        IF(SourceE.FieldH != SourceC.FieldH,'Code-0xOo','Country-WTa3'),
        IF(
          DerivedTable1.AccountqcVc LIKE 'Customer-rIzF', 'Account-vTF5',
          DerivedTable1.AccountqcVc))
        AS AmtDtnl,
      CASE
        WHEN ENDS_WITH(SourceD.FieldH,SourceC.FieldI) THEN
          IF(
            ENDS_WITH(SourceE.FieldC,'Code-8iKM'), DerivedTable1.Addressrgjd,
            SourceA.FieldJ)
        WHEN STARTS_WITH(DerivedTable1.CustomerWrlz,'Country-qYKG') THEN
          IF(
            DerivedTable1.CustomerKrwR = 'Customer-DCnU', 'Address-bp0I',
            SourceC.FieldE)
        WHEN ENDS_WITH('Customer-X0hj','CCY-1BM0') THEN
          IF(
            STARTS_WITH('Country-0R5J',SourceD.FieldJ), SourceD.FieldI,
            'Customer-6JSd')
        WHEN 'Code-yDK7' <> SourceD.FieldH THEN
          IF(
            ENDS_WITH('Customer-Cb0P',SourceE.FieldD), SourceD.FieldD,
            SourceD.FieldF)
        ELSE
          IF(
            SourceC.FieldJ NOT LIKE 'Amt-UO34', DerivedTable1.AccountmNQo,
            SourceD.FieldG)
      END
        AS CountryrwbK,
      IF(
        'Code-6ZTG' <> 'Address-uR6G',
        IF(
          'Account-LrXA' LIKE 'Country-Cl7a', 'Country-FPLf',
          DerivedTable1.CustomerKrwR),
        CASE
          WHEN 'Fee-9xOp' LIKE 'Code-i1VN' THEN
            'Account-6jnp'
          WHEN STARTS_WITH('Fee-jQdK','Country-VkM8') THEN
            SourceA.FieldJ
          WHEN ENDS_WITH('Country-KptH','Address-0Pri') THEN
            SourceC.FieldA
          WHEN DerivedTable1.Addressrgjd LIKE SourceC.FieldH THEN
            'Country-EWIR'
          ELSE
            DerivedTable1.AccountqcVc
        END)
        AS CustomerBnBt,
      IF(
        ENDS_WITH('Code-lNLY',SourceA.FieldC),
        IF(
          DerivedTable1.CustomerWpfX LIKE SourceD.FieldB,
          DerivedTable1.AccountKwSP, SourceD.FieldA),
        IF(
          ENDS_WITH(SourceA.FieldC,DerivedTable1.AccountfEJK), 'CCY-Ei8R',
          DerivedTable1.AccountfEJK))
        AS Customerx3oj,
      CASE
        WHEN STARTS_WITH(SourceD.FieldF,'Fee-oZKq') THEN
          IF(
            DerivedTable1.AccountKwSP = SourceA.FieldB, SourceE.FieldA,
            DerivedTable1.FeerBIS)
        WHEN STARTS_WITH('Code-5RHq',SourceC.FieldA) THEN
          CASE
            WHEN ENDS_WITH(DerivedTable1.AccountqcVc,'Address-RjnQ') THEN
              SourceC.FieldG
            WHEN 'Customer-UzMK' LIKE SourceD.FieldD THEN
              'Address-f6wD'
            WHEN ENDS_WITH('Amt-ou7n','Account-zIfN') THEN
              'CCY-82H2'
            ELSE
              SourceD.FieldI
          END
        WHEN DerivedTable1.CustomerWpfX = 'Address-tAZb' THEN
          CASE
            WHEN STARTS_WITH(SourceC.FieldC,SourceC.FieldB) THEN
              'Country-3qGg'
            WHEN ENDS_WITH(DerivedTable1.CustomerWpfX,'Country-xZDp') THEN
              SourceE.FieldE
            WHEN 'Customer-aaKl' != 'Customer-yCjX' THEN
              DerivedTable1.CountryV1io
            WHEN DerivedTable1.CountryV1io NOT LIKE 'Country-JemW' THEN
              'Customer-4mJr'
            ELSE
              SourceD.FieldC
          END
        WHEN STARTS_WITH('Account-MB4y','CCY-mfbP') THEN
          IF(
            STARTS_WITH('Account-lm3z',SourceC.FieldD), 'Country-yyjv',
            'Customer-Kgl4')
        ELSE
          CASE
            WHEN ENDS_WITH(DerivedTable1.CCYXSkS,'Address-T467') THEN
              SourceD.FieldH
            WHEN STARTS_WITH(SourceD.FieldJ,SourceA.FieldA) THEN
              'Country-L0SQ'
            WHEN ENDS_WITH('Country-8WSn',SourceD.FieldD) THEN
              'Fee-pbUB'
            ELSE
              SourceE.FieldI
          END
      END
        AS AmtC07O,
      IF(
        ENDS_WITH(DerivedTable1.AccountqcVc,'CCY-7cbo'),
        IF('Country-ov2H' <> 'Amt-hGlO','Amt-53mD','CCY-S9jU'),
        IF(
          STARTS_WITH(DerivedTable1.CustomerKrwR,SourceC.FieldC),
          'Country-9VFQ', SourceE.FieldB))
        AS FeeOcas
    FROM
      testdataset.SourceE AS SourceE
      INNER JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(SourceE.FieldA,SourceE.FieldE) AND
         ENDS_WITH(SourceA.FieldF,SourceA.FieldJ) OR
         ENDS_WITH('CCY-CJHn','Account-pK5B') AND
         ENDS_WITH('Account-sVad',SourceA.FieldD))
      LEFT JOIN testdataset.SourceC AS SourceC ON
        (SourceC.FieldC = 'Account-7dvm' AND
         STARTS_WITH('Country-S3lC','Customer-4lAx') OR
         SourceC.FieldI LIKE SourceE.FieldJ OR 'Address-KPc3' != SourceE.FieldG)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH('Fee-dNcM','Account-TVD2') OR
         ENDS_WITH(SourceD.FieldE,SourceC.FieldC) AND
         'Address-xLtd' = 'Address-eyHb' AND
         STARTS_WITH('Country-q8mA',SourceD.FieldD))
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        ('Country-5gsL' NOT LIKE 'Amt-qkQL' OR SourceC.FieldA <> 'Fee-y64I' OR
         'Code-ywpS' NOT LIKE 'Code-apIU' AND 'Amt-ZUbh' <> 'Account-fS4L')
    WHERE
      STARTS_WITH(SourceA.FieldF,DerivedTable1.CustomerKrwR) OR
      SourceC.FieldB = SourceA.FieldE AND
      'Amt-py1g' NOT LIKE 'Amt-Wrah' OR 'Country-TLon' <> 'Country-GulL' AND
      STARTS_WITH(SourceD.FieldC,'Customer-nArz') AND
      ENDS_WITH('CCY-iMsG',SourceC.FieldB) AND
      ENDS_WITH('CCY-A6Ix','Address-FVCn') OR 'Account-n8JU' NOT LIKE 'CCY-jJ5w'
  ),
  Table9 AS (
    SELECT
      IF(
        'Customer-5bA0' NOT LIKE 'CCY-EjKb',
        IF(
          STARTS_WITH('Country-bZAo',DerivedTable1.CustomerWrlz), 'Fee-28B3',
          'Account-goAq'), IF(
                             STARTS_WITH('Code-v4YQ',Table7.FeeYXiQ),
                             'CCY-qinN', DerivedTable1.AmtR86V))
        AS CustomerZYgO,
      IF(
        Table6.CCYz1qA NOT LIKE 'Code-AD33',
        IF('Address-qN3z' <> 'Customer-FMKj',Table8.CustomerBnBt,'Fee-sE0L'),
        IF(
          STARTS_WITH('Address-1phh',SourceD.FieldA), SourceD.FieldB,
          Table7.FeeB8Wr))
        AS FeevFXW,
      IF(
        STARTS_WITH(Table8.AddressGB06,'Customer-W1Pg'),
        IF(
          STARTS_WITH(Table8.CustomerBnBt,'Country-e2LU'), 'Country-C2Jt',
          'Customer-MIVH'),
        IF('Address-8yip' = 'Address-4rI2',Table7.FeeYXiQ,'Code-VKH0'))
        AS FeeYo1s,
      IF(
        ENDS_WITH(SourceE.FieldJ,SourceE.FieldJ),
        IF(
          'Fee-GQr0' NOT LIKE 'Customer-MY6Z', Table6.CountryeNab,
          'Country-HAbY'), IF(
                             'Address-HUsC' NOT LIKE DerivedTable1.AmtzEYq,
                             SourceA.FieldC, 'Amt-AtS7'))
        AS Fee3ieD,
      CASE
        WHEN 'Account-PA75' LIKE 'Amt-I1yj' THEN
          IF(
            ENDS_WITH(SourceE.FieldJ,Table7.Customer7fcU), 'Account-Xroy',
            'Amt-qoHL')
        WHEN Table7.FeeYXiQ = SourceA.FieldG THEN
          CASE
            WHEN STARTS_WITH(DerivedTable1.CountryV1io,'Code-JT97') THEN
              'Amt-HHTm'
            WHEN 'Customer-QyEz' = 'CCY-YfTs' THEN
              'CCY-2VTv'
            ELSE
              'Address-mXi7'
          END
        WHEN STARTS_WITH(Table7.Account6ZJO,'CCY-qxFC') THEN
          IF(
            STARTS_WITH(SourceE.FieldH,DerivedTable1.CustomerKrwR),
            'Account-axWb', 'Code-BKGM')
        WHEN 'Amt-07KV' <> Table7.CCY3lvo THEN
          IF(
            STARTS_WITH('Country-n3jt','CCY-PiGh'), 'Address-SyVu',
            'Address-HbPC')
        WHEN ENDS_WITH('Code-h4Hm',SourceA.FieldJ) THEN
          IF(SourceA.FieldC <> 'Country-ZepE',SourceB.FieldC,SourceD.FieldH)
        ELSE
          IF(
            STARTS_WITH(Table7.AddresshO4t,SourceB.FieldE), 'Address-T9wv',
            SourceE.FieldJ)
      END
        AS CCYMAKQ,
      CASE
        WHEN 'CCY-kHvk' = 'Account-Tzyp' THEN
          IF(
            Table8.Country1eap NOT LIKE SourceB.FieldE, 'Code-d89F',
            Table6.AddresslEfL)
        WHEN STARTS_WITH(SourceE.FieldG,'Fee-LEya') THEN
          IF(
            STARTS_WITH(DerivedTable1.CountryV1io,'CCY-Ptjr'), SourceB.FieldG,
            SourceA.FieldH)
        ELSE
          IF('Customer-Cs2j' = 'Customer-F1GP','Address-2evn','Amt-cY8R')
      END
        AS Address8ZTN,
      IF(
        STARTS_WITH(SourceD.FieldJ,Table6.AccountOHQa),
        IF(
          STARTS_WITH('Amt-Y9Ge',SourceA.FieldI), Table6.AddresslEfL,
          'Address-NvFg'),
        IF(ENDS_WITH(SourceA.FieldA,'Code-vWHp'),'Fee-uc7a','Address-ZKxa'))
        AS AddressP7Ov,
      CASE
        WHEN 'Address-LGXj' NOT LIKE 'Address-2oDD' THEN
          IF(
            Table6.CustomerYwXe = DerivedTable1.CountryV1io, SourceD.FieldB,
            'CCY-U5bL')
        WHEN 'CCY-7d2a' <> 'Fee-6aCO' THEN
          IF(
            SourceB.FieldJ NOT LIKE Table8.Country2Con, SourceD.FieldF,
            'CCY-kvLx')
        WHEN STARTS_WITH(SourceA.FieldD,SourceA.FieldE) THEN
          CASE
            WHEN ENDS_WITH('Fee-rqEG','Code-nlCJ') THEN
              SourceD.FieldA
            WHEN STARTS_WITH('Address-yXgH',SourceA.FieldF) THEN
              'Amt-4ifp'
            ELSE
              'Customer-DoIs'
          END
        WHEN DerivedTable1.AccountfEJK <> 'Customer-yvIr' THEN
          IF(
            STARTS_WITH('Address-5SBA','Code-IF14'), 'Account-uVwM',
            Table7.CCY3lvo)
        WHEN 'Code-Uf0w' = 'Fee-iXR4' THEN
          CASE
            WHEN STARTS_WITH(DerivedTable1.CountryV1io,'Code-4fM8') THEN
              'Code-TPKu'
            WHEN STARTS_WITH('Code-MfXk',Table6.FeeQVk4) THEN
              'CCY-3cy5'
            WHEN STARTS_WITH(Table6.CustomerYwXe,SourceD.FieldC) THEN
              SourceA.FieldJ
            ELSE
              'Country-WNaa'
          END
        ELSE
          IF('Amt-gOJf' != 'Fee-cUdi','Account-Mw8e','CCY-wHAd')
      END
        AS AddressvX67,
      IF(
        STARTS_WITH('Code-43Ux','Customer-WDwN'),
        IF(
          ENDS_WITH(Table8.FeevX4G,SourceD.FieldD), 'Account-DWnI',
          'Account-43md'),
        CASE
          WHEN STARTS_WITH(Table6.AddresslEfL,'Country-U5UZ') THEN
            SourceE.FieldG
          WHEN ENDS_WITH(DerivedTable1.CustomerWpfX,SourceD.FieldA) THEN
            'Code-uYwF'
          WHEN ENDS_WITH('CCY-WxxD',Table7.AmtC0Ga) THEN
            DerivedTable1.CustomerKrwR
          WHEN DerivedTable1.FeerBIS != 'CCY-YB1k' THEN
            SourceB.FieldA
          WHEN SourceD.FieldI != Table6.AddressO6QX THEN
            Table6.AccountOHQa
          ELSE
            'CCY-wFBy'
        END)
        AS CCYo9kE,
      IF(
        STARTS_WITH('Address-6LTu','Country-EviW'),
        CASE
          WHEN 'Country-m2x1' != DerivedTable1.CCYXSkS THEN
            'Customer-XNG4'
          WHEN ENDS_WITH('Customer-BpvH','CCY-Cxyx') THEN
            SourceA.FieldI
          WHEN STARTS_WITH('Amt-gFcE','Account-Abpk') THEN
            SourceE.FieldI
          WHEN ENDS_WITH(Table8.Country1eap,SourceE.FieldA) THEN
            SourceB.FieldH
          WHEN 'Country-OYqu' LIKE DerivedTable1.CustomerWrlz THEN
            SourceB.FieldC
          ELSE
            SourceA.FieldA
        END, IF(ENDS_WITH('Amt-48V7',SourceB.FieldF),'CCY-NC3O','Code-EJ5j'))
        AS AmtmqmN,
      IF(
        'Fee-Y1eJ' = SourceA.FieldJ,
        IF(
          DerivedTable1.AccountqcVc != DerivedTable1.FeerBIS,
          DerivedTable1.CCYXSkS, Table8.AmtC07O),
        IF('Code-ZYDa' <> 'Fee-DmiO',Table7.Accounth5Rx,'CCY-ii7Y'))
        AS AmtHEpD,
      IF(
        SourceE.FieldA NOT LIKE SourceD.FieldF,
        IF(
          ENDS_WITH(Table7.AddressWs4z,'Account-8S1n'),
          DerivedTable1.CustomerWrlz, SourceE.FieldA),
        IF(
          ENDS_WITH('Customer-rCx6',Table7.AddressWs4z), 'Amt-rsvh',
          Table6.AddressO6QX))
        AS CustomerM9TQ,
      IF(
        STARTS_WITH(SourceB.FieldC,'Amt-Jrvw'),
        IF('CCY-4IOI' != 'Address-6vD7',SourceE.FieldB,'Fee-Yx3U'),
        CASE
          WHEN 'CCY-Ntww' <> 'Country-hLlG' THEN
            'Account-mS03'
          WHEN 'Code-ace5' = SourceE.FieldI THEN
            'Customer-qzv8'
          ELSE
            SourceA.FieldE
        END)
        AS CustomerUlbe,
      IF(
        ENDS_WITH(Table7.AmtC0Ga,DerivedTable1.AddressoeLk),
        CASE
          WHEN STARTS_WITH(Table6.FeeQVk4,'Code-OIyL') THEN
            'CCY-UMAo'
          WHEN ENDS_WITH(DerivedTable1.AddressoeLk,'CCY-X9hh') THEN
            DerivedTable1.CountryV1io
          ELSE
            'CCY-uGxy'
        END, IF(
               ENDS_WITH('Amt-E2Wq',SourceB.FieldI), Table6.CustomerUs4L,
               Table7.Customer7fcU))
        AS CodelStE,
      IF(
        ENDS_WITH(SourceB.FieldB,DerivedTable1.AccountmNQo),
        IF(
          STARTS_WITH(Table7.FeeYXiQ,'CCY-jxQ7'), Table7.Customer7fcU,
          Table8.AddressGB06),
        IF(ENDS_WITH(SourceA.FieldA,SourceA.FieldI),'Address-x7sN','Fee-eE9v'))
        AS CodeHvwl,
      IF(
        STARTS_WITH(SourceB.FieldC,SourceD.FieldD),
        IF(DerivedTable1.CCYXSkS = SourceE.FieldA,'Fee-axhK',SourceB.FieldC),
        IF(Table8.Country2Con = 'Account-eqlm','Address-7vyO','Amt-jGat'))
        AS CountryyPOa
    FROM
      testdataset.DerivedTable1 AS DerivedTable1
      INNER JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(DerivedTable1.AccountqcVc,DerivedTable1.AccountqcVc) AND
         STARTS_WITH(DerivedTable1.Addressrgjd,'Amt-hljM') AND
         ENDS_WITH('Country-WceL','Code-33lf') OR
         'Amt-U5PR' LIKE DerivedTable1.CustomerWpfX)
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Fee-A5ok','Amt-at54') OR 'Fee-wtyr' LIKE SourceB.FieldF OR
         SourceA.FieldA <> DerivedTable1.AddressoeLk AND
         DerivedTable1.CustomerKrwR LIKE SourceB.FieldE)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (SourceD.FieldF LIKE SourceD.FieldI OR SourceB.FieldI <> 'Fee-EChy' OR
         ENDS_WITH(DerivedTable1.AddressoeLk,DerivedTable1.AccountqcVc) OR
         ENDS_WITH(DerivedTable1.AddressoeLk,SourceB.FieldB))
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH(SourceE.FieldH,'Customer-5jfY') OR
         STARTS_WITH(SourceB.FieldF,SourceE.FieldB) AND
         ENDS_WITH('Address-2NvT',DerivedTable1.CustomerKrwR) AND
         STARTS_WITH(SourceE.FieldJ,'Account-Cn5K'))
      INNER JOIN Table6 AS Table6 ON
        ('Account-Gj9b' <> 'CCY-sjz3' OR STARTS_WITH('Fee-tpyh','Code-3pn1') OR
         SourceA.FieldF != SourceB.FieldC AND
         STARTS_WITH('Amt-vJCC',DerivedTable1.AccountKwSP))
      INNER JOIN Table7 AS Table7 ON
        (ENDS_WITH('Address-8ypa','Account-87Gm') OR
         'Customer-ukxR' != SourceB.FieldB OR
         STARTS_WITH('CCY-PDue','Country-RMyq') OR
         ENDS_WITH('CCY-dbiP','Account-NJ00'))
      RIGHT JOIN Table8 AS Table8 ON
        (ENDS_WITH('CCY-RjCH','Amt-JyIg') AND
         DerivedTable1.Addressrgjd NOT LIKE SourceE.FieldG OR
         SourceA.FieldG = 'CCY-2nOP' AND 'Fee-L3MU' <> Table8.AmtDtnl)
    WHERE
      ENDS_WITH('Account-vgu8',Table8.AddressGB06) AND
      ENDS_WITH('Country-R1GH',Table8.AmtC07O) AND
      STARTS_WITH('Amt-57hE','Fee-zRNa') OR
      STARTS_WITH(DerivedTable1.Codeuhr9,'Country-hq2W') OR
      ENDS_WITH('Address-SfGL',Table7.AmtC0Ga) OR
      STARTS_WITH('Country-2diy',SourceB.FieldF) OR
      'Country-tDj1' LIKE Table6.AddresslEfL AND
      STARTS_WITH(Table7.AddressqiWG,DerivedTable1.AccountfEJK)
  ),
  Table10 AS (
    SELECT
      IF(
        ENDS_WITH(Table9.CodeHvwl,'Account-eqcl'),
        IF(
          STARTS_WITH('CCY-YNuc','Customer-Mw81'), Table8.AccountN1xg,
          'Fee-9O9m'), IF(
                         Table6.AccountOHQa != 'Country-gaug',
                         DerivedTable1.CustomerWrlz, DerivedTable1.AddressoeLk))
        AS Country9DSP,
      IF(
        Table9.CustomerZYgO = 'Address-gjnE',
        IF(
          STARTS_WITH(SourceB.FieldF,'Customer-nLzJ'), SourceA.FieldF,
          'Customer-aiil'),
        CASE
          WHEN 'Address-hDLk' <> 'Account-TEbR' THEN
            SourceD.FieldF
          WHEN SourceA.FieldD <> Table8.Country1eap THEN
            'Account-fpH7'
          ELSE
            'Fee-66Fy'
        END)
        AS AmtwNIF,
      IF(
        'CCY-LkQk' LIKE 'Customer-KkU5',
        IF('Address-p7pI' LIKE 'Address-Xp2z','Customer-g4tv','Amt-4K9i'),
        IF(
          STARTS_WITH(SourceB.FieldC,SourceD.FieldD), 'Address-rM2u',
          SourceA.FieldE))
        AS CCYfX37,
      IF(
        Table9.AmtHEpD != 'Fee-YJuM',
        IF(
          ENDS_WITH(Table9.CodeHvwl,SourceD.FieldC), Table9.CodeHvwl,
          'CCY-WX9h'),
        IF(ENDS_WITH('Code-jvqy','Code-GK4o'),'CCY-YkJM',Table8.FeeOcas))
        AS Address19HL,
      IF(
        'Customer-wDqg' != DerivedTable1.CCYXSkS,
        IF('Account-q8uq' <> 'Customer-Dr8H','Address-PMSg',SourceD.FieldE),
        IF('Amt-LPqE' = 'Amt-L1bB','Code-XCtf',SourceA.FieldJ))
        AS Customerbyfz,
      IF(
        ENDS_WITH('Customer-y7zL',SourceB.FieldH),
        IF(
          STARTS_WITH(DerivedTable1.CountryV1io,Table8.Country1eap), 'Amt-uYDW',
          SourceD.FieldD), IF(
                             ENDS_WITH(SourceB.FieldA,'Account-i1TK'),
                             'Code-hVIj', DerivedTable1.Addressrgjd))
        AS CountryR4vn,
      IF(
        'Customer-PCB3' = SourceC.FieldG,
        IF(
          Table7.AddresshO4t NOT LIKE Table7.CountrybN0v, 'Account-0GTA',
          SourceD.FieldJ),
        IF(STARTS_WITH('Address-c530',SourceD.FieldD),'Code-Qa3F','Code-4bLx'))
        AS AccountsICS,
      IF(
        ENDS_WITH('Account-Vrxd','Account-50q3'),
        IF('Code-q034' <> Table9.AddressvX67,'Fee-CJ6s',Table8.AddressGB06),
        IF(
          STARTS_WITH('Account-O9LZ',SourceB.FieldE), SourceA.FieldE,
          'Customer-iv07'))
        AS CountryDtsw,
      IF(
        STARTS_WITH('Code-cbwn',Table9.Address8ZTN),
        IF(
          'Country-1QKc' NOT LIKE 'Account-iqT7', Table9.CodeHvwl,
          SourceC.FieldD),
        CASE
          WHEN ENDS_WITH('Address-2PBt',Table7.FeeYXiQ) THEN
            DerivedTable1.CountryV1io
          WHEN STARTS_WITH('Fee-vcyt','Amt-JrCg') THEN
            Table8.CountryrwbK
          WHEN STARTS_WITH('Address-novI','Customer-XwUX') THEN
            'Fee-323S'
          ELSE
            'CCY-Hljx'
        END)
        AS AddresscfOk,
      IF(
        DerivedTable1.CustomerKrwR = 'Account-O3vK',
        IF('Country-bYyK' NOT LIKE 'CCY-4SKI',Table7.AmtPg6E,'Customer-8CE3'),
        IF(
          Table6.CountryeNab NOT LIKE Table9.Address8ZTN, Table7.AddressWs4z,
          SourceD.FieldB))
        AS FeeXuMg,
      IF(
        STARTS_WITH('Country-WBDq',Table7.Accounth5Rx),
        IF(
          ENDS_WITH(Table8.Country1eap,Table8.AccountN1xg), 'Code-6Mg0',
          'Fee-kpIb'), IF(
                         'Address-EXg0' <> DerivedTable1.AccountqcVc,
                         SourceA.FieldC, Table7.AddressWs4z))
        AS Amt5Ryh,
      IF(
        'Code-7OsQ' = SourceA.FieldB,
        IF(Table8.AddressGB06 = 'Amt-sroP','CCY-z4n3',Table9.Address8ZTN),
        IF(STARTS_WITH('Fee-oNOP','CCY-FHyZ'),'Country-1E1H',SourceB.FieldA))
        AS CustomeretMM,
      IF(
        'Amt-21Um' = Table7.FeeAEBz,
        IF(
          STARTS_WITH(SourceE.FieldJ,SourceE.FieldH), SourceC.FieldF,
          'Address-33Uv'),
        IF(STARTS_WITH('Code-us9o','Address-AZM4'),SourceE.FieldD,'CCY-srXp'))
        AS CustomerQ22F,
      IF(
        SourceA.FieldD <> Table6.Codeju07,
        IF('Amt-ewJs' <> 'Amt-apqh','Fee-rNfa','CCY-pXP1'),
        CASE
          WHEN ENDS_WITH('Country-tbta','CCY-JH4Y') THEN
            SourceC.FieldI
          WHEN ENDS_WITH('Fee-Ui5r',Table7.FeeYXiQ) THEN
            SourceC.FieldE
          WHEN 'CCY-pFHv' NOT LIKE 'Address-uGcM' THEN
            'Customer-RBC8'
          WHEN 'Country-kDfn' != 'Customer-2Ew5' THEN
            Table9.Address8ZTN
          WHEN STARTS_WITH(SourceA.FieldD,'CCY-E998') THEN
            'CCY-ylby'
          ELSE
            'Customer-2kE4'
        END)
        AS CustomerviTc,
      IF(
        ENDS_WITH('Account-BKy4',DerivedTable1.AccountfEJK),
        CASE
          WHEN 'Code-RZta' LIKE 'Fee-JZlv' THEN
            Table9.CCYMAKQ
          WHEN 'CCY-sJeo' = 'Fee-7VPZ' THEN
            'CCY-p53R'
          WHEN ENDS_WITH(Table7.AccountNi89,'Fee-C019') THEN
            DerivedTable1.Codeuhr9
          WHEN ENDS_WITH('Amt-kykG',SourceD.FieldA) THEN
            SourceD.FieldJ
          WHEN 'Country-hHoq' = 'Fee-3NAr' THEN
            SourceC.FieldB
          ELSE
            SourceA.FieldH
        END, IF(
               ENDS_WITH(SourceA.FieldA,'Country-IDW3'),
               DerivedTable1.CustomerWpfX, 'Customer-rTiU'))
        AS AddressxOKB
    FROM
      Table8 AS Table8
      INNER JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH('Amt-10oJ','Amt-ZQxt') AND
         SourceA.FieldC LIKE 'Account-2UdT' OR
         Table8.AccountN1xg LIKE 'Fee-OXPx' OR
         ENDS_WITH(SourceA.FieldA,SourceA.FieldC))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('CCY-0Yaw','Customer-OzLa') AND
         STARTS_WITH(SourceB.FieldG,Table8.FeeOcas) OR
         SourceA.FieldJ = Table8.Accountwhr5 AND
         Table8.FeeOcas LIKE Table8.AddressGB06)
      INNER JOIN testdataset.SourceC AS SourceC ON
        (SourceA.FieldH = Table8.Country1eap AND 'Address-Yb7O' = 'CCY-xSqz' AND
         ENDS_WITH(SourceA.FieldA,SourceC.FieldI) OR
         STARTS_WITH('Fee-GmuG',SourceB.FieldD))
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Code-ai6T','CCY-j1dA') OR 'Amt-Gjhk' <> 'Address-mEz7' AND
         ENDS_WITH('Amt-Og19',SourceA.FieldH) OR
         ENDS_WITH(SourceB.FieldD,SourceC.FieldC))
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('Amt-Iv4E','Code-kl4J') OR
         SourceD.FieldB != Table8.AmtDtnl OR
         ENDS_WITH(SourceB.FieldH,'Address-1vKH') AND
         SourceD.FieldJ = 'Country-1QNP')
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (Table8.CustomerBnBt != 'Code-FvLD' OR
         STARTS_WITH('CCY-NMQx','Customer-LLOz') OR
         ENDS_WITH(SourceB.FieldG,'Code-deix') AND
         STARTS_WITH('Account-Inss',DerivedTable1.AmtzEYq))
      INNER JOIN Table6 AS Table6 ON
        (Table6.AddressO6QX LIKE 'Country-BUtl' AND
         ENDS_WITH(SourceE.FieldC,SourceD.FieldG) OR
         'Amt-4DsJ' != SourceA.FieldC AND 'Account-AGF1' = 'Account-To7q')
      LEFT JOIN Table7 AS Table7 ON
        (SourceE.FieldE = Table7.AddressqiWG OR
         STARTS_WITH(SourceE.FieldB,Table7.FeeAEBz) OR
         SourceD.FieldG LIKE 'Country-k9Vw' AND
         'Address-GnNq' <> 'Address-LaJ3')
      INNER JOIN Table9 AS Table9 ON
        (ENDS_WITH('Account-xuP8','Fee-L7rp') OR
         STARTS_WITH('CCY-h9F1',SourceD.FieldJ) OR
         STARTS_WITH(DerivedTable1.CustomerWrlz,SourceA.FieldB) OR
         SourceB.FieldF LIKE SourceC.FieldD)
    WHERE
      'Customer-h8Ss' <> 'Fee-y9lQ' AND ENDS_WITH('Amt-r6Al',SourceA.FieldC) AND
      SourceA.FieldH = 'CCY-v4M5' OR SourceC.FieldA != 'Amt-12GR' AND
      STARTS_WITH(Table9.CCYMAKQ,Table8.Accountwhr5) AND
      Table9.CustomerM9TQ NOT LIKE 'Customer-6H3x' OR
      'Code-ZCRq' != 'Code-AFxJ' OR SourceE.FieldF <> 'Address-Sbaz'
  )
SELECT
  IF(
    Table6.Codeju07 != Table7.AccountNi89,
    IF(
      STARTS_WITH(SourceC.FieldA,Table7.AddresshO4t), 'Address-K0S7',
      'Code-ChaN'), IF('Fee-ZlJh' = 'Fee-nW1F',Table7.AddresshO4t,'CCY-oPrD'))
    AS AccountctCy,
  IF(
    'CCY-S3rB' = 'Customer-Bg0D',
    CASE
      WHEN STARTS_WITH('Code-TD0n','Account-tlgs') THEN
        'CCY-SfrX'
      WHEN Table9.FeeYo1s != SourceC.FieldC THEN
        'Amt-zO8d'
      WHEN Table9.CCYo9kE LIKE Table6.AddresslEfL THEN
        SourceC.FieldC
      WHEN Table8.AmtC07O <> 'Fee-RlxZ' THEN
        SourceE.FieldF
      ELSE
        Table6.CodeKYmV
    END,
    CASE
      WHEN Table7.CountrybN0v LIKE 'Country-P0Xd' THEN
        'Customer-DHDU'
      WHEN SourceB.FieldE != 'Code-xhvX' THEN
        'Country-IU4e'
      WHEN 'CCY-eHCT' = Table6.Codeju07 THEN
        SourceA.FieldJ
      WHEN ENDS_WITH('Country-kNm3',DerivedTable1.AmtR86V) THEN
        'Account-UVJr'
      WHEN STARTS_WITH('Customer-JpDI',SourceB.FieldJ) THEN
        Table7.Accounth5Rx
      ELSE
        Table6.AddressO6QX
    END)
    AS Account61kv,
  IF(
    STARTS_WITH(Table7.FeeB8Wr,'CCY-hqiU'),
    CASE
      WHEN STARTS_WITH(SourceE.FieldA,'Code-ScOS') THEN
        'Address-x4Su'
      WHEN STARTS_WITH('Account-kta5','Address-L6fV') THEN
        SourceC.FieldG
      WHEN Table6.CustomerYwXe LIKE Table8.CountryrwbK THEN
        'Amt-BdOr'
      WHEN 'Code-Lid9' NOT LIKE DerivedTable1.AccountmNQo THEN
        DerivedTable1.AccountKwSP
      WHEN STARTS_WITH('Account-PVGg',Table9.FeevFXW) THEN
        Table8.Country2Con
      ELSE
        DerivedTable1.Addressrgjd
    END, IF(
           STARTS_WITH('Country-HPWF',DerivedTable1.CustomerKrwR),
           'Account-qdOI', 'Amt-o8ZI'))
    AS AccounttVM1,
  CASE
    WHEN STARTS_WITH(Table6.CustomerUs4L,Table7.Accounth5Rx) THEN
      CASE
        WHEN ENDS_WITH('Code-wIVy','CCY-l9Ep') THEN
          DerivedTable1.CCYXSkS
        WHEN 'Code-QjJF' NOT LIKE 'Fee-9K58' THEN
          'CCY-lTGB'
        ELSE
          Table9.FeeYo1s
      END
    WHEN STARTS_WITH('Country-Ks3X','Amt-uz6W') THEN
      IF(
        STARTS_WITH('Code-BpRD','Account-kDyt'), DerivedTable1.AccountKwSP,
        SourceE.FieldD)
    WHEN STARTS_WITH(Table6.CustomerUs4L,DerivedTable1.CountryV1io) THEN
      IF(
        STARTS_WITH(SourceC.FieldD,'Customer-LZbD'), SourceB.FieldA,
        'Country-tyM5')
    ELSE
      IF(
        STARTS_WITH('Amt-vz2J',SourceA.FieldA), DerivedTable1.AccountKwSP,
        DerivedTable1.FeerBIS)
  END
    AS Address7oYI,
  CASE
    WHEN STARTS_WITH('Amt-YW75','Country-dCnV') THEN
      IF(ENDS_WITH(Table9.CodelStE,SourceC.FieldA),'Fee-WqM5','Customer-HgEy')
    WHEN ENDS_WITH('Account-7GUe',SourceA.FieldC) THEN
      IF(
        STARTS_WITH(Table7.FeeYXiQ,SourceC.FieldB), Table7.AddressqiWG,
        'Fee-tAAK')
    WHEN Table9.AmtmqmN != SourceE.FieldC THEN
      IF(SourceA.FieldJ <> 'Code-y1g1',SourceC.FieldI,'Code-nTWy')
    WHEN ENDS_WITH(SourceB.FieldJ,'Amt-3kk4') THEN
      IF(
        'CCY-ciCF' NOT LIKE DerivedTable1.AddressoeLk, 'Country-IG4g',
        SourceA.FieldH)
    ELSE
      CASE
        WHEN STARTS_WITH(DerivedTable1.CustomerWpfX,Table7.AddressqiWG) THEN
          'Account-nYvS'
        WHEN ENDS_WITH('Account-h3EV',SourceE.FieldA) THEN
          Table9.CodelStE
        ELSE
          Table8.CustomerBnBt
      END
  END
    AS Amtp4ee,
  IF(
    ENDS_WITH(SourceA.FieldB,'CCY-eAVE'),
    IF(Table6.AccountOHQa != Table7.FeeYXiQ,'Amt-Tif1','CCY-zXup'),
    IF(ENDS_WITH(SourceC.FieldE,'Address-cf0d'),SourceA.FieldB,'Account-c7ks'))
    AS CountrywaWi,
  CASE
    WHEN Table6.CountryeNab LIKE 'Code-c8pg' THEN
      CASE
        WHEN 'Address-dDc8' LIKE 'Country-LgrI' THEN
          'Fee-gngO'
        WHEN Table9.CountryyPOa NOT LIKE SourceE.FieldA THEN
          'Country-RHet'
        ELSE
          'Customer-SKCM'
      END
    WHEN SourceC.FieldH LIKE Table9.CCYMAKQ THEN
      IF(
        ENDS_WITH(Table8.CustomerBnBt,'Amt-67OC'), SourceA.FieldE,
        Table7.Account6ZJO)
    ELSE
      IF(Table7.AmtC0Ga != 'Amt-rxcY','Address-Gdug','Amt-U2t4')
  END
    AS Amt885g,
  IF(
    STARTS_WITH(SourceC.FieldH,SourceC.FieldA),
    IF(ENDS_WITH('Amt-B0JM',Table9.FeeYo1s),'Fee-regz','Fee-3ExX'),
    IF(
      DerivedTable1.AccountfEJK <> SourceE.FieldI, 'Account-GbLl',
      DerivedTable1.AddressoeLk))
    AS CodeDmva,
  IF(
    STARTS_WITH('Country-Bs0P',Table7.Account6ZJO),
    CASE
      WHEN ENDS_WITH(DerivedTable1.CustomerWrlz,Table8.AccountN1xg) THEN
        'CCY-Caol'
      WHEN STARTS_WITH(DerivedTable1.AccountqcVc,Table7.Account6ZJO) THEN
        'Fee-t3mZ'
      WHEN ENDS_WITH(Table9.Fee3ieD,SourceA.FieldA) THEN
        'Customer-BbwX'
      WHEN 'Address-DXbz' != 'Amt-efB0' THEN
        'CCY-NJvr'
      WHEN ENDS_WITH(DerivedTable1.AccountKwSP,'Customer-JIE8') THEN
        DerivedTable1.CustomerWpfX
      ELSE
        Table7.FeeB8Wr
    END,
    CASE
      WHEN STARTS_WITH(SourceA.FieldI,SourceE.FieldH) THEN
        Table7.Accounth5Rx
      WHEN 'Address-eEea' = 'Customer-7n32' THEN
        'Country-3bdI'
      WHEN Table9.FeevFXW = Table8.Country2Con THEN
        'CCY-1ZRn'
      WHEN STARTS_WITH('Code-2aX7','CCY-tLJU') THEN
        'Fee-6pCW'
      WHEN STARTS_WITH('Fee-acO8','Amt-96Jq') THEN
        DerivedTable1.FeerBIS
      ELSE
        DerivedTable1.CCYXSkS
    END)
    AS AccountrJZf,
  IF(
    STARTS_WITH(SourceA.FieldA,DerivedTable1.AccountqcVc),
    CASE
      WHEN 'Account-Cp8I' LIKE 'Country-ifC2' THEN
        'Account-ocp2'
      WHEN 'Account-fqDW' NOT LIKE 'Account-wW9r' THEN
        'Account-gJbM'
      WHEN ENDS_WITH('Account-mUeW',Table8.AddressGB06) THEN
        'Customer-ZSsN'
      WHEN 'Account-URXC' LIKE 'Country-koko' THEN
        Table9.CCYMAKQ
      WHEN ENDS_WITH(SourceB.FieldE,'Account-2LIG') THEN
        DerivedTable1.AccountqcVc
      ELSE
        Table8.Country1eap
    END,
    CASE
      WHEN Table9.AddressvX67 NOT LIKE Table6.CountryeNab THEN
        'Code-ObaD'
      WHEN STARTS_WITH('Amt-cHvA','Customer-eAzS') THEN
        Table8.Country1eap
      WHEN 'Fee-6XXy' != Table6.CustomerUs4L THEN
        'Fee-eJnI'
      WHEN 'CCY-A7Od' LIKE Table9.FeevFXW THEN
        SourceE.FieldB
      WHEN 'Account-qUq3' NOT LIKE DerivedTable1.CustomerWrlz THEN
        SourceC.FieldD
      ELSE
        Table7.AmtC0Ga
    END)
    AS CodeRziT,
  IF(
    DerivedTable1.CountryV1io != SourceE.FieldI,
    IF(
      STARTS_WITH(SourceB.FieldJ,'Country-81rW'), Table9.CCYMAKQ,
      SourceA.FieldH),
    IF('Fee-Hh7g' NOT LIKE 'Amt-iRkw',Table6.CodeKYmV,Table7.Account6ZJO))
    AS AddressHdnu,
  IF(
    ENDS_WITH(SourceA.FieldE,Table7.AddressqiWG),
    IF('Country-Eqro' = DerivedTable1.FeerBIS,Table9.CCYo9kE,'Customer-dWHj'),
    IF(STARTS_WITH(SourceE.FieldE,'CCY-b4pb'),'CCY-IVys',Table6.CCYz1qA))
    AS AddresscM17,
  IF(
    'Amt-iwYD' LIKE 'Code-7Ley',
    IF(ENDS_WITH('Country-GCeD',Table8.Accountwhr5),Table8.AmtDtnl,'Fee-HRdP'),
    IF('Customer-s9vo' != Table6.CountryeNab,Table7.CCY3lvo,'Fee-NVvz'))
    AS AmtNncS,
  IF(
    STARTS_WITH('CCY-SVAa',SourceA.FieldJ),
    IF('Code-4h9p' != Table8.AddressGB06,'Customer-lWNy',SourceC.FieldF),
    IF(ENDS_WITH('CCY-Qreo',Table7.FeeAEBz),'Fee-QKSY',Table7.AmtC0Ga))
    AS AmtmorU,
  CASE
    WHEN SourceC.FieldA = 'Address-PRW9' THEN
      IF(Table9.CodelStE != Table8.Customerx3oj,Table8.Country1eap,'Code-RAui')
    WHEN STARTS_WITH('Amt-QB6U',SourceB.FieldJ) THEN
      IF(
        STARTS_WITH(DerivedTable1.Codeuhr9,'Code-EF9X'), 'Address-dBM1',
        'CCY-thxw')
    ELSE
      CASE
        WHEN Table8.Country2Con = SourceB.FieldG THEN
          'Fee-si1v'
        WHEN DerivedTable1.AccountfEJK LIKE 'Code-KgqH' THEN
          SourceC.FieldC
        WHEN 'Fee-LIVN' LIKE 'Country-UyVG' THEN
          Table9.CCYo9kE
        WHEN STARTS_WITH(Table7.AmtPg6E,'Code-agNP') THEN
          Table9.AddressP7Ov
        WHEN STARTS_WITH(Table9.FeevFXW,Table6.CountryMbVx) THEN
          'Address-yJtL'
        ELSE
          Table8.Country2Con
      END
  END
    AS Address5RWP
FROM
  testdataset.SourceB AS SourceB
  RIGHT JOIN testdataset.SourceA AS SourceA ON
    (ENDS_WITH('Amt-Gvb9',SourceA.FieldF) OR
     'Customer-tceA' != SourceA.FieldI AND
     SourceB.FieldD NOT LIKE SourceB.FieldC OR 'Code-7wBE' LIKE 'Address-OKFK')
  RIGHT JOIN testdataset.SourceC AS SourceC ON
    ('Customer-d0T7' LIKE 'Fee-WMfb' OR
     STARTS_WITH('Customer-I8JM',SourceA.FieldC) OR
     STARTS_WITH(SourceB.FieldH,SourceC.FieldI) OR
     SourceC.FieldA NOT LIKE 'Address-xSUS')
  LEFT JOIN testdataset.SourceE AS SourceE ON
    (STARTS_WITH('Customer-5BFi','Fee-Iaxf') OR
     'Code-Oqui' NOT LIKE SourceB.FieldH AND
     SourceA.FieldA <> 'Fee-KKre' AND SourceB.FieldG NOT LIKE 'Customer-XY2j')
  LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
    ('Fee-3Hz8' LIKE SourceA.FieldF OR SourceA.FieldB = 'Account-PfYI' AND
     ENDS_WITH('Fee-OGeY',SourceB.FieldF) OR 'Customer-04vC' <> SourceC.FieldH)
  LEFT JOIN Table6 AS Table6 ON
    (ENDS_WITH('Code-miew','Code-D3xj') OR
     SourceE.FieldF <> DerivedTable1.AmtzEYq AND
     'Customer-sZpH' <> 'Amt-nGVv' OR 'Fee-BKm0' <> 'Fee-EHHo')
  INNER JOIN Table7 AS Table7 ON
    (STARTS_WITH('Country-sY1P','Country-ZGqk') OR
     ENDS_WITH('Amt-t8Qz',Table7.AddressWs4z) AND
     Table7.CountrybN0v LIKE 'Code-ezBj' AND
     STARTS_WITH(Table6.CustomerYwXe,'Country-8ptN'))
  RIGHT JOIN Table8 AS Table8 ON
    (ENDS_WITH(DerivedTable1.CCYXSkS,SourceB.FieldB) AND
     SourceA.FieldJ <> 'Fee-Vg76' AND
     Table8.FeevX4G NOT LIKE SourceC.FieldJ AND
     'Customer-G7ku' != Table7.AccountNi89)
  LEFT JOIN Table9 AS Table9 ON
    (STARTS_WITH('Country-8cIf',SourceE.FieldG) OR
     DerivedTable1.CustomerKrwR NOT LIKE 'Code-TNV1' OR
     SourceE.FieldD != 'CCY-bjml' AND ENDS_WITH('Amt-ewFh',Table9.AmtHEpD))
WHERE
  SourceC.FieldG != 'Fee-H6xv' OR 'CCY-6bbP' NOT LIKE 'Customer-Of2u' AND
  'CCY-zfz5' = 'Code-USH9' OR 'Account-SenS' NOT LIKE 'CCY-Targ' AND
  STARTS_WITH(DerivedTable1.AccountmNQo,SourceA.FieldC) OR
  DerivedTable1.Codeuhr9 <> 'Country-PbHM' AND
  STARTS_WITH(Table6.CountryMbVx,SourceB.FieldH) AND
  STARTS_WITH(SourceA.FieldJ,SourceC.FieldA)