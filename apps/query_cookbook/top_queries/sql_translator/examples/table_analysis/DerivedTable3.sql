CREATE OR REPLACE TABLE testdataset.DerivedTable3 AS
WITH
  Table11 AS (
    SELECT
      IF(
        ENDS_WITH(DerivedTable1.AddressoeLk,SourceC.FieldJ),
        CASE
          WHEN SourceE.FieldF NOT LIKE SourceC.FieldF THEN
            SourceC.FieldF
          WHEN STARTS_WITH('Account-nuAf','Code-drYW') THEN
            'Fee-TnbZ'
          WHEN 'CCY-n6QE' = SourceE.FieldH THEN
            SourceA.FieldA
          WHEN STARTS_WITH(SourceD.FieldE,'Amt-jura') THEN
            'Customer-s6UU'
          ELSE
            DerivedTable1.AccountqcVc
        END,
        CASE
          WHEN STARTS_WITH('Address-CQeN',DerivedTable1.Codeuhr9) THEN
            'Amt-Cimk'
          WHEN SourceA.FieldA <> SourceE.FieldB THEN
            DerivedTable1.CustomerWpfX
          WHEN SourceC.FieldC LIKE SourceE.FieldG THEN
            DerivedTable1.AddressoeLk
          WHEN SourceA.FieldH != SourceE.FieldJ THEN
            SourceE.FieldB
          WHEN SourceC.FieldE <> 'Customer-7Tsu' THEN
            'Code-NDVl'
          ELSE
            'Code-XcoH'
        END)
        AS Fee4bcw,
      IF(
        ENDS_WITH('Account-A4dg',SourceE.FieldD),
        IF(
          ENDS_WITH(SourceC.FieldB,'Country-zvYh'), 'Account-BINS',
          SourceA.FieldI),
        IF(STARTS_WITH(SourceC.FieldI,'Fee-T77H'),SourceE.FieldD,'CCY-aLrT'))
        AS CCYLIK1,
      CASE
        WHEN 'Fee-0FLA' LIKE 'Country-di9X' THEN
          IF('CCY-OT8v' != 'Address-ZYFa','Address-f0eW','Amt-HvKd')
        WHEN ENDS_WITH(SourceD.FieldA,SourceE.FieldC) THEN
          IF(ENDS_WITH('Fee-i2Kc','Amt-PMy8'),SourceE.FieldE,SourceC.FieldH)
        WHEN 'Country-5hdY' NOT LIKE SourceD.FieldD THEN
          IF(
            ENDS_WITH('Address-XRHA','Country-cuaI'), 'Country-IrRS',
            DerivedTable1.FeerBIS)
        WHEN STARTS_WITH('Customer-U2od','Fee-o586') THEN
          IF('Fee-1co0' != SourceD.FieldA,'Address-XHYl',SourceD.FieldI)
        WHEN ENDS_WITH('Fee-RbXy','Country-JMk1') THEN
          IF(
            STARTS_WITH('Address-hroH','CCY-T9gb'), DerivedTable1.AccountqcVc,
            SourceC.FieldD)
        ELSE
          IF(
            DerivedTable1.FeerBIS NOT LIKE SourceE.FieldB, 'Address-Mhdd',
            DerivedTable1.CountryV1io)
      END
        AS CountryeDVQ,
      IF(
        STARTS_WITH('Account-2XSi',DerivedTable1.AmtR86V),
        IF('Country-iAA9' <> SourceE.FieldB,'Customer-t4kg','Customer-Ap42'),
        IF(
          'Code-ftbq' NOT LIKE DerivedTable1.AmtR86V, 'Code-pTXi',
          SourceD.FieldF))
        AS CountryNYV9,
      IF(
        'CCY-GQb5' LIKE 'CCY-dONr',
        IF(ENDS_WITH('Account-pdeG','Fee-gub5'),'Code-a6GP',SourceE.FieldD),
        IF(
          STARTS_WITH('Account-PGH0',SourceC.FieldG), SourceD.FieldF,
          DerivedTable1.AddressoeLk))
        AS Amt3Xpw,
      IF(
        SourceC.FieldC != SourceD.FieldJ,
        IF(
          STARTS_WITH(SourceD.FieldE,SourceC.FieldJ), SourceD.FieldC,
          'CCY-E3jK'), IF(
                         'Account-dmjW' LIKE DerivedTable1.AmtzEYq,
                         'Customer-5nNk', 'Customer-oKrb'))
        AS Amtn5ax,
      IF(
        'Address-nuMs' <> SourceD.FieldD,
        IF(
          'Amt-J5Sv' != 'Customer-jxoy', DerivedTable1.AddressoeLk,
          SourceA.FieldI),
        IF(ENDS_WITH('CCY-BawZ','Code-FnBm'),'Account-9Qc8','Customer-XzSv'))
        AS FeeH55t,
      IF(
        STARTS_WITH('Customer-rcY7',SourceE.FieldC),
        IF(
          'Country-eJq4' <> DerivedTable1.AccountqcVc, SourceE.FieldH,
          SourceE.FieldJ),
        CASE
          WHEN 'Amt-BZ5s' LIKE DerivedTable1.FeerBIS THEN
            'Country-2ugT'
          WHEN 'Address-TVct' LIKE 'Fee-gt97' THEN
            DerivedTable1.Addressrgjd
          ELSE
            SourceA.FieldH
        END)
        AS CodeCOXA,
      IF(
        ENDS_WITH(DerivedTable1.Addressrgjd,SourceC.FieldH),
        IF(STARTS_WITH(SourceA.FieldF,'Country-71mK'),'Amt-8P2o','Amt-07M7'),
        IF(
          ENDS_WITH(DerivedTable1.Addressrgjd,SourceC.FieldC), 'Address-luU8',
          DerivedTable1.AccountKwSP))
        AS Country8Jw0,
      IF(
        STARTS_WITH('Amt-FZs0','CCY-sQI3'),
        IF(
          STARTS_WITH(SourceA.FieldE,SourceD.FieldA), SourceA.FieldI,
          'Customer-qE59'),
        CASE
          WHEN 'Address-GZp9' NOT LIKE 'Customer-2MCy' THEN
            'Fee-WpG1'
          WHEN 'Customer-FoN0' = 'Customer-QI7f' THEN
            SourceA.FieldJ
          WHEN DerivedTable1.AccountfEJK LIKE 'Amt-JNIJ' THEN
            'Country-E4x1'
          WHEN ENDS_WITH('Code-t2AT',DerivedTable1.Codeuhr9) THEN
            'Code-t2NX'
          WHEN 'Country-XifV' <> 'Country-H0uz' THEN
            DerivedTable1.Codeuhr9
          ELSE
            DerivedTable1.CountryV1io
        END)
        AS CodeFi9c,
      CASE
        WHEN DerivedTable1.AddressoeLk != 'Country-Mtip' THEN
          IF(
            DerivedTable1.AccountmNQo NOT LIKE 'CCY-qgjX', 'Customer-m3jl',
            SourceC.FieldF)
        WHEN STARTS_WITH(SourceE.FieldF,SourceD.FieldF) THEN
          CASE
            WHEN ENDS_WITH('Country-LUBV',SourceD.FieldJ) THEN
              'Code-71Ic'
            WHEN STARTS_WITH('Customer-TxUb','CCY-CnJt') THEN
              'Code-53Ue'
            ELSE
              SourceA.FieldE
          END
        WHEN ENDS_WITH(SourceA.FieldD,'Code-jNPx') THEN
          IF(SourceC.FieldE LIKE 'Fee-Z6wn','CCY-17UI','Code-vbhP')
        ELSE
          IF(
            SourceD.FieldB LIKE DerivedTable1.AddressoeLk, SourceC.FieldC,
            SourceE.FieldD)
      END
        AS AddressyI1n,
      IF(
        'Code-rkrM' != 'Account-HaZK',
        IF(SourceC.FieldC LIKE 'Amt-Wq3p','Amt-YQiH','Fee-IRch'),
        IF(
          STARTS_WITH('Address-9gZn',SourceE.FieldG), SourceD.FieldE,
          'Address-xq0J'))
        AS CodeQx0v,
      IF(
        'Code-dEB0' != SourceC.FieldF,
        IF(
          DerivedTable1.Addressrgjd NOT LIKE DerivedTable1.CustomerWpfX,
          SourceA.FieldG, DerivedTable1.CustomerKrwR),
        IF(
          SourceE.FieldH = SourceE.FieldE, DerivedTable1.AccountKwSP,
          SourceE.FieldE))
        AS FeeBKf5,
      IF(
        STARTS_WITH(SourceC.FieldE,SourceC.FieldG),
        IF(DerivedTable1.CCYXSkS LIKE 'Amt-J42E',SourceD.FieldE,'Amt-0g4i'),
        IF(SourceA.FieldE = SourceC.FieldG,SourceD.FieldH,SourceA.FieldC))
        AS CCYlUeY,
      CASE
        WHEN ENDS_WITH(SourceD.FieldE,'Code-gwOU') THEN
          CASE
            WHEN STARTS_WITH(SourceD.FieldE,SourceC.FieldH) THEN
              SourceD.FieldA
            WHEN SourceA.FieldA <> 'Code-NHfM' THEN
              'Country-spMU'
            WHEN SourceE.FieldE != SourceA.FieldI THEN
              DerivedTable1.AmtR86V
            WHEN 'Customer-VOMs' LIKE 'Account-9rqI' THEN
              'Address-I7M1'
            WHEN SourceD.FieldI LIKE 'Country-eged' THEN
              SourceE.FieldC
            ELSE
              SourceD.FieldG
          END
        WHEN ENDS_WITH(SourceE.FieldD,'Account-4e5I') THEN
          IF(
            ENDS_WITH('CCY-ASeu',DerivedTable1.AccountqcVc), 'CCY-WfdN',
            'Fee-DXd1')
        ELSE
          IF(
            ENDS_WITH('Country-477T',SourceA.FieldG), 'Account-uSD8',
            SourceD.FieldF)
      END
        AS CodeNXAR,
      IF(
        SourceE.FieldH <> 'CCY-WLCU',
        IF(
          DerivedTable1.AccountfEJK NOT LIKE 'Address-iK0v', 'Account-Py0t',
          'Amt-Fsoq'),
        IF(STARTS_WITH('Fee-olLE','Fee-WTEF'),'Country-RAkg',SourceA.FieldI))
        AS AddressE3Ue,
      IF(
        'Amt-Hr9O' LIKE 'Address-Wu50',
        IF(
          STARTS_WITH(SourceD.FieldJ,'Account-fUc4'), 'Country-d3BU',
          SourceD.FieldC),
        IF('Code-Nrbj' = 'Amt-wLKX',DerivedTable1.CustomerWrlz,'Account-EVI7'))
        AS CountryHAgU,
      IF(
        STARTS_WITH(SourceE.FieldE,SourceE.FieldG),
        IF(
          'Country-NRKl' NOT LIKE 'Customer-Uaaw', 'Address-Xkz9',
          DerivedTable1.AccountmNQo),
        IF(
          ENDS_WITH(SourceC.FieldB,SourceC.FieldE), 'Code-mT6J',
          'Customer-VqJI'))
        AS AmtRYSm,
      IF(
        ENDS_WITH('Fee-8iGX','Fee-cS56'),
        IF('CCY-J9Zn' LIKE 'Country-bjQb',DerivedTable1.AmtzEYq,'Account-kpRM'),
        CASE
          WHEN 'Amt-0DkS' <> SourceD.FieldF THEN
            DerivedTable1.FeerBIS
          WHEN ENDS_WITH(DerivedTable1.AddressoeLk,SourceE.FieldB) THEN
            'Account-aXYr'
          WHEN DerivedTable1.AmtR86V <> DerivedTable1.AccountmNQo THEN
            'CCY-Xg0V'
          WHEN SourceE.FieldJ = 'Country-JGi6' THEN
            SourceC.FieldA
          WHEN 'Code-jW6T' = 'Customer-dj1Y' THEN
            'Customer-vlDI'
          ELSE
            'Fee-pE0j'
        END)
        AS CodeiTnY
    FROM
      testdataset.SourceA AS SourceA
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Customer-YQyS',SourceC.FieldI) OR
         'CCY-rt09' != 'Code-fAVd' OR
         SourceC.FieldG NOT LIKE 'Amt-YAlf' OR
         SourceC.FieldC NOT LIKE SourceC.FieldG)
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (SourceC.FieldI <> SourceC.FieldE OR
         SourceA.FieldC <> 'Customer-Wov2' OR
         STARTS_WITH(SourceD.FieldF,'Address-Tm5I') OR
         'Address-szTr' = 'Account-pbJF')
      INNER JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('CCY-dqCZ','Account-fTf5') AND
         ENDS_WITH('Account-DNAF',SourceE.FieldF) OR
         STARTS_WITH(SourceA.FieldD,SourceC.FieldH) AND
         'Code-LLFF' = 'Customer-ZmVL')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        ('Fee-1Iyw' LIKE 'Address-AWA9' AND 'Amt-qmUm' <> SourceA.FieldF OR
         SourceC.FieldD != 'Fee-zJUi' AND ENDS_WITH('Fee-6LLA',SourceD.FieldB))
    WHERE
      ENDS_WITH('Customer-VwXM',SourceC.FieldD) OR
      DerivedTable1.AccountKwSP LIKE 'Code-bzTU' OR
      DerivedTable1.AddressoeLk NOT LIKE 'Amt-Cpf8' OR
      SourceE.FieldA NOT LIKE SourceD.FieldC AND
      SourceE.FieldB != 'Code-J7VC' OR
      STARTS_WITH('Account-TfZ3','Account-M16v') OR
      ENDS_WITH(SourceA.FieldH,SourceE.FieldG) OR
      'Address-KAx7' LIKE 'Customer-Vmkl'
  ),
  Table12 AS (
    SELECT
      IF(
        ENDS_WITH('Fee-yLsV','Amt-2rgq'),
        IF(
          'Code-9IGB' = SourceC.FieldJ, 'Country-jMEc',
          DerivedTable1.CountryV1io),
        IF(
          STARTS_WITH(SourceC.FieldD,SourceD.FieldB), 'Country-ixED',
          'Fee-jm5U'))
        AS Account8xb8,
      IF(
        'Amt-juyE' LIKE 'Country-DqyG',
        CASE
          WHEN STARTS_WITH('Customer-vNYx',SourceB.FieldB) THEN
            'CCY-KesC'
          WHEN STARTS_WITH(DerivedTable1.CustomerWpfX,SourceD.FieldI) THEN
            SourceB.FieldE
          WHEN ENDS_WITH('Account-WraP','Address-alrw') THEN
            SourceA.FieldD
          WHEN ENDS_WITH(SourceC.FieldG,'Code-LtAo') THEN
            'Address-QwuA'
          ELSE
            'Amt-oLTW'
        END, IF(
               STARTS_WITH('Amt-7e1t','Customer-77dq'), 'Country-GHOw',
               'Customer-Ao5R'))
        AS CodeecQr,
      IF(
        SourceB.FieldJ LIKE 'Fee-jEXD',
        IF(
          ENDS_WITH('Account-gqaG',Table11.CodeNXAR), Table11.Fee4bcw,
          SourceB.FieldH),
        CASE
          WHEN STARTS_WITH('Amt-M409',SourceA.FieldA) THEN
            'Customer-vKsq'
          WHEN 'Address-GrJl' LIKE SourceD.FieldJ THEN
            'Country-VidI'
          WHEN 'Account-va8a' LIKE 'CCY-gSag' THEN
            'Country-13KZ'
          WHEN 'CCY-Fq5c' != SourceB.FieldG THEN
            'Fee-7q8E'
          WHEN STARTS_WITH(Table11.FeeBKf5,'Address-IXYg') THEN
            'Country-jQNu'
          ELSE
            Table11.CountryeDVQ
        END)
        AS AddressrUj4,
      IF(
        ENDS_WITH(Table11.Amtn5ax,'CCY-N81V'),
        IF(SourceA.FieldA != SourceB.FieldA,'Code-sn2n',SourceA.FieldJ),
        IF(
          STARTS_WITH(SourceD.FieldG,'Account-WUiS'), SourceC.FieldA,
          'Code-sqZb'))
        AS Address5Z39,
      IF(
        'Account-gron' NOT LIKE 'Fee-1knA',
        IF(
          STARTS_WITH(SourceD.FieldE,'Amt-i5z3'), Table11.AmtRYSm,
          SourceA.FieldG),
        IF('Code-RLLb' != SourceC.FieldD,DerivedTable1.AccountmNQo,'Code-uxyO'))
        AS AddressKHBI,
      IF(
        STARTS_WITH('CCY-bgQE','Amt-kVO4'),
        IF(
          STARTS_WITH(SourceB.FieldA,'Country-GRLm'), SourceA.FieldF,
          'Amt-Q0pV'),
        CASE
          WHEN DerivedTable1.CountryV1io NOT LIKE 'Code-eRkG' THEN
            SourceB.FieldD
          WHEN 'Fee-3DL5' LIKE 'Fee-V3LY' THEN
            'Amt-EiM8'
          WHEN STARTS_WITH(Table11.CodeNXAR,SourceB.FieldB) THEN
            'Amt-RVsZ'
          WHEN SourceA.FieldB <> 'Fee-pDtt' THEN
            'Fee-0ovg'
          WHEN ENDS_WITH(SourceA.FieldI,'Customer-2TQ0') THEN
            DerivedTable1.CustomerWrlz
          ELSE
            SourceB.FieldG
        END)
        AS FeeFF4u,
      CASE
        WHEN 'Customer-0ymP' = 'Customer-n6ff' THEN
          CASE
            WHEN ENDS_WITH(SourceC.FieldA,'Amt-SY8g') THEN
              'Code-uG09'
            WHEN ENDS_WITH(SourceA.FieldB,SourceB.FieldC) THEN
              'Customer-94ka'
            ELSE
              DerivedTable1.Codeuhr9
          END
        WHEN ENDS_WITH('Fee-QaAW',SourceC.FieldJ) THEN
          CASE
            WHEN STARTS_WITH(SourceC.FieldC,'Amt-ffp1') THEN
              'Amt-Nziz'
            WHEN ENDS_WITH(Table11.CodeNXAR,Table11.Amt3Xpw) THEN
              'Customer-AQ8k'
            WHEN STARTS_WITH(Table11.Amt3Xpw,Table11.FeeBKf5) THEN
              DerivedTable1.AmtR86V
            WHEN Table11.Amt3Xpw LIKE 'CCY-PLY1' THEN
              SourceC.FieldH
            ELSE
              SourceA.FieldD
          END
        WHEN 'Country-nPyG' <> 'Amt-MUYW' THEN
          IF(
            ENDS_WITH(SourceA.FieldG,'Account-u2E2'), Table11.CountryNYV9,
            'Country-ODUK')
        WHEN 'CCY-95WQ' NOT LIKE 'Customer-4xpJ' THEN
          IF(SourceA.FieldE != 'Code-RpUF','Amt-qX1s',SourceA.FieldD)
        WHEN 'CCY-fg6Z' = SourceD.FieldH THEN
          CASE
            WHEN STARTS_WITH(DerivedTable1.AmtR86V,'CCY-7aGk') THEN
              SourceA.FieldF
            WHEN 'Amt-0bVT' NOT LIKE SourceB.FieldC THEN
              Table11.FeeH55t
            WHEN DerivedTable1.AddressoeLk LIKE DerivedTable1.CCYXSkS THEN
              Table11.CCYlUeY
            ELSE
              Table11.CodeCOXA
          END
        ELSE
          IF(
            ENDS_WITH('Address-Z3Xm','CCY-Pavc'), Table11.CountryNYV9,
            DerivedTable1.FeerBIS)
      END
        AS FeePG8N,
      IF(
        'Amt-vYdG' NOT LIKE SourceD.FieldF,
        IF(
          ENDS_WITH('Address-rL6e','Address-YUEK'), Table11.CodeiTnY,
          'Country-0Or4'),
        CASE
          WHEN ENDS_WITH(DerivedTable1.AmtR86V,'Customer-kaxK') THEN
            'Customer-w4Qg'
          WHEN 'Customer-oGHy' = 'Amt-GnOA' THEN
            'Amt-qAoA'
          ELSE
            'CCY-8WHc'
        END)
        AS CustomerEC7o,
      IF(
        ENDS_WITH('Code-amOu',SourceC.FieldI),
        IF(
          ENDS_WITH(SourceD.FieldC,'Fee-RLUl'), DerivedTable1.CCYXSkS,
          DerivedTable1.AccountqcVc),
        IF(ENDS_WITH('Code-XjcW','Code-Ga2z'),'Account-KNuB','Code-T8mN'))
        AS CustomerbvFt,
      IF(
        Table11.CCYLIK1 != 'Address-G4SF',
        IF(
          ENDS_WITH(SourceC.FieldI,SourceA.FieldE), 'Account-YBXi',
          SourceD.FieldE),
        IF(SourceC.FieldI = 'Code-UUkj','Address-Dej1','CCY-2JOx'))
        AS FeeWUq7,
      IF(
        ENDS_WITH('Customer-isOX','CCY-T9re'),
        IF(
          DerivedTable1.CCYXSkS <> DerivedTable1.CustomerKrwR, SourceC.FieldD,
          Table11.CodeNXAR),
        IF(
          SourceC.FieldG NOT LIKE DerivedTable1.CustomerWrlz, 'Fee-Za8i',
          DerivedTable1.AccountfEJK))
        AS CCYxplp,
      IF(
        'Amt-CDL6' <> 'CCY-IC6v',
        IF(
          STARTS_WITH(SourceD.FieldA,'Fee-qySX'), DerivedTable1.CustomerKrwR,
          Table11.Fee4bcw),
        CASE
          WHEN STARTS_WITH(Table11.FeeH55t,SourceB.FieldI) THEN
            'Account-lH1h'
          WHEN STARTS_WITH(SourceC.FieldC,'Amt-VtvP') THEN
            SourceB.FieldH
          ELSE
            SourceA.FieldH
        END)
        AS AddressvU4a,
      IF(
        STARTS_WITH('Address-KZ7Y',DerivedTable1.AccountKwSP),
        IF('Account-AAo6' NOT LIKE 'Code-AZNK',SourceC.FieldE,'Code-R2Yv'),
        IF('Amt-vUc4' <> 'Address-7Ll9',SourceC.FieldA,Table11.CountryNYV9))
        AS CustomerOwiE,
      IF(
        ENDS_WITH(SourceB.FieldG,'CCY-6nxy'),
        IF(
          STARTS_WITH(DerivedTable1.AccountfEJK,SourceD.FieldA), 'Amt-Z8Vv',
          Table11.Fee4bcw),
        IF(Table11.AddressyI1n NOT LIKE 'Country-TyRf','CCY-reXT','Fee-Rv3v'))
        AS CountrypRxt,
      IF(
        ENDS_WITH('Country-ibGA','Fee-SjEP'),
        IF('Fee-JKGE' = 'Country-25RA','CCY-M1ll',SourceC.FieldD),
        IF(
          ENDS_WITH(SourceC.FieldH,Table11.AmtRYSm), 'Code-ZXj4',
          SourceA.FieldH))
        AS CCYRxrw
    FROM
      testdataset.SourceD AS SourceD
      INNER JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(SourceA.FieldG,'Fee-FxbP') OR
         ENDS_WITH(SourceA.FieldE,SourceD.FieldE) OR
         SourceA.FieldJ LIKE 'Customer-0QZ8' OR
         ENDS_WITH(SourceD.FieldB,'Code-XGgN'))
      INNER JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Code-r5XR',SourceB.FieldD) OR
         ENDS_WITH(SourceB.FieldA,'Address-rreI') OR
         STARTS_WITH('Fee-eQB6',SourceD.FieldH) AND
         ENDS_WITH('Customer-yNPq',SourceB.FieldA))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Country-RUDM','Customer-GhjK') AND
         ENDS_WITH(SourceC.FieldI,SourceD.FieldI) OR
         STARTS_WITH(SourceA.FieldC,SourceD.FieldJ) OR
         SourceB.FieldC = 'CCY-0sTw')
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('CCY-0nWY',SourceC.FieldI) AND
         SourceD.FieldG <> SourceB.FieldA OR
         'Amt-Emgy' != 'Fee-x6fQ' OR
         ENDS_WITH('Account-3QKZ',DerivedTable1.AddressoeLk))
      LEFT JOIN Table11 AS Table11 ON
        (Table11.CCYlUeY <> SourceD.FieldB AND
         Table11.CodeFi9c NOT LIKE SourceB.FieldJ AND
         'Country-VXMv' = 'Code-ak6W' AND Table11.Fee4bcw = 'Country-n1jb')
    WHERE
      SourceA.FieldB NOT LIKE Table11.CodeFi9c AND
      'CCY-gUjf' LIKE 'Code-vMAZ' OR
      STARTS_WITH('Country-4ErR','Address-101J') OR
      SourceC.FieldA LIKE SourceB.FieldJ OR
      STARTS_WITH(SourceA.FieldJ,'Address-oaDz') AND
      STARTS_WITH(DerivedTable1.FeerBIS,Table11.FeeH55t) OR
      SourceB.FieldC != 'Amt-wrjI' AND
      STARTS_WITH(DerivedTable1.AmtR86V,'Customer-WRUd')
  ),
  Table13 AS (
    SELECT
      IF(
        'Address-cjXq' LIKE 'CCY-Su0F',
        CASE
          WHEN DerivedTable2.AddressHdnu <> 'Code-fgvL' THEN
            'Country-pI07'
          WHEN STARTS_WITH(Table12.AddressrUj4,DerivedTable2.Amt885g) THEN
            SourceC.FieldJ
          WHEN ENDS_WITH('Customer-cjeS','Fee-ikl5') THEN
            SourceC.FieldA
          ELSE
            DerivedTable1.AccountmNQo
        END, CASE
               WHEN SourceD.FieldG NOT LIKE 'Fee-E3kb' THEN
                 DerivedTable1.AccountKwSP
               WHEN 'CCY-kZCQ' <> DerivedTable1.CustomerKrwR THEN
                 'Country-yU8A'
               WHEN ENDS_WITH(Table12.Account8xb8,'Fee-4mMf') THEN
                 DerivedTable1.CountryV1io
               ELSE
                 'Amt-QS9e'
             END)
        AS AddressSIwC,
      IF(
        'Code-7TnH' != SourceD.FieldC,
        IF(
          SourceA.FieldF = 'Account-F2tL', 'Country-UxT6',
          DerivedTable1.Addressrgjd),
        IF(ENDS_WITH(SourceD.FieldG,SourceB.FieldE),'Fee-Z7CZ','CCY-565r'))
        AS CustomereSku,
      IF(
        'Country-uUoS' NOT LIKE Table12.AddressKHBI,
        IF('Fee-KJ54' LIKE 'Customer-bYwU','Country-6MUH',SourceA.FieldA),
        CASE
          WHEN 'Customer-ImbY' <> 'Customer-VRQx' THEN
            SourceC.FieldB
          WHEN SourceB.FieldG != SourceB.FieldC THEN
            'Amt-O3M4'
          ELSE
            Table12.FeeFF4u
        END)
        AS CountrylUbo,
      IF(
        DerivedTable1.AmtzEYq LIKE 'Customer-blN2',
        CASE
          WHEN STARTS_WITH('Customer-3OS1',SourceE.FieldC) THEN
            SourceD.FieldF
          WHEN 'CCY-F9Q9' LIKE 'Account-78Ck' THEN
            'CCY-3t1y'
          WHEN STARTS_WITH(DerivedTable2.CountrywaWi,'Code-AvOH') THEN
            'Country-DUF5'
          ELSE
            DerivedTable1.Codeuhr9
        END,
        IF(ENDS_WITH(SourceD.FieldB,'Code-HbLa'),SourceB.FieldD,'CCY-6OYZ'))
        AS CCYzfFz,
      IF(
        SourceB.FieldJ LIKE DerivedTable1.AmtzEYq,
        IF('Fee-hxhp' != 'Amt-RUV0','Country-3Tst',DerivedTable2.AddresscM17),
        IF(
          ENDS_WITH('Address-i2hz','Address-irce'), SourceC.FieldI,
          Table12.Account8xb8))
        AS AddressV8YO,
      IF(
        ENDS_WITH(Table12.FeeWUq7,SourceA.FieldD),
        CASE
          WHEN 'Code-ruae' = 'Customer-POqg' THEN
            'Account-RRAT'
          WHEN SourceD.FieldF <> SourceB.FieldJ THEN
            DerivedTable1.AccountqcVc
          WHEN SourceA.FieldH = 'CCY-sGJ5' THEN
            SourceB.FieldD
          ELSE
            'Fee-dFBq'
        END, CASE
               WHEN DerivedTable1.AddressoeLk != 'Code-hT11' THEN
                 DerivedTable1.AddressoeLk
               WHEN STARTS_WITH(SourceE.FieldG,'CCY-K7YV') THEN
                 'Fee-bdxC'
               WHEN DerivedTable2.AccountrJZf <> SourceC.FieldH THEN
                 'Fee-eioU'
               ELSE
                 'Fee-hRLZ'
             END)
        AS Feeahin,
      IF(
        STARTS_WITH(SourceC.FieldC,SourceA.FieldF),
        IF(
          ENDS_WITH('Country-Wcip',DerivedTable1.AddressoeLk),
          DerivedTable2.CodeDmva, SourceA.FieldJ),
        IF(
          ENDS_WITH(DerivedTable2.AccounttVM1,SourceE.FieldF), 'CCY-iy3D',
          SourceE.FieldB))
        AS Amtqzad,
      IF(
        'Address-pXlE' = Table12.FeeWUq7,
        IF(
          'Customer-MnnR' <> DerivedTable2.AccounttVM1, SourceC.FieldC,
          SourceB.FieldC),
        IF(STARTS_WITH('Code-wuUQ','Fee-khku'),'Address-4s57',SourceA.FieldG))
        AS AmtR7bS,
      CASE
        WHEN ENDS_WITH(SourceB.FieldH,'Fee-9B12') THEN
          IF(
            SourceD.FieldB = 'Country-3cqS', DerivedTable2.Account61kv,
            SourceD.FieldI)
        WHEN ENDS_WITH('Account-N4Qy',SourceC.FieldJ) THEN
          IF(
            STARTS_WITH('Account-ibSK','Customer-vgdp'), 'Address-EEeB',
            DerivedTable2.AmtNncS)
        WHEN ENDS_WITH('Code-6CAR',Table12.CodeecQr) THEN
          IF(
            ENDS_WITH('CCY-TYRQ',Table12.AddressvU4a), 'Fee-XXt7',
            DerivedTable1.CustomerKrwR)
        ELSE
          IF(
            'Address-Jtta' NOT LIKE 'Code-SKPq', 'Address-Dgx8',
            DerivedTable1.FeerBIS)
      END
        AS CountryJ3J7,
      IF(
        DerivedTable1.CountryV1io NOT LIKE 'Code-CvC1',
        CASE
          WHEN DerivedTable2.AmtmorU <> Table12.CustomerbvFt THEN
            'Address-kSZc'
          WHEN 'Address-3Hxq' = DerivedTable1.CCYXSkS THEN
            'Amt-nvNY'
          WHEN DerivedTable2.AmtNncS != DerivedTable2.Amt885g THEN
            SourceB.FieldE
          WHEN STARTS_WITH('Customer-TuqW','Customer-pPH1') THEN
            SourceB.FieldG
          WHEN ENDS_WITH(SourceC.FieldF,DerivedTable1.AccountfEJK) THEN
            Table12.CCYRxrw
          ELSE
            Table12.CustomerbvFt
        END, CASE
               WHEN STARTS_WITH('CCY-2F3p','Fee-RPc6') THEN
                 'Account-JOg0'
               WHEN 'Customer-Gfp4' = DerivedTable1.AmtzEYq THEN
                 'Country-PNbv'
               WHEN STARTS_WITH('CCY-DZIz',SourceC.FieldH) THEN
                 DerivedTable1.AddressoeLk
               ELSE
                 'Country-JtUu'
             END)
        AS AddressZGT9,
      IF(
        STARTS_WITH('Code-R4aL',DerivedTable2.AccountrJZf),
        IF(
          SourceC.FieldE != DerivedTable1.AccountmNQo, 'Address-iWFC',
          Table12.AddressKHBI),
        IF('Address-HnbD' <> SourceA.FieldA,SourceA.FieldJ,SourceD.FieldJ))
        AS Feej8VV
    FROM
      testdataset.DerivedTable2 AS DerivedTable2
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Address-Hbsh' <> 'Code-53GA' AND
         STARTS_WITH(SourceA.FieldI,SourceA.FieldC) AND
         'Account-ELB4' LIKE DerivedTable2.CodeDmva AND
         'Code-XtrS' <> SourceA.FieldC)
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('Address-U7KF','Code-oBZK') AND
         SourceB.FieldG NOT LIKE DerivedTable2.AccountctCy AND
         DerivedTable2.AccounttVM1 LIKE 'CCY-ipln' OR
         SourceB.FieldH NOT LIKE 'Code-jHMD')
      LEFT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH('Fee-m636',SourceB.FieldI) AND
         SourceA.FieldJ != 'Country-B1s5' OR
         ENDS_WITH(SourceC.FieldI,DerivedTable2.Amt885g) OR
         SourceA.FieldH NOT LIKE DerivedTable2.AccountctCy)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH('Fee-SSDN','Address-g96B') AND
         DerivedTable2.AmtmorU <> DerivedTable2.AddresscM17 OR
         SourceD.FieldC NOT LIKE SourceB.FieldG OR
         'Account-oyYo' <> DerivedTable2.Amtp4ee)
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        ('Customer-5pTn' != 'Country-rntY' OR
         STARTS_WITH(SourceE.FieldC,'Code-Z5lA') AND
         ENDS_WITH('Amt-IjY9','Address-LVET') AND
         'Account-IDH0' != SourceD.FieldF)
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (SourceB.FieldD NOT LIKE 'Code-sjyC' OR
         STARTS_WITH('Account-3cnI',SourceB.FieldJ) AND
         ENDS_WITH('CCY-EccN',DerivedTable2.AmtmorU) AND
         ENDS_WITH('Country-dbx9','Amt-Sz6B'))
      RIGHT JOIN Table12 AS Table12 ON
        (DerivedTable2.AddressHdnu <> 'Customer-Kc63' AND
         ENDS_WITH(SourceA.FieldA,SourceB.FieldF) AND
         STARTS_WITH('Amt-wTO9',DerivedTable1.AccountmNQo) AND
         'Customer-8cS5' = SourceA.FieldI)
    WHERE
      STARTS_WITH(Table12.CodeecQr,DerivedTable1.CustomerKrwR) AND
      'Account-JB9Y' <> DerivedTable1.AmtR86V AND
      'Code-l2VZ' <> DerivedTable2.Amtp4ee AND
      DerivedTable2.Address5RWP <> Table12.Address5Z39 OR
      ENDS_WITH('Account-aUSj','Address-dgsc') AND
      STARTS_WITH(DerivedTable1.Codeuhr9,Table12.CountrypRxt) OR
      'Address-LNTJ' NOT LIKE 'Customer-DBsN' AND
      ENDS_WITH('Customer-s4nT',SourceD.FieldB)
  ),
  Table14 AS (
    SELECT
      IF(
        STARTS_WITH('Customer-zZs8','CCY-ffaA'),
        IF('Code-m1wZ' = SourceA.FieldA,'Address-AegT','CCY-zhYC'),
        IF(
          STARTS_WITH(SourceB.FieldA,DerivedTable1.AccountmNQo), SourceA.FieldE,
          SourceA.FieldA))
        AS AccountYqWM,
      IF(
        STARTS_WITH(SourceA.FieldD,SourceC.FieldA),
        IF(Table11.Amt3Xpw <> 'Code-6bZp',Table11.AmtRYSm,SourceD.FieldF),
        IF(
          STARTS_WITH('Code-vFgu','CCY-gVMn'), SourceA.FieldA,
          DerivedTable1.Codeuhr9))
        AS CountrysgZD,
      IF(
        ENDS_WITH('Amt-eUzx','Code-LYWE'),
        IF(SourceC.FieldB NOT LIKE 'Amt-9lDa','Country-KN8K','Country-7eqY'),
        IF(SourceE.FieldB != SourceE.FieldE,'Address-WkXZ',Table11.CountryNYV9))
        AS Country9tG9,
      IF(
        'Customer-0YsW' NOT LIKE 'Customer-3H9r',
        IF(
          STARTS_WITH('Account-oYmS','Customer-mcOf'), 'Code-H2Xm',
          Table13.AddressV8YO),
        IF('Code-1Twn' != Table11.AddressE3Ue,'CCY-Gwo4','Customer-uA2D'))
        AS FeeVw4C,
      IF(
        STARTS_WITH('Account-fHBV','Account-BkNL'),
        IF(SourceC.FieldG <> SourceA.FieldA,'Address-q43o','Customer-ySUA'),
        IF(ENDS_WITH('Amt-raYP','Country-sj4U'),Table11.CCYLIK1,'Code-u4nY'))
        AS AmtXeix,
      CASE
        WHEN 'CCY-KCQL' LIKE 'Address-aftG' THEN
          IF(
            ENDS_WITH(SourceE.FieldA,SourceD.FieldE), 'Code-Mh8W',
            'Country-YCpt')
        WHEN STARTS_WITH('Country-RWMX','Code-Xhk8') THEN
          CASE
            WHEN
              STARTS_WITH(DerivedTable1.Addressrgjd,DerivedTable1.CustomerWrlz)
            THEN
              'Amt-sTs5'
            WHEN
              STARTS_WITH(SourceD.FieldG,SourceA.FieldG)
            THEN
              SourceE.FieldD
            WHEN
              ENDS_WITH(SourceA.FieldC,'Code-0kMg')
            THEN
              'Amt-DSn3'
            ELSE
              'Code-wL2G'
          END
        ELSE
          IF(
            STARTS_WITH('Amt-VARz','Customer-owF4'), SourceC.FieldH,
            'Account-VSlJ')
      END
        AS AddressUm6F,
      IF(
        STARTS_WITH(Table13.Feeahin,Table11.CCYlUeY),
        CASE
          WHEN
            STARTS_WITH(DerivedTable1.AccountKwSP,DerivedTable1.AddressoeLk)
          THEN
            'Amt-GWZy'
          WHEN
            STARTS_WITH('Customer-BZHs','Fee-TPUs')
          THEN
            'Fee-5Rj4'
          WHEN
            STARTS_WITH(Table11.Amt3Xpw,'Address-a2kN')
          THEN
            Table11.Country8Jw0
          WHEN
            STARTS_WITH('Code-JUOS',Table13.Amtqzad)
          THEN
            'CCY-wmQ6'
          ELSE
            DerivedTable1.CustomerWrlz
        END, CASE
               WHEN 'Code-RJf1' NOT LIKE 'Account-DkXZ' THEN
                 SourceC.FieldB
               WHEN SourceD.FieldG <> 'Fee-FDbq' THEN
                 'CCY-QWP9'
               ELSE
                 'CCY-kV7g'
             END)
        AS FeeNtSk,
      IF(
        ENDS_WITH(Table11.Amt3Xpw,'Amt-wN4z'),
        IF(
          'Address-D45a' LIKE SourceB.FieldI, 'Customer-xFyP',
          DerivedTable1.CountryV1io),
        IF('Address-OCTP' LIKE 'Country-nErr','Fee-1GoT',Table13.Feeahin))
        AS FeejceS,
      IF(
        'Account-7ZLB' != Table13.AmtR7bS,
        IF(ENDS_WITH(SourceA.FieldJ,SourceE.FieldI),Table13.AmtR7bS,'Amt-YzWD'),
        CASE
          WHEN ENDS_WITH(SourceD.FieldA,'Fee-UD7X') THEN
            SourceD.FieldF
          WHEN STARTS_WITH('Fee-mfLy',SourceC.FieldA) THEN
            Table13.AddressV8YO
          WHEN STARTS_WITH(SourceD.FieldE,Table11.CodeQx0v) THEN
            SourceD.FieldJ
          WHEN 'Code-drgg' != SourceA.FieldA THEN
            SourceD.FieldH
          ELSE
            'Address-4tWE'
        END)
        AS FeerGWo,
      IF(
        'Customer-QBYj' <> SourceA.FieldG,
        CASE
          WHEN SourceE.FieldE != Table11.FeeH55t THEN
            'Customer-9XMK'
          WHEN STARTS_WITH(DerivedTable1.Codeuhr9,'Amt-SJQm') THEN
            Table11.FeeH55t
          ELSE
            Table11.AddressyI1n
        END, IF('CCY-fnW0' = 'CCY-A9cM','Account-tj33','Country-ax0A'))
        AS CCYTVOg,
      CASE
        WHEN SourceD.FieldF NOT LIKE SourceB.FieldB THEN
          CASE
            WHEN ENDS_WITH('Fee-1w3Z','Fee-f451') THEN
              'Fee-7exM'
            WHEN STARTS_WITH(SourceE.FieldD,SourceA.FieldB) THEN
              'Customer-a4Dz'
            WHEN DerivedTable1.AmtR86V = SourceC.FieldJ THEN
              SourceC.FieldH
            WHEN STARTS_WITH('Code-VSVN','Address-7deJ') THEN
              'Amt-MzVU'
            ELSE
              'CCY-W5FB'
          END
        WHEN STARTS_WITH('Country-0e5n','Code-g8pJ') THEN
          IF(Table11.CodeFi9c <> 'Code-j96r','Fee-llsr',SourceE.FieldJ)
        WHEN 'CCY-zKVy' NOT LIKE SourceE.FieldC THEN
          IF(SourceA.FieldF != SourceB.FieldJ,SourceE.FieldA,SourceB.FieldD)
        WHEN 'Country-lKYj' NOT LIKE 'Code-DvdU' THEN
          IF(
            STARTS_WITH('CCY-EVzN',Table11.Amt3Xpw), SourceE.FieldD,
            SourceA.FieldI)
        WHEN ENDS_WITH('Fee-tL4U','Account-v7T0') THEN
          IF('Fee-aXii' NOT LIKE 'Account-k2x0','Amt-lcUM','Amt-2LrA')
        ELSE
          IF('Fee-VNQL' != Table11.FeeH55t,'Fee-vvxc',Table11.AddressyI1n)
      END
        AS Code9aVn
    FROM
      testdataset.SourceC AS SourceC
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (SourceA.FieldD = SourceA.FieldF AND
         ENDS_WITH('Account-DRQU','Address-4Pct') AND
         STARTS_WITH(SourceC.FieldB,SourceA.FieldG) AND
         SourceC.FieldB != SourceC.FieldD)
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceB.FieldH,'Fee-SYg6') AND
         'CCY-gWp3' NOT LIKE SourceA.FieldH AND
         STARTS_WITH(SourceB.FieldI,SourceB.FieldJ) OR
         STARTS_WITH('Code-oNmS','Customer-m9kU'))
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Country-cX9p','Customer-pbdo') OR
         ENDS_WITH(SourceA.FieldF,'Account-WjVT') OR
         SourceA.FieldF = 'CCY-cCr9' OR
         STARTS_WITH('Address-bg1e',SourceA.FieldA))
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (SourceD.FieldI NOT LIKE 'Customer-XQx0' AND
         'CCY-MG5S' = SourceA.FieldG OR
         ENDS_WITH(SourceD.FieldB,'Customer-15Mk') AND
         'Code-sEWo' != 'Fee-nUnO')
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Fee-W2so','Code-ZgVL') AND
         SourceA.FieldD != SourceC.FieldH AND
         ENDS_WITH(SourceE.FieldD,DerivedTable1.AccountfEJK) AND
         DerivedTable1.AccountmNQo LIKE 'Fee-LRj3')
      LEFT JOIN Table11 AS Table11 ON
        (STARTS_WITH(SourceA.FieldH,'Fee-7a8m') AND
         STARTS_WITH(DerivedTable1.AccountqcVc,SourceA.FieldD) AND
         SourceE.FieldA NOT LIKE SourceB.FieldC OR
         ENDS_WITH('CCY-VixF',SourceC.FieldB))
      LEFT JOIN Table13 AS Table13 ON
        ('CCY-KMeG' = 'Country-hw3Q' AND
         ENDS_WITH(DerivedTable1.AmtzEYq,DerivedTable1.AddressoeLk) AND
         Table11.Amt3Xpw NOT LIKE SourceB.FieldB OR
         STARTS_WITH('Country-Mn4t',SourceB.FieldI))
    WHERE
      STARTS_WITH('Fee-9wDJ',DerivedTable1.CustomerWrlz) AND
      STARTS_WITH(SourceE.FieldF,SourceD.FieldH) AND
      STARTS_WITH(SourceB.FieldD,'Country-YfUv') AND
      Table13.AddressSIwC = 'Country-eXz4' AND
      STARTS_WITH(DerivedTable1.Addressrgjd,SourceB.FieldD) AND
      STARTS_WITH(SourceA.FieldF,'CCY-ACjK') OR
      'Address-eyE9' LIKE Table11.CodeFi9c AND
      ENDS_WITH(Table11.Fee4bcw,'Fee-U6Qb')
  ),
  Table15 AS (
    SELECT
      IF(
        STARTS_WITH(Table12.CustomerbvFt,SourceE.FieldF),
        CASE
          WHEN DerivedTable2.CountrywaWi = SourceB.FieldB THEN
            'Fee-ag85'
          WHEN 'Country-2WSX' LIKE 'Amt-7b9w' THEN
            'Fee-Jdls'
          WHEN 'Fee-aIRZ' != 'Account-KuAX' THEN
            'Account-2kwm'
          ELSE
            'Code-lHL1'
        END,
        IF(Table11.Fee4bcw != 'Address-DwUz','Country-fdPq',Table11.CCYLIK1))
        AS AddressNnl1,
      IF(
        STARTS_WITH(DerivedTable2.CodeDmva,SourceD.FieldB),
        IF(
          ENDS_WITH(SourceE.FieldA,Table13.CountryJ3J7), 'Address-9I4H',
          Table12.FeeFF4u),
        IF(Table11.CCYLIK1 NOT LIKE 'Address-FxbJ','Customer-Qr0p','CCY-1S7e'))
        AS CCY1P7V,
      IF(
        'CCY-GRUY' = Table13.CountrylUbo,
        CASE
          WHEN SourceC.FieldC != 'Fee-ZyAi' THEN
            'Address-0Eg2'
          WHEN STARTS_WITH('CCY-Tgiy','Customer-avrh') THEN
            DerivedTable1.AmtR86V
          ELSE
            DerivedTable1.AccountmNQo
        END, IF(
               ENDS_WITH(DerivedTable1.AmtR86V,Table12.FeeFF4u),
               Table11.CodeCOXA, Table12.CustomerEC7o))
        AS CodejQvr,
      IF(
        STARTS_WITH('Country-LUuF',SourceB.FieldB),
        IF(
          STARTS_WITH(DerivedTable2.AddresscM17,Table12.AddressKHBI),
          Table14.AddressUm6F, SourceC.FieldI),
        CASE
          WHEN STARTS_WITH('Account-Zeyo',SourceE.FieldH) THEN
            DerivedTable1.CustomerKrwR
          WHEN Table14.Country9tG9 = 'CCY-25Df' THEN
            'CCY-HWvl'
          WHEN Table13.CCYzfFz != SourceB.FieldI THEN
            Table13.CountrylUbo
          WHEN SourceE.FieldA != 'CCY-Q04W' THEN
            SourceE.FieldD
          WHEN 'Address-wFGv' <> 'CCY-l476' THEN
            Table14.CCYTVOg
          ELSE
            Table11.Amtn5ax
        END)
        AS CCYbDjE,
      CASE
        WHEN 'Code-lTJ1' = 'Country-v6M2' THEN
          IF(
            ENDS_WITH(DerivedTable1.AccountKwSP,'Country-cWbd'),
            Table11.AddressE3Ue, Table14.CountrysgZD)
        WHEN Table11.CCYlUeY NOT LIKE 'Address-c30N' THEN
          IF(
            STARTS_WITH('Fee-BvAt',SourceD.FieldD), Table11.Fee4bcw,
            SourceE.FieldE)
        WHEN 'Customer-BESW' LIKE SourceB.FieldD THEN
          IF(
            STARTS_WITH(SourceD.FieldG,SourceC.FieldH), SourceB.FieldF,
            'Customer-TJmb')
        WHEN Table12.CodeecQr = 'Country-p9LU' THEN
          CASE
            WHEN ENDS_WITH(SourceC.FieldC,'CCY-4gU2') THEN
              DerivedTable2.CodeRziT
            WHEN STARTS_WITH('Fee-fyHK',DerivedTable1.Addressrgjd) THEN
              SourceE.FieldC
            ELSE
              'Customer-L6iC'
          END
        ELSE
          IF(
            ENDS_WITH('Code-POis','Customer-hXiH'), 'Address-KVZX',
            'Account-dt9j')
      END
        AS FeeOZGC,
      IF(
        'Address-2Fr3' <> 'Customer-SHhQ',
        IF(
          'Country-DVUL' NOT LIKE 'Country-idRb', DerivedTable1.AccountKwSP,
          SourceB.FieldD), IF(
                             SourceD.FieldH NOT LIKE Table12.AddressvU4a,
                             'Country-le4Z', 'Fee-ALMi'))
        AS Codez3ys,
      CASE
        WHEN STARTS_WITH('Fee-hsgp','Account-IZRR') THEN
          IF(
            'Fee-ZcCv' <> Table11.Amt3Xpw, DerivedTable2.Amt885g,
            DerivedTable2.Amt885g)
        WHEN DerivedTable2.AddresscM17 = SourceB.FieldH THEN
          CASE
            WHEN ENDS_WITH(Table13.CountryJ3J7,'Fee-gGgH') THEN
              DerivedTable2.Amt885g
            WHEN ENDS_WITH('Customer-EwVE','Code-DBoA') THEN
              'Customer-pTGL'
            ELSE
              SourceB.FieldB
          END
        WHEN STARTS_WITH('Country-RsAt','CCY-r1eB') THEN
          CASE
            WHEN DerivedTable1.CustomerWpfX <> 'Address-yTtM' THEN
              DerivedTable2.AccounttVM1
            WHEN Table11.CCYLIK1 = Table13.CCYzfFz THEN
              'Customer-GyGA'
            WHEN STARTS_WITH('CCY-3lIX','Country-odQf') THEN
              SourceB.FieldE
            WHEN Table14.CCYTVOg != Table11.Amtn5ax THEN
              Table11.CodeCOXA
            WHEN STARTS_WITH(SourceE.FieldH,'Country-k3rh') THEN
              'Country-d3G0'
            ELSE
              'Address-4KJM'
          END
        ELSE
          IF(
            STARTS_WITH('Country-7qpi',Table13.AddressZGT9), 'Address-uEv6',
            'Fee-Nnl3')
      END
        AS CustomerLHe3,
      IF(
        SourceC.FieldB NOT LIKE Table13.Feej8VV,
        IF('CCY-qt0y' LIKE 'Address-otF5','Country-gAhZ',Table13.Amtqzad),
        IF(ENDS_WITH(SourceC.FieldJ,'Amt-ox1A'),'CCY-Oiln',SourceB.FieldI))
        AS CCYziif,
      CASE
        WHEN STARTS_WITH(Table12.Account8xb8,'Country-ZgFX') THEN
          IF(
            Table13.AddressZGT9 <> DerivedTable2.AccountctCy, 'Fee-Zwdi',
            'Fee-gIFl')
        WHEN 'CCY-yNbE' NOT LIKE 'Amt-HG8y' THEN
          IF(
            ENDS_WITH(Table11.AddressE3Ue,'Code-5T9c'),
            DerivedTable2.AccountrJZf, 'Address-7hQI')
        WHEN STARTS_WITH(Table11.CountryNYV9,DerivedTable2.CodeRziT) THEN
          IF('Code-fzNp' != SourceD.FieldE,'Code-TbFR',Table13.AddressZGT9)
        ELSE
          CASE
            WHEN ENDS_WITH(SourceB.FieldA,'Address-29u0') THEN
              'Amt-Lsd1'
            WHEN STARTS_WITH(Table14.AccountYqWM,'Fee-CmJ9') THEN
              Table11.CodeFi9c
            WHEN STARTS_WITH('Account-Mdp5',Table11.AddressyI1n) THEN
              'Fee-E2wG'
            WHEN ENDS_WITH('Country-rotp','Account-SJfX') THEN
              'Amt-xALb'
            WHEN ENDS_WITH(Table12.CustomerEC7o,'Country-ZUkB') THEN
              SourceC.FieldC
            ELSE
              'Address-513G'
          END
      END
        AS AmtyHkc,
      IF(
        ENDS_WITH(DerivedTable2.Address5RWP,'Customer-mLwe'),
        IF(
          STARTS_WITH(Table11.CCYLIK1,'Account-KGzL'), 'Customer-HfuR',
          'Fee-K5Ze'),
        CASE
          WHEN ENDS_WITH('Customer-4UX3','Customer-TdYU') THEN
            DerivedTable1.CustomerWrlz
          WHEN ENDS_WITH('CCY-gphv',Table14.AddressUm6F) THEN
            DerivedTable2.CodeDmva
          WHEN ENDS_WITH(Table12.CCYRxrw,'Country-2JVE') THEN
            'Address-fpae'
          ELSE
            Table11.CodeQx0v
        END)
        AS CCYg7uk,
      IF(
        'Amt-ElEM' = SourceC.FieldC,
        IF(
          Table12.CodeecQr = 'Country-cQBF', Table12.AddressvU4a,
          SourceE.FieldJ), IF(
                             DerivedTable1.AccountmNQo != SourceD.FieldF,
                             SourceC.FieldC, DerivedTable1.AccountfEJK))
        AS CodeoLJg,
      IF(
        STARTS_WITH(SourceE.FieldF,Table12.CountrypRxt),
        IF(Table14.AccountYqWM != 'Country-94OI',SourceD.FieldJ,'CCY-L9Iy'),
        IF(
          ENDS_WITH('Customer-iiHr',Table11.AmtRYSm), SourceE.FieldG,
          'Country-JiVa'))
        AS CodenAcN,
      CASE
        WHEN ENDS_WITH('Address-wgWb','Code-Zjrv') THEN
          CASE
            WHEN SourceB.FieldB <> 'Customer-2OER' THEN
              'Fee-iF0M'
            WHEN 'Country-DfsR' <> Table13.AddressSIwC THEN
              SourceB.FieldD
            ELSE
              'CCY-u9UW'
          END
        WHEN 'Code-jaS7' NOT LIKE 'Amt-DZ5x' THEN
          IF(
            ENDS_WITH(Table11.FeeBKf5,'Account-VTtb'), DerivedTable1.Codeuhr9,
            'Amt-Mtc2')
        ELSE
          IF(
            'Account-yoSk' != SourceD.FieldC, SourceD.FieldI,
            DerivedTable1.FeerBIS)
      END
        AS CCYzOx9,
      IF(
        STARTS_WITH(DerivedTable1.CountryV1io,SourceB.FieldJ),
        CASE
          WHEN STARTS_WITH(Table12.CustomerOwiE,'CCY-dHgi') THEN
            DerivedTable2.AmtNncS
          WHEN Table13.CustomereSku NOT LIKE 'Fee-UJba' THEN
            'Account-nMA3'
          ELSE
            DerivedTable1.FeerBIS
        END, IF('Amt-q3CP' != Table14.FeeVw4C,Table14.FeerGWo,Table13.Feeahin))
        AS AddressKFxX,
      IF(
        ENDS_WITH(SourceB.FieldI,Table12.Address5Z39),
        IF(
          STARTS_WITH(DerivedTable2.AmtNncS,'Amt-puTc'), 'Address-dPF4',
          'Code-3zii'),
        IF(SourceB.FieldG NOT LIKE 'Customer-Zz7w','Country-Upyq','Amt-6YC3'))
        AS Addressrklq,
      IF(
        SourceC.FieldA != DerivedTable1.AccountfEJK,
        IF(
          Table11.AmtRYSm LIKE 'Fee-qv5D', DerivedTable1.CustomerWrlz,
          Table11.CCYLIK1), IF(
                              DerivedTable2.AddressHdnu <> 'Amt-BAkH',
                              'Country-dDeF', DerivedTable2.AccountrJZf))
        AS FeeBUSI
    FROM
      Table14 AS Table14
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (Table14.FeejceS = 'Account-OptN' OR
         STARTS_WITH(SourceB.FieldG,'Address-3x5o') AND
         ENDS_WITH('Address-OiuX','Address-Gvj9') OR
         'Country-MZhy' != 'Code-tU8a')
      INNER JOIN testdataset.SourceC AS SourceC ON
        (Table14.Code9aVn NOT LIKE 'Address-SEeQ' OR
         ENDS_WITH('Country-SdFY',SourceC.FieldC) OR
         'CCY-Uiby' <> SourceB.FieldI AND 'CCY-8CSr' LIKE 'Country-fNeP')
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(Table14.FeeNtSk,SourceB.FieldF) AND
         STARTS_WITH(Table14.AccountYqWM,'CCY-30b2') OR
         'Address-yYnq' != 'Customer-ViVK' AND SourceB.FieldB = 'Code-bd4c')
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('CCY-1Bgm','Customer-ZxtY') OR
         STARTS_WITH('Code-PtRc','Country-Mgrz') AND
         ENDS_WITH(SourceB.FieldF,'Amt-EBJI') OR
         ENDS_WITH(SourceC.FieldE,SourceC.FieldC))
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (Table14.AccountYqWM = SourceC.FieldF OR
         STARTS_WITH('Amt-PuHM',Table14.AddressUm6F) AND
         SourceD.FieldG = SourceB.FieldH AND 'Address-9oMk' NOT LIKE 'Amt-Nudt')
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (DerivedTable2.Amtp4ee NOT LIKE 'Country-fBor' AND
         STARTS_WITH('CCY-QauJ',SourceB.FieldH) OR
         'Code-BNUl' NOT LIKE 'CCY-OuUU' AND 'Fee-7qi8' = SourceB.FieldE)
      RIGHT JOIN Table11 AS Table11 ON
        (ENDS_WITH(Table11.CodeCOXA,SourceE.FieldA) OR
         STARTS_WITH('Address-V8CF','CCY-d1vR') AND
         ENDS_WITH('Code-iPh2',DerivedTable1.AccountmNQo) OR
         SourceB.FieldJ NOT LIKE 'Code-2bFC')
      RIGHT JOIN Table12 AS Table12 ON
        ('Fee-du3q' NOT LIKE DerivedTable2.CodeDmva AND
         ENDS_WITH(DerivedTable2.Amtp4ee,DerivedTable1.CustomerWpfX) OR
         ENDS_WITH(SourceD.FieldE,DerivedTable1.Addressrgjd) AND
         STARTS_WITH(SourceC.FieldJ,SourceC.FieldB))
      INNER JOIN Table13 AS Table13 ON
        (Table13.CustomereSku LIKE 'Country-SeCq' AND
         ENDS_WITH(Table13.CountrylUbo,SourceD.FieldG) OR
         'Code-flta' NOT LIKE 'Fee-AqKg' AND Table13.Amtqzad != 'Amt-Zp3W')
    WHERE
      Table11.Amt3Xpw NOT LIKE SourceE.FieldE AND
      ENDS_WITH(Table11.CodeCOXA,Table13.Feeahin) OR
      'Customer-aSNP' != 'Amt-94C1' AND Table12.CountrypRxt <> SourceD.FieldI OR
      ENDS_WITH(DerivedTable2.CountrywaWi,'Amt-zAdU') OR
      'Account-JJCl' LIKE DerivedTable2.Amtp4ee OR
      STARTS_WITH('Account-h97n','Customer-OOFi') AND
      ENDS_WITH('Address-qCEn',Table12.AddressKHBI)
  )
SELECT
  IF(
    ENDS_WITH('Customer-4VwA','Account-d8Up'),
    IF('Country-1PNT' <> 'Fee-nWFt','Account-pEBx',SourceB.FieldJ),
    CASE
      WHEN 'Address-0H53' LIKE DerivedTable2.AddresscM17 THEN
        Table12.AddressvU4a
      WHEN DerivedTable2.AddresscM17 != 'Fee-BdE7' THEN
        'Amt-CNzM'
      ELSE
        'Fee-jrca'
    END)
    AS AccountViga,
  IF(
    Table12.Address5Z39 <> 'Account-uIjW',
    IF('Amt-Rf7O' LIKE Table11.Amtn5ax,'Account-hatx','Code-25zt'),
    IF(ENDS_WITH(SourceE.FieldF,'Customer-PNt3'),'CCY-8zO5','Account-3xR9'))
    AS CodeUeLq,
  IF(
    'Country-kqfw' != 'Account-98iH',
    CASE
      WHEN ENDS_WITH(Table15.AddressKFxX,Table11.CodeNXAR) THEN
        Table11.CCYlUeY
      WHEN 'Fee-xgVp' LIKE Table13.AmtR7bS THEN
        SourceB.FieldH
      WHEN ENDS_WITH(DerivedTable2.AmtNncS,'CCY-osKc') THEN
        Table15.Codez3ys
      ELSE
        'Address-9Ce6'
    END, IF(
           STARTS_WITH('Amt-yErm',Table11.CodeCOXA), Table15.CCYg7uk,
           Table15.FeeBUSI))
    AS Codeaqvh,
  IF(
    'Amt-Abig' <> 'Code-OkMs',
    IF(
      STARTS_WITH(Table13.CustomereSku,'Fee-ukEF'), Table11.Amtn5ax,
      'Address-4xZ0'),
    IF(ENDS_WITH('Address-u4VN','Amt-VZzn'),'Customer-c14d','Account-OaUb'))
    AS FeeD0Po,
  CASE
    WHEN 'Fee-EpKE' = Table11.Amtn5ax THEN
      IF('Customer-hFpL' <> 'Account-bwDz',Table15.CustomerLHe3,SourceC.FieldD)
    WHEN ENDS_WITH(SourceD.FieldC,'Customer-cp0z') THEN
      IF('Code-hKfb' != DerivedTable2.Amt885g,Table15.CodenAcN,'Country-BkPO')
    WHEN SourceD.FieldD NOT LIKE 'Address-f908' THEN
      IF(
        SourceB.FieldA LIKE Table12.CustomerbvFt, Table14.FeeNtSk,
        'Customer-VZJk')
    WHEN STARTS_WITH(Table14.AmtXeix,'Fee-bpDK') THEN
      IF(STARTS_WITH(Table13.Feeahin,'Fee-UPsJ'),'Fee-8LUO',Table13.AmtR7bS)
    ELSE
      CASE
        WHEN ENDS_WITH('Address-rXer','Code-nPX9') THEN
          Table11.AddressE3Ue
        WHEN SourceA.FieldH <> Table11.CCYLIK1 THEN
          'Customer-Yl9h'
        WHEN ENDS_WITH('Fee-2ytc',Table11.Fee4bcw) THEN
          'Account-ciT0'
        ELSE
          'Customer-TOHL'
      END
  END
    AS Account4sxv,
  IF(
    'Customer-tYKx' LIKE 'Address-eJ2a',
    CASE
      WHEN ENDS_WITH('Customer-cqqS',Table11.FeeBKf5) THEN
        SourceE.FieldE
      WHEN STARTS_WITH(Table14.AccountYqWM,'Customer-I19Z') THEN
        'Country-l1PH'
      WHEN ENDS_WITH(Table13.CustomereSku,'Customer-CGGh') THEN
        Table12.CustomerEC7o
      WHEN SourceE.FieldC NOT LIKE 'Address-fE6y' THEN
        'Country-kL8I'
      ELSE
        'Account-EtK4'
    END,
    IF('Customer-0Wn9' NOT LIKE 'Customer-5GU3','Country-7ZQw',SourceC.FieldA))
    AS FeeF6P4,
  IF(
    'Amt-eYkn' != SourceE.FieldI,
    IF('Code-bpyb' = 'CCY-bXzK',SourceB.FieldH,SourceD.FieldC),
    IF(
      Table14.AmtXeix LIKE Table15.AddressKFxX, 'Customer-ThgV',
      Table15.Codez3ys))
    AS CodeVHWz,
  IF(
    'Address-xHfd' != 'Code-CvE2',
    IF(
      DerivedTable2.Address7oYI <> DerivedTable2.AmtNncS, 'Country-FnoW',
      Table11.CodeiTnY),
    IF(SourceB.FieldG = 'Code-E8JT',Table15.Codez3ys,'Account-ZN0l'))
    AS CustomerLP0M,
  CASE
    WHEN ENDS_WITH(Table14.FeeVw4C,'Fee-oCn7') THEN
      IF(
        STARTS_WITH('Country-hqIP',Table12.Address5Z39), DerivedTable2.AmtmorU,
        Table11.CCYLIK1)
    WHEN ENDS_WITH('Address-P67N','Address-70TD') THEN
      IF(
        'Fee-qT6k' NOT LIKE 'Amt-BoTG', SourceC.FieldF,
        DerivedTable2.Address5RWP)
    ELSE
      IF(SourceE.FieldB NOT LIKE Table15.FeeOZGC,'CCY-L1uv','Address-wxPj')
  END
    AS FeepQGh,
  IF(
    SourceE.FieldB <> 'Account-cqtQ',
    IF(
      STARTS_WITH('Customer-Om76','Country-68Zx'), Table15.CodenAcN,
      'Account-gFRF'), IF(
                         STARTS_WITH(SourceE.FieldH,Table12.AddressrUj4),
                         SourceB.FieldJ, 'Customer-A38f'))
    AS FeeT7G9,
  IF(
    'Fee-424F' != 'CCY-R3L2',
    IF(Table11.CodeFi9c LIKE 'CCY-FM4B','Address-oFRX','Customer-abbd'),
    IF(STARTS_WITH(SourceC.FieldA,'CCY-hOtk'),'CCY-MVgO',Table15.CustomerLHe3))
    AS Code3Xip,
  IF(
    'Customer-fU6d' LIKE SourceE.FieldH,
    IF('Address-qgLb' != Table13.AddressSIwC,'Amt-VIlb',SourceA.FieldC),
    IF('Code-iQzc' <> DerivedTable2.Amt885g,'Address-KrXk',SourceE.FieldJ))
    AS CCYO742,
  IF(
    STARTS_WITH('Customer-c3fK',Table15.CCYbDjE),
    IF(
      DerivedTable2.CodeDmva LIKE Table15.FeeOZGC, Table13.Amtqzad,
      SourceC.FieldG),
    CASE
      WHEN ENDS_WITH('Country-kY9A',Table11.CCYLIK1) THEN
        'Account-oltq'
      WHEN 'CCY-wDAO' <> 'Country-HHXk' THEN
        Table11.Fee4bcw
      WHEN SourceC.FieldA <> Table12.CodeecQr THEN
        'Code-I5DI'
      ELSE
        Table11.CodeiTnY
    END)
    AS FeeqkjX,
  IF(
    ENDS_WITH('Fee-x1SX',Table12.CodeecQr),
    IF(
      ENDS_WITH(SourceD.FieldI,Table12.AddressKHBI), SourceD.FieldE,
      'Code-1fU8'),
    IF(Table15.CCYziif LIKE 'Code-AOvP',SourceB.FieldD,'Address-oINh'))
    AS CountryoFFA,
  IF(
    STARTS_WITH(Table14.FeeNtSk,'CCY-hpo4'),
    IF(Table15.CodejQvr NOT LIKE Table11.AmtRYSm,SourceA.FieldF,'Country-pmXh'),
    IF(
      STARTS_WITH(Table12.AddressrUj4,Table11.CCYlUeY), 'Address-DYwg',
      Table13.AmtR7bS))
    AS CustomerULyd,
  IF(
    'CCY-uFVj' != SourceB.FieldA,
    IF(STARTS_WITH('Code-leyp','Address-k0RL'),'Fee-pW9i','Address-AqXZ'),
    CASE
      WHEN ENDS_WITH(Table13.AddressSIwC,Table11.AddressyI1n) THEN
        SourceB.FieldH
      WHEN 'Fee-voHH' <> 'Amt-Kt5K' THEN
        'Country-C3gs'
      WHEN SourceE.FieldI NOT LIKE Table13.CustomereSku THEN
        'Customer-cg1D'
      WHEN 'Country-rjLA' = Table15.CCYzOx9 THEN
        Table13.Feeahin
      ELSE
        SourceE.FieldD
    END)
    AS Feeg8kl,
  IF(
    ENDS_WITH(Table14.CountrysgZD,Table11.Amtn5ax),
    CASE
      WHEN STARTS_WITH(DerivedTable2.AmtNncS,'Fee-iCYA') THEN
        'Amt-yMhU'
      WHEN Table15.CustomerLHe3 NOT LIKE SourceA.FieldJ THEN
        Table11.FeeBKf5
      WHEN ENDS_WITH('Address-vjUq',Table13.CustomereSku) THEN
        Table13.Amtqzad
      WHEN SourceB.FieldG != 'Account-uPU5' THEN
        Table14.Country9tG9
      WHEN ENDS_WITH('Amt-Rnlb',Table11.CountryeDVQ) THEN
        'Customer-aQ4D'
      ELSE
        'Country-kvFs'
    END, IF(
           STARTS_WITH(SourceB.FieldH,DerivedTable2.Amtp4ee), Table15.CodenAcN,
           'Amt-6Kta'))
    AS CountrygMB4,
  IF(
    Table15.CodejQvr LIKE 'Code-MmRr',
    IF('Amt-Jvb8' = 'Code-WmrU','Fee-ICTI','CCY-khFR'),
    IF(
      DerivedTable2.Amtp4ee NOT LIKE 'Fee-set1', 'Address-VYaT',
      'Customer-RQeA'))
    AS CountrygPvi,
  IF(
    SourceE.FieldA NOT LIKE SourceA.FieldI,
    IF('Code-3GKL' NOT LIKE 'Fee-L2DG','Country-gA2a','Account-2kdn'),
    IF(STARTS_WITH('Country-qAlQ',Table12.FeeFF4u),'Account-gCUw','CCY-6YsB'))
    AS FeeCUde,
  IF(
    'Account-LcMt' != Table11.CountryeDVQ,
    CASE
      WHEN STARTS_WITH('CCY-DJa8','Country-y3d3') THEN
        Table11.FeeH55t
      WHEN ENDS_WITH('Customer-s1Nv','Address-Hl9Z') THEN
        'Account-Jp5M'
      WHEN Table15.CCYbDjE != SourceA.FieldI THEN
        Table11.Amtn5ax
      WHEN 'Customer-5kMF' <> SourceE.FieldD THEN
        SourceB.FieldI
      ELSE
        'Country-6zUo'
    END, IF('Customer-We2Q' NOT LIKE 'Account-Av7b','Amt-XpTI','Address-zmj4'))
    AS CustomereBLI
FROM
  Table14 AS Table14
  RIGHT JOIN testdataset.SourceA AS SourceA ON
    (Table14.FeeNtSk NOT LIKE SourceA.FieldC OR
     STARTS_WITH(Table14.FeeVw4C,Table14.FeerGWo) AND
     ENDS_WITH('Address-2yV1','Country-Punj') OR 'Customer-QlKm' = 'Amt-IGfX')
  INNER JOIN testdataset.SourceB AS SourceB ON
    ('Country-IzfW' LIKE SourceB.FieldE AND
     STARTS_WITH(SourceB.FieldH,Table14.CCYTVOg) AND
     'Fee-1suN' = Table14.AmtXeix OR ENDS_WITH(SourceA.FieldF,'Customer-Ymhq'))
  LEFT JOIN testdataset.SourceC AS SourceC ON
    (SourceC.FieldJ LIKE Table14.CCYTVOg AND
     STARTS_WITH('Code-THca','Customer-9uRW') AND
     'CCY-MNey' NOT LIKE SourceC.FieldG OR
     STARTS_WITH('Address-rFCy','Customer-rkyI'))
  INNER JOIN testdataset.SourceD AS SourceD ON
    (ENDS_WITH(SourceA.FieldD,SourceB.FieldH) OR
     SourceB.FieldB != SourceA.FieldH AND
     STARTS_WITH(SourceB.FieldF,SourceB.FieldJ) OR SourceD.FieldE <> 'CCY-ncFY')
  LEFT JOIN testdataset.SourceE AS SourceE ON
    ('Customer-Exlv' NOT LIKE SourceC.FieldB AND
     STARTS_WITH('Country-KqSA',SourceE.FieldH) OR
     'Amt-HA56' != 'Account-4uZM' AND 'Fee-YC0i' = Table14.CCYTVOg)
  LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
    ('Amt-e1Ws' NOT LIKE 'CCY-oOBt' OR ENDS_WITH(SourceD.FieldI,'Amt-OfBO') AND
     SourceB.FieldF LIKE 'Account-jVRC' OR
     ENDS_WITH('Customer-EEvf',SourceE.FieldC))
  INNER JOIN Table11 AS Table11 ON
    ('Address-IB5B' = SourceE.FieldF AND 'Code-T95x' != 'Fee-GoF6' OR
     ENDS_WITH(SourceE.FieldJ,'CCY-YdIf') AND
     ENDS_WITH(SourceD.FieldC,SourceD.FieldJ))
  INNER JOIN Table12 AS Table12 ON
    (ENDS_WITH('Fee-owfs',DerivedTable2.AddressHdnu) OR
     ENDS_WITH(SourceB.FieldF,'Code-LHXf') AND
     ENDS_WITH(Table12.AddressvU4a,SourceC.FieldG) OR
     STARTS_WITH('Fee-2uEj',SourceD.FieldC))
  RIGHT JOIN Table13 AS Table13 ON
    (ENDS_WITH(SourceA.FieldE,'Customer-Dzxw') OR
     DerivedTable2.AmtNncS != SourceB.FieldC OR
     'Code-nTdC' NOT LIKE 'Account-kLqM' AND
     ENDS_WITH(Table13.AmtR7bS,SourceB.FieldB))
  RIGHT JOIN Table15 AS Table15 ON
    (ENDS_WITH(Table13.Feej8VV,'Country-UdZK') OR
     ENDS_WITH(SourceE.FieldB,'Address-hhbT') OR
     ENDS_WITH(Table15.CCYbDjE,SourceA.FieldG) AND
     SourceC.FieldE = 'Address-5y5X')
WHERE
  SourceA.FieldG != Table13.Amtqzad OR ENDS_WITH(SourceA.FieldA,'Code-9HV3') OR
  'Country-adQk' LIKE Table12.CCYRxrw AND
  DerivedTable2.AccounttVM1 != 'CCY-E08N' AND
  'Fee-bAw4' = SourceE.FieldF OR
  DerivedTable2.Address5RWP LIKE SourceC.FieldI OR
  ENDS_WITH('Country-PcZB',SourceE.FieldG) AND
  STARTS_WITH(Table12.FeeWUq7,Table15.CCYziif)