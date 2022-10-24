CREATE OR REPLACE TABLE testdataset.DerivedTable6 AS
WITH
  Table26 AS (
    SELECT
      CASE
        WHEN 'Address-jvEO' != 'CCY-qkcc' THEN
          IF(
            STARTS_WITH('Amt-Oobr','Amt-tfNf'), DerivedTable5.Code7INp,
            'CCY-OeM9')
        WHEN SourceD.FieldA LIKE 'Amt-EWT1' THEN
          IF('Fee-iRQZ' = 'Fee-JWWJ',DerivedTable2.Amtp4ee,'Address-ejM8')
        WHEN ENDS_WITH('Address-Pl8O',DerivedTable3.CustomereBLI) THEN
          IF('Country-Fya9' = SourceC.FieldI,SourceA.FieldG,'Amt-zBVW')
        WHEN 'CCY-Un9X' LIKE 'Fee-cjWF' THEN
          CASE
            WHEN STARTS_WITH(SourceC.FieldJ,'CCY-0s8U') THEN
              DerivedTable5.Code7INp
            WHEN ENDS_WITH(SourceD.FieldF,'Address-gPUv') THEN
              DerivedTable3.CountryoFFA
            WHEN SourceD.FieldB LIKE 'Address-xuHr' THEN
              SourceB.FieldF
            ELSE
              DerivedTable5.FeeogF5
          END
        ELSE
          IF(
            DerivedTable3.CustomereBLI NOT LIKE SourceB.FieldB, 'Address-dbmY',
            'Country-JGpK')
      END
        AS CustomerXdHw,
      CASE
        WHEN STARTS_WITH(DerivedTable1.AccountKwSP,'CCY-TR9j') THEN
          IF(
            STARTS_WITH('Fee-nSrl',DerivedTable3.CountryoFFA),
            DerivedTable5.Code7INp, DerivedTable3.Code3Xip)
        WHEN STARTS_WITH('Code-DcXg',SourceD.FieldI) THEN
          IF(
            STARTS_WITH(DerivedTable5.FeeogF5,SourceD.FieldD), 'Code-lfY1',
            'CCY-W9Je')
        ELSE
          IF(
            STARTS_WITH(DerivedTable5.AddressV4AX,'Amt-fju3'), 'Account-JL5E',
            DerivedTable2.AccounttVM1)
      END
        AS Customer7qUf,
      IF(
        DerivedTable2.CountrywaWi = 'Customer-VbNM',
        IF(
          ENDS_WITH(DerivedTable2.Amt885g,SourceA.FieldD), 'Country-PVEh',
          SourceD.FieldE), IF(
                             DerivedTable2.AccountctCy = 'Code-Iu8T',
                             SourceB.FieldC, SourceA.FieldF))
        AS AmthWaa,
      IF(
        ENDS_WITH('Fee-ckXE','Amt-lVm3'),
        IF(
          SourceB.FieldI = 'Code-ynI7', SourceD.FieldE,
          DerivedTable5.Customera0qh),
        IF('Account-sAHD' NOT LIKE SourceE.FieldG,'Code-cU1f','Country-gjdn'))
        AS FeeNiKJ,
      IF(
        'Address-6R9T' = DerivedTable3.FeeqkjX,
        CASE
          WHEN ENDS_WITH(SourceE.FieldE,DerivedTable3.FeepQGh) THEN
            SourceB.FieldG
          WHEN DerivedTable3.CountryoFFA != SourceB.FieldJ THEN
            DerivedTable3.FeepQGh
          WHEN SourceE.FieldF NOT LIKE 'Customer-Raf0' THEN
            DerivedTable3.FeeF6P4
          ELSE
            SourceA.FieldD
        END, CASE
               WHEN ENDS_WITH(DerivedTable1.CountryV1io,'Account-JBDr') THEN
                 DerivedTable3.CCYO742
               WHEN DerivedTable2.CodeDmva != 'Amt-EVCI' THEN
                 SourceE.FieldE
               ELSE
                 'Amt-OUTs'
             END)
        AS CountryI5Fk,
      CASE
        WHEN STARTS_WITH(DerivedTable3.FeeCUde,'Code-0bcg') THEN
          IF('CCY-gNtj' != DerivedTable3.FeeCUde,'Address-vSCb','Address-T5ep')
        WHEN STARTS_WITH(SourceA.FieldA,DerivedTable1.FeerBIS) THEN
          CASE
            WHEN 'Country-HRpT' != 'Amt-OcVL' THEN
              SourceD.FieldJ
            WHEN SourceE.FieldI LIKE DerivedTable3.FeepQGh THEN
              'Country-iahZ'
            WHEN ENDS_WITH(SourceD.FieldC,'Customer-4Tof') THEN
              DerivedTable1.CustomerWpfX
            WHEN DerivedTable1.CustomerKrwR != 'Address-zEXA' THEN
              'Code-vdEy'
            ELSE
              SourceA.FieldC
          END
        WHEN ENDS_WITH(SourceE.FieldE,'Customer-GDsP') THEN
          IF(ENDS_WITH(SourceC.FieldC,'Code-go0E'),'Fee-8GAX',SourceD.FieldD)
        ELSE
          IF(
            STARTS_WITH('Customer-tnKv','Customer-ser0'), 'Address-CnOO',
            DerivedTable2.AccountctCy)
      END
        AS AccountCVze,
      IF(
        STARTS_WITH('Address-rXTY',DerivedTable5.Code7INp),
        IF(
          STARTS_WITH('Country-BXr4','Amt-dpnQ'), DerivedTable5.Feepmor,
          DerivedTable1.AmtzEYq),
        IF(
          'Fee-EBWh' NOT LIKE 'Customer-tnmN', DerivedTable2.AmtmorU,
          SourceB.FieldE))
        AS CustomerRbsW,
      IF(
        DerivedTable1.CountryV1io != SourceB.FieldB,
        CASE
          WHEN ENDS_WITH(SourceE.FieldJ,DerivedTable2.CodeDmva) THEN
            'Fee-bpGy'
          WHEN 'Customer-2v3g' NOT LIKE 'Fee-BJsw' THEN
            SourceE.FieldG
          WHEN ENDS_WITH(SourceC.FieldF,SourceB.FieldA) THEN
            'CCY-t8qM'
          WHEN 'Account-nNA7' != DerivedTable1.AmtzEYq THEN
            DerivedTable1.FeerBIS
          WHEN ENDS_WITH(DerivedTable1.AddressoeLk,SourceC.FieldC) THEN
            SourceC.FieldD
          ELSE
            SourceC.FieldC
        END, IF(
               STARTS_WITH(DerivedTable3.FeeT7G9,DerivedTable1.CustomerWrlz),
               DerivedTable1.AddressoeLk, 'Account-9f79'))
        AS AddressCO7c,
      IF(
        ENDS_WITH(DerivedTable1.Addressrgjd,'CCY-ywvx'),
        IF(ENDS_WITH('Country-nDUC','Account-IPiB'),SourceE.FieldH,'CCY-IAL6'),
        CASE
          WHEN STARTS_WITH(SourceD.FieldI,'Country-TB1n') THEN
            SourceD.FieldE
          WHEN STARTS_WITH('Fee-TSjL','Fee-mcCr') THEN
            'Amt-5reM'
          WHEN ENDS_WITH('Address-jkDQ',SourceA.FieldD) THEN
            SourceA.FieldH
          WHEN 'Customer-omQf' = 'Fee-bcW4' THEN
            'Account-7mnR'
          WHEN STARTS_WITH(SourceB.FieldI,DerivedTable2.AmtmorU) THEN
            'Address-PWBn'
          ELSE
            SourceD.FieldI
        END)
        AS FeekVuZ,
      IF(
        STARTS_WITH('CCY-77sB',SourceC.FieldD),
        IF(SourceC.FieldA != 'Customer-bip3','Amt-p6hI',SourceC.FieldH),
        IF(STARTS_WITH('Country-JwOJ','Fee-NLk1'),'Customer-eWrl','CCY-ZpzR'))
        AS Feevrju,
      IF(
        STARTS_WITH(DerivedTable3.CustomereBLI,DerivedTable2.Amtp4ee),
        IF('Account-KvYz' <> 'Code-TLlC','Account-ndX4',DerivedTable3.FeeD0Po),
        CASE
          WHEN 'Code-Kixa' != 'Customer-g7uS' THEN
            DerivedTable3.CustomerULyd
          WHEN 'Address-nuX9' NOT LIKE DerivedTable2.AccountctCy THEN
            DerivedTable2.CodeDmva
          WHEN ENDS_WITH(DerivedTable1.CustomerWrlz,'Amt-7FFA') THEN
            'CCY-RTki'
          ELSE
            'Code-5GHC'
        END)
        AS AccountBLmg,
      CASE
        WHEN 'Amt-bdTS' LIKE 'Fee-CJMN' THEN
          CASE
            WHEN 'Code-VQzA' <> 'Code-3zVJ' THEN
              DerivedTable3.CodeVHWz
            WHEN ENDS_WITH(DerivedTable1.AmtR86V,DerivedTable3.FeeqkjX) THEN
              'Country-2KtR'
            WHEN 'CCY-HS7f' = 'Account-OJ2B' THEN
              SourceD.FieldC
            WHEN 'Fee-UWwN' != 'Customer-fBDA' THEN
              'Address-KoMy'
            WHEN ENDS_WITH('Amt-e9Qj',SourceB.FieldI) THEN
              'Address-pdvC'
            ELSE
              SourceB.FieldD
          END
        WHEN STARTS_WITH(DerivedTable3.CountryoFFA,'Country-Ho6S') THEN
          IF(ENDS_WITH(SourceD.FieldC,'Customer-8Rqt'),'Amt-GcSf','Code-w2QY')
        WHEN 'CCY-Et1l' NOT LIKE DerivedTable3.FeeT7G9 THEN
          IF(
            ENDS_WITH('Account-PZGj',DerivedTable5.AddressV4AX), SourceD.FieldJ,
            'Fee-XmzX')
        ELSE
          IF(STARTS_WITH('Account-viac','Amt-Q4rT'),'Customer-brvB','CCY-hdUo')
      END
        AS CodeNAHw,
      IF(
        ENDS_WITH('Account-onrO',SourceC.FieldC),
        IF('Customer-REvN' != 'Amt-zgth','Code-wM9v',DerivedTable2.AmtNncS),
        IF(
          ENDS_WITH('Customer-2AZE','Amt-Zskr'), SourceB.FieldF,
          DerivedTable3.FeeCUde))
        AS AddressR2r0,
      IF(
        ENDS_WITH('Code-EXpD','Fee-6SHp'),
        IF(
          STARTS_WITH(DerivedTable3.CodeUeLq,SourceA.FieldI), 'CCY-p8VF',
          'Code-BYks'), IF(
                          STARTS_WITH('Address-XOvQ',SourceC.FieldB),
                          SourceD.FieldG, 'Code-Ajq8'))
        AS CountrysK1v,
      IF(
        'Fee-iFsd' <> 'Country-vl3n',
        IF(SourceC.FieldE = 'Code-V2Lh','Address-WpSq','Fee-cWri'),
        IF(
          STARTS_WITH('Address-IXV4','Amt-Q2Wp'), SourceD.FieldG,
          SourceC.FieldF))
        AS CustomerYZsr,
      IF(
        SourceB.FieldF LIKE DerivedTable3.CCYO742,
        IF(SourceA.FieldE LIKE 'Country-4D7n','Fee-gurE',SourceA.FieldG),
        CASE
          WHEN SourceE.FieldI != 'Address-QTHN' THEN
            DerivedTable2.CodeDmva
          WHEN STARTS_WITH(SourceA.FieldC,'Account-ncn7') THEN
            DerivedTable2.AddressHdnu
          WHEN ENDS_WITH(DerivedTable1.CustomerKrwR,'CCY-lk23') THEN
            DerivedTable3.CountryoFFA
          ELSE
            DerivedTable3.FeeF6P4
        END)
        AS Countrym03Z,
      CASE
        WHEN 'Customer-hJu2' NOT LIKE SourceD.FieldD THEN
          IF(
            DerivedTable1.AmtzEYq LIKE 'Fee-lQp5', DerivedTable3.CountrygPvi,
            DerivedTable1.AmtR86V)
        WHEN ENDS_WITH('Customer-LrSX','Code-EaA9') THEN
          IF(
            ENDS_WITH('Address-SqnK',DerivedTable1.AccountfEJK), SourceB.FieldH,
            SourceD.FieldD)
        ELSE
          IF(
            'Address-UDIO' LIKE DerivedTable3.FeeqkjX, 'Address-EA4w',
            'Code-yvMT')
      END
        AS CountryavC2,
      CASE
        WHEN ENDS_WITH('CCY-v05r',DerivedTable3.CustomerLP0M) THEN
          IF(
            STARTS_WITH(DerivedTable5.AddressV4AX,SourceD.FieldH), 'Amt-G7Ax',
            'CCY-azaF')
        WHEN STARTS_WITH(DerivedTable1.Codeuhr9,SourceE.FieldA) THEN
          IF('Address-Ya56' = 'Fee-oLut',SourceC.FieldG,DerivedTable5.CCY76AX)
        WHEN SourceE.FieldJ NOT LIKE 'Account-QLMJ' THEN
          IF(
            STARTS_WITH('Amt-6ErZ',DerivedTable1.CustomerWrlz), 'Fee-TDCh',
            'CCY-alMZ')
        WHEN SourceD.FieldG = DerivedTable5.Code7INp THEN
          IF(
            DerivedTable2.AccountctCy <> 'Customer-Txjo', 'Fee-29cJ',
            DerivedTable2.AmtNncS)
        WHEN STARTS_WITH(DerivedTable3.CCYO742,DerivedTable2.Amt885g) THEN
          IF(
            STARTS_WITH('Country-3W5k','Customer-jEJl'), SourceE.FieldE,
            'Code-2zeM')
        ELSE
          IF(ENDS_WITH('Country-fS8k','Code-9qie'),'CCY-Tz9d','Code-YKPZ')
      END
        AS AccountJG1r,
      CASE
        WHEN STARTS_WITH(SourceE.FieldA,DerivedTable3.CustomereBLI) THEN
          IF(
            DerivedTable5.AddressV4AX LIKE SourceE.FieldC, 'Fee-ti6P',
            'Fee-yCKF')
        WHEN STARTS_WITH('Address-MNzf',DerivedTable2.Address5RWP) THEN
          IF(
            DerivedTable1.CustomerWpfX = 'Customer-pJ27', SourceE.FieldG,
            SourceA.FieldA)
        ELSE
          IF('Country-50aH' NOT LIKE 'CCY-gyum','Account-iAAg','Amt-bQwY')
      END
        AS FeeDLkv,
      IF(
        'Account-a9a0' = 'CCY-05Qj',
        IF(
          SourceE.FieldE = DerivedTable2.Amt885g, SourceA.FieldB,
          'Customer-FZTL'),
        CASE
          WHEN STARTS_WITH('Fee-ionk','Code-dyGt') THEN
            'Customer-bAGs'
          WHEN SourceA.FieldA LIKE DerivedTable3.CustomerLP0M THEN
            DerivedTable5.Customera0qh
          WHEN DerivedTable5.Feepmor NOT LIKE 'Country-2RdZ' THEN
            'CCY-mkoR'
          WHEN 'Customer-aHKG' != 'Account-GOYk' THEN
            'Code-lkFW'
          ELSE
            DerivedTable5.FeeogF5
        END)
        AS AccounthcEB
    FROM
      testdataset.SourceB AS SourceB
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH(SourceB.FieldD,SourceA.FieldJ) AND
         SourceA.FieldA <> 'Account-C7qg' AND
         SourceA.FieldH LIKE 'Customer-cFQf' AND
         ENDS_WITH('Customer-3fw5',SourceA.FieldH))
      INNER JOIN testdataset.SourceC AS SourceC ON
        ('Code-jFtI' <> SourceA.FieldA OR SourceB.FieldJ = SourceC.FieldJ OR
         'CCY-2eJg' LIKE SourceA.FieldI OR 'Customer-uXl5' LIKE 'Amt-qn4B')
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Amt-l0dd',SourceD.FieldJ) OR
         STARTS_WITH('Account-nUdX',SourceD.FieldA) OR
         STARTS_WITH(SourceB.FieldB,'Fee-eW0s') OR
         STARTS_WITH(SourceC.FieldI,SourceC.FieldG))
      INNER JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH('Country-Wt62',SourceC.FieldG) OR
         'Amt-k4By' LIKE SourceC.FieldF OR
         'Customer-Rljq' <> SourceC.FieldG AND
         'Account-cLa5' NOT LIKE SourceD.FieldC)
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH(SourceE.FieldJ,'Account-Lzfl') OR
         SourceD.FieldE != DerivedTable1.Addressrgjd AND
         STARTS_WITH(SourceD.FieldI,'Country-vd5w') OR
         ENDS_WITH('Fee-TRQV',SourceA.FieldG))
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (DerivedTable2.Address5RWP <> DerivedTable2.AccounttVM1 AND
         'Code-tcqC' NOT LIKE 'Country-lFiB' OR
         ENDS_WITH(DerivedTable1.CountryV1io,DerivedTable2.AmtmorU) OR
         DerivedTable1.AddressoeLk = 'Customer-wIU8')
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (SourceA.FieldA != DerivedTable1.AccountqcVc AND
         ENDS_WITH('Address-XUK0','Account-4Sfj') OR
         'Address-6vbm' = 'CCY-zGUm' AND
         SourceC.FieldF != DerivedTable3.CCYO742)
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        ('Amt-Tka5' = 'Code-2GvR' OR
         'Customer-m5t0' NOT LIKE DerivedTable1.AccountqcVc OR
         ENDS_WITH(SourceC.FieldE,'Customer-7QcU') OR
         SourceC.FieldI = 'Code-4lwr')
    WHERE
      'Amt-Nj0C' LIKE SourceD.FieldA AND STARTS_WITH('Amt-NMr2','Code-IAge') OR
      ENDS_WITH('Customer-1xwR',SourceB.FieldC) OR
      ENDS_WITH(DerivedTable3.CodeVHWz,'Fee-3Ptm') AND
      'Code-3oFe' LIKE DerivedTable2.CodeRziT AND
      'CCY-GcD8' != SourceE.FieldA AND
      STARTS_WITH('CCY-gI89','Code-9Sbs') OR SourceB.FieldJ <> SourceC.FieldF
  ),
  Table27 AS (
    SELECT
      IF(
        'Country-EoaB' = SourceA.FieldF,
        IF(
          ENDS_WITH('Country-2FO5','Country-Yaud'), DerivedTable4.AddressB6KJ,
          'Code-ZFJM'), IF(
                          ENDS_WITH(SourceA.FieldF,DerivedTable2.AmtmorU),
                          SourceA.FieldI, 'Country-H96B'))
        AS Fee4lfH,
      IF(
        SourceB.FieldB LIKE 'Customer-cYpE',
        IF(
          STARTS_WITH('Amt-LrVk','Customer-5lig'), SourceA.FieldB,
          'Country-zrAb'),
        IF(
          DerivedTable4.FeezLpC NOT LIKE DerivedTable3.Codeaqvh,
          DerivedTable5.AddressV4AX, DerivedTable2.AccounttVM1))
        AS CodecTJM,
      IF(
        ENDS_WITH(DerivedTable3.Codeaqvh,DerivedTable4.AccountMw3l),
        IF(
          DerivedTable5.CCYVD4h LIKE DerivedTable2.AccounttVM1, 'Customer-Bmna',
          SourceC.FieldI),
        IF('Fee-sFxo' LIKE DerivedTable4.CodeJM19,SourceD.FieldI,'Fee-tqLG'))
        AS Accounttxhn,
      IF(
        STARTS_WITH('Country-YLdY',SourceB.FieldC),
        CASE
          WHEN ENDS_WITH('Customer-pWfN',SourceA.FieldJ) THEN
            'Amt-cQmV'
          WHEN ENDS_WITH('Fee-pRbh','Country-MWiN') THEN
            DerivedTable4.FeezLpC
          WHEN 'CCY-0WpP' LIKE 'Customer-fTmX' THEN
            SourceA.FieldE
          ELSE
            DerivedTable4.AddressB6KJ
        END,
        CASE
          WHEN
            ENDS_WITH(SourceD.FieldG,DerivedTable4.CodeJM19)
          THEN
            'Customer-nvuA'
          WHEN
            SourceC.FieldD != SourceC.FieldE
          THEN
            DerivedTable3.Code3Xip
          WHEN
            'Address-r1E7' <> DerivedTable3.Code3Xip
          THEN
            SourceA.FieldJ
          WHEN
            ENDS_WITH(DerivedTable3.CustomerULyd,DerivedTable4.Addressm87D)
          THEN
            'Customer-qDJM'
          ELSE
            SourceC.FieldA
        END)
        AS Code8uGc,
      IF(
        DerivedTable2.CountrywaWi != 'Country-IFnA',
        IF(
          STARTS_WITH('Country-8Bz0',DerivedTable4.CCYY6No), 'CCY-JxcL',
          'Address-BEg9'), IF(
                             DerivedTable2.AccountctCy = 'Code-9Nay',
                             DerivedTable2.AmtNncS, DerivedTable3.CustomerLP0M))
        AS CustomersVEq,
      IF(
        DerivedTable4.FeexvTE = 'Account-3yrK',
        IF(
          DerivedTable4.Country8VtM != SourceD.FieldG, SourceC.FieldI,
          DerivedTable4.CCYY6No),
        IF('Customer-uJlb' = 'Customer-DwSh',SourceA.FieldF,'Address-z8KW'))
        AS FeeelOV,
      IF(
        STARTS_WITH('CCY-pEC1',SourceA.FieldD),
        IF(
          STARTS_WITH('Customer-Scb9',DerivedTable4.CustomerCDJF),
          DerivedTable5.CCYVD4h, SourceB.FieldI),
        IF(
          DerivedTable4.Country8VtM NOT LIKE 'Fee-zIKK',
          DerivedTable2.Account61kv, SourceB.FieldE))
        AS Address9INh,
      IF(
        'Customer-IRc6' = 'Customer-Hnna',
        IF(
          DerivedTable2.AddressHdnu NOT LIKE 'Fee-63ii', 'Account-iDqR',
          'Address-3Lzu'),
        IF('Account-kNMV' LIKE 'Fee-XcUZ','CCY-9n0g','Address-Ezme'))
        AS CodeDoG3,
      IF(
        STARTS_WITH('CCY-mh58','Code-p4e7'),
        IF(ENDS_WITH(SourceC.FieldH,'Fee-URzr'),'Code-BBAk','Code-jVa8'),
        IF(
          SourceC.FieldJ NOT LIKE DerivedTable5.FeeogF5, SourceD.FieldB,
          'Address-WCzj'))
        AS CodezbdN,
      IF(
        DerivedTable3.CountrygPvi NOT LIKE 'Amt-FtPD',
        IF(
          ENDS_WITH(SourceA.FieldI,'Address-U3SL'), 'Address-3Klu',
          DerivedTable2.AccountrJZf),
        IF(
          'CCY-8V91' <> 'Country-m98u', DerivedTable4.CodelvtV,
          DerivedTable2.Amtp4ee))
        AS Account4KpN,
      IF(
        STARTS_WITH(DerivedTable2.CodeRziT,'Address-vCEM'),
        CASE
          WHEN 'Amt-6TEC' = DerivedTable5.Customera0qh THEN
            'Amt-os1k'
          WHEN ENDS_WITH('Address-qajx',SourceC.FieldA) THEN
            DerivedTable3.CountrygMB4
          WHEN SourceB.FieldG <> 'CCY-6a0e' THEN
            DerivedTable5.CCY76AX
          ELSE
            'Customer-4uAe'
        END, IF(
               ENDS_WITH('CCY-1jfK','Account-GWmP'), 'Address-70X6',
               DerivedTable4.AccountMw3l))
        AS Amtuba6,
      IF(
        STARTS_WITH(SourceA.FieldA,DerivedTable3.CountrygPvi),
        CASE
          WHEN DerivedTable3.CodeUeLq = 'Amt-pMJP' THEN
            SourceA.FieldG
          WHEN STARTS_WITH(DerivedTable4.CCYY6No,'Account-Mgl9') THEN
            'Amt-11zF'
          WHEN ENDS_WITH(DerivedTable3.FeeT7G9,DerivedTable2.CodeRziT) THEN
            'Country-EooP'
          WHEN STARTS_WITH('Address-BdyB',DerivedTable4.FeexvTE) THEN
            'Amt-zxN8'
          ELSE
            SourceB.FieldH
        END,
        IF(STARTS_WITH('Code-3gf1',SourceA.FieldD),'Fee-PuEu','Customer-Jn42'))
        AS AccountrVGO
    FROM
      testdataset.SourceA AS SourceA
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('Address-xUJp','Fee-c2uY') AND
         STARTS_WITH('Code-ylld','CCY-ICjE') AND
         ENDS_WITH('Account-K9kO','Country-JUvy') OR
         'Amt-N6Az' NOT LIKE 'Customer-QZkt')
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        ('Account-Irem' NOT LIKE 'Amt-3WU8' OR
         ENDS_WITH(SourceB.FieldB,'Customer-pWA8') AND
         ENDS_WITH(SourceC.FieldG,SourceC.FieldB) AND
         SourceA.FieldI <> SourceC.FieldA)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Country-OTcM','Country-MRTi') AND
         'Address-1hyo' = SourceC.FieldB OR
         'CCY-j1m8' LIKE SourceC.FieldD OR 'Amt-Yx0z' <> 'Code-GZrz')
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (SourceC.FieldF = 'Amt-275K' AND
         STARTS_WITH('Customer-73r9','Country-Fk5z') OR
         STARTS_WITH(SourceC.FieldD,SourceC.FieldE) OR
         STARTS_WITH(SourceC.FieldF,'Customer-lSNz'))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH(DerivedTable3.CustomereBLI,DerivedTable2.AccountctCy) AND
         'Code-ikGk' = 'Code-5Om8' AND
         'Address-EUfN' NOT LIKE DerivedTable2.CodeRziT AND
         DerivedTable2.CodeRziT LIKE DerivedTable2.CodeDmva)
      LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (SourceB.FieldF LIKE 'Address-kp4F' OR
         SourceD.FieldB NOT LIKE 'Customer-SfJR' OR
         'CCY-kmT0' LIKE 'CCY-3LAq' AND 'Fee-TN9c' != 'Amt-c6Fd')
      LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (DerivedTable2.Address5RWP = DerivedTable3.CodeVHWz AND
         ENDS_WITH(DerivedTable2.Address7oYI,DerivedTable3.FeeF6P4) OR
         STARTS_WITH('Code-fkww',SourceD.FieldG) OR
         ENDS_WITH('Address-neuJ','Fee-Bk95'))
    WHERE
      'Customer-7bx7' <> 'Customer-8JqR' OR
      ENDS_WITH(SourceA.FieldB,'Customer-eltJ') OR
      'Customer-12GM' != 'Fee-6AFi' AND
      'Country-xtqH' NOT LIKE SourceA.FieldB OR
      'Country-nQ6j' NOT LIKE SourceB.FieldI AND
      SourceC.FieldE LIKE DerivedTable4.FeezLpC OR
      STARTS_WITH('CCY-bszr',DerivedTable3.FeeT7G9) OR
      'Code-07nP' != DerivedTable3.FeeT7G9
  ),
  Table28 AS (
    SELECT
      CASE
        WHEN 'Account-NeKc' = 'Address-CmQF' THEN
          IF('CCY-jx2r' = 'Account-XkHS','Address-MM5S','Customer-mxZC')
        WHEN ENDS_WITH('Account-dSVc','Address-9wL4') THEN
          CASE
            WHEN STARTS_WITH('Fee-RA69','Fee-Z1rc') THEN
              Table27.AccountrVGO
            WHEN ENDS_WITH(DerivedTable2.Account61kv,'Country-Cb0y') THEN
              Table27.Code8uGc
            WHEN DerivedTable3.CodeVHWz = DerivedTable2.AmtmorU THEN
              'Amt-y8Qw'
            WHEN ENDS_WITH('Fee-PiC2','CCY-VI7v') THEN
              'Customer-OO9E'
            WHEN 'CCY-TlFH' = 'Account-HCqL' THEN
              'CCY-xZXc'
            ELSE
              SourceA.FieldF
          END
        ELSE
          IF(
            ENDS_WITH('Amt-F1Qp','Fee-P1HT'), DerivedTable3.FeeqkjX,
            SourceC.FieldB)
      END
        AS AddressJUrf,
      IF(
        'Amt-JvEq' <> 'Country-kblv',
        CASE
          WHEN 'Account-G2Gx' LIKE SourceD.FieldC THEN
            DerivedTable1.AccountqcVc
          WHEN ENDS_WITH(Table27.Fee4lfH,'Country-RkRA') THEN
            'CCY-cZyw'
          WHEN SourceD.FieldD NOT LIKE 'CCY-jRpi' THEN
            'Customer-GW4U'
          ELSE
            SourceA.FieldB
        END, IF(
               STARTS_WITH('Code-V3EK',SourceC.FieldJ),
               DerivedTable5.CountrynpDj, SourceA.FieldH))
        AS Amt5uao,
      IF(
        ENDS_WITH('Customer-OhV9','Customer-lOMs'),
        IF('Account-Ac69' = 'Customer-xmzb',SourceA.FieldF,'Address-VBci'),
        IF('Amt-dNYs' <> 'CCY-jPRu','Address-hZP1',SourceD.FieldG))
        AS AccountAOUE,
      IF(
        ENDS_WITH(DerivedTable1.CustomerKrwR,DerivedTable2.AmtNncS),
        IF(
          Table27.AccountrVGO NOT LIKE 'Code-dRu6', 'Fee-M8Bt',
          DerivedTable1.CustomerKrwR),
        CASE
          WHEN STARTS_WITH('Fee-TKeI',DerivedTable1.CCYXSkS) THEN
            'Address-ldbb'
          WHEN ENDS_WITH(SourceA.FieldC,SourceA.FieldC) THEN
            'Customer-AQAo'
          WHEN 'Code-MHNR' = 'Code-1tj0' THEN
            DerivedTable2.AccountrJZf
          WHEN ENDS_WITH('CCY-atPc',SourceB.FieldA) THEN
            'Amt-BG3n'
          ELSE
            'Account-svk9'
        END)
        AS AddressOcLL,
      IF(
        STARTS_WITH('CCY-77th','Amt-UbLE'),
        CASE
          WHEN ENDS_WITH(SourceB.FieldI,SourceA.FieldF) THEN
            DerivedTable2.Address7oYI
          WHEN 'Amt-0qcB' <> DerivedTable3.Feeg8kl THEN
            DerivedTable2.Address5RWP
          ELSE
            'Amt-waoi'
        END, IF(
               'Fee-yGjU' LIKE 'Account-imVU', 'Address-S526',
               DerivedTable1.AccountfEJK))
        AS CustomerzprS,
      CASE
        WHEN ENDS_WITH('Account-Oi70','Fee-fY5E') THEN
          IF(
            ENDS_WITH(SourceA.FieldE,SourceB.FieldC), SourceA.FieldH,
            SourceD.FieldJ)
        WHEN DerivedTable2.Amtp4ee NOT LIKE DerivedTable1.AmtzEYq THEN
          IF(
            ENDS_WITH(Table27.Address9INh,'Address-ZaAs'),
            DerivedTable5.CCYVD4h, DerivedTable1.Codeuhr9)
        WHEN 'Customer-zJuU' = 'Amt-2ngZ' THEN
          IF('Address-NYG3' <> 'Code-O6sy',SourceC.FieldA,'Account-9Iai')
        WHEN STARTS_WITH('Account-oQEy',DerivedTable1.CustomerKrwR) THEN
          IF(
            Table27.CodeDoG3 = DerivedTable5.CodePGRC,
            DerivedTable2.AccountrJZf, SourceD.FieldA)
        ELSE
          IF(
            SourceB.FieldF <> 'Country-JjdX', DerivedTable3.CountrygPvi,
            DerivedTable5.CodePGRC)
      END
        AS AccountatGX,
      IF(
        SourceC.FieldD <> 'Customer-XJcP',
        IF(
          SourceA.FieldB LIKE DerivedTable2.AddresscM17, 'Customer-aUwz',
          'Code-ZSSs'), IF(
                          STARTS_WITH(Table27.FeeelOV,DerivedTable5.CodePGRC),
                          DerivedTable2.Address5RWP, 'Address-JZvB'))
        AS FeeRfkh,
      IF(
        ENDS_WITH('Customer-SpEz',DerivedTable5.CodePGRC),
        IF(
          STARTS_WITH('CCY-QhBN',Table27.CustomersVEq), DerivedTable3.CodeUeLq,
          DerivedTable5.CountrynpDj),
        IF(STARTS_WITH(SourceC.FieldH,'Amt-YwEA'),'Address-o5A5','CCY-iCTM'))
        AS AccountG3dN,
      IF(
        SourceB.FieldC <> SourceA.FieldJ,
        IF(
          'Fee-Pop2' NOT LIKE DerivedTable3.CustomereBLI, SourceC.FieldH,
          Table27.Account4KpN),
        IF(STARTS_WITH('Country-nR3v','Code-bJUJ'),'Fee-Ia9b',SourceD.FieldH))
        AS AddresssIgw,
      IF(
        ENDS_WITH(DerivedTable3.CodeVHWz,DerivedTable5.Feepmor),
        IF(
          ENDS_WITH(DerivedTable1.CustomerKrwR,DerivedTable1.AccountqcVc),
          SourceB.FieldC, 'CCY-7wU6'),
        IF(
          'CCY-onnX' LIKE SourceC.FieldG, DerivedTable3.CustomerULyd,
          'Address-dN81'))
        AS Accountg5SY,
      IF(
        DerivedTable3.FeeCUde = DerivedTable2.AmtmorU,
        IF(
          'Country-5yG9' LIKE DerivedTable3.CountrygMB4, DerivedTable3.FeeF6P4,
          'Code-53XK'), IF(
                          SourceC.FieldG <> DerivedTable3.CountryoFFA,
                          'Country-EW2L', DerivedTable3.Feeg8kl))
        AS CustomerTQGK,
      CASE
        WHEN ENDS_WITH('Account-3NNN',SourceC.FieldB) THEN
          IF(
            'CCY-97Ar' <> DerivedTable3.AccountViga, SourceA.FieldE,
            DerivedTable2.Address7oYI)
        WHEN DerivedTable3.FeeT7G9 = DerivedTable3.CustomerLP0M THEN
          IF('CCY-uxAh' LIKE DerivedTable1.FeerBIS,'Code-lHuv',SourceD.FieldE)
        WHEN 'CCY-1N9A' NOT LIKE SourceB.FieldG THEN
          IF(
            'Country-THXu' NOT LIKE DerivedTable3.FeeD0Po, 'Address-K9Z5',
            DerivedTable3.CustomereBLI)
        WHEN ENDS_WITH('Code-F01b','Address-6Kn3') THEN
          IF(
            'Customer-Pu1y' = DerivedTable3.FeeF6P4, DerivedTable3.CountryoFFA,
            'Address-VrZ8')
        ELSE
          IF(
            DerivedTable5.Feepmor <> 'Customer-k5G2', SourceA.FieldF,
            DerivedTable2.Account61kv)
      END
        AS CustomerD3nz,
      IF(
        SourceD.FieldC <> DerivedTable5.CodePGRC,
        IF(
          DerivedTable3.CustomerULyd != 'Address-ytlA', SourceB.FieldF,
          'CCY-R9ng'),
        IF(
          STARTS_WITH(DerivedTable3.CodeUeLq,DerivedTable3.CustomerULyd),
          DerivedTable3.CodeUeLq, SourceC.FieldF))
        AS CCYZz2i,
      IF(
        STARTS_WITH(DerivedTable5.Code7INp,SourceB.FieldJ),
        IF(STARTS_WITH('CCY-xewB','Code-STWh'),'Fee-g74F','Address-ABdY'),
        CASE
          WHEN STARTS_WITH(Table27.Account4KpN,DerivedTable5.Feepmor) THEN
            'Country-80Tq'
          WHEN DerivedTable3.CCYO742 NOT LIKE 'Country-vaBl' THEN
            DerivedTable2.Address7oYI
          ELSE
            SourceD.FieldE
        END)
        AS Account45kz,
      IF(
        'Fee-DU54' LIKE DerivedTable1.CustomerWpfX,
        IF(
          'CCY-QcR0' LIKE DerivedTable1.CountryV1io, 'Country-ieoH',
          'Country-PDBn'), IF(
                             SourceC.FieldC = DerivedTable2.AmtNncS,
                             'Account-hB96', DerivedTable3.CustomereBLI))
        AS CCYGKA8,
      IF(
        'Amt-8R3J' = 'Customer-ZXSL',
        CASE
          WHEN STARTS_WITH(SourceC.FieldE,DerivedTable5.FeeogF5) THEN
            'CCY-Wzgk'
          WHEN STARTS_WITH('Country-cByW','Country-4kea') THEN
            'Account-y7BU'
          WHEN SourceB.FieldA = DerivedTable2.AddressHdnu THEN
            DerivedTable5.AddressV4AX
          WHEN SourceD.FieldD <> 'Address-69Z2' THEN
            SourceB.FieldJ
          ELSE
            Table27.FeeelOV
        END, IF(
               'CCY-OLbI' != DerivedTable5.CodePGRC, DerivedTable2.CodeDmva,
               'Amt-Uq1l'))
        AS CodeQ1hZ
    FROM
      testdataset.DerivedTable3 AS DerivedTable3
      INNER JOIN testdataset.SourceA AS SourceA ON
        ('Account-4BmW' LIKE DerivedTable3.CountrygMB4 OR
         STARTS_WITH('Amt-fkaR',DerivedTable3.CustomerULyd) AND
         ENDS_WITH(DerivedTable3.Codeaqvh,'Customer-Wltg') AND
         DerivedTable3.Feeg8kl LIKE 'Address-snI7')
      INNER JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('Fee-Ho2n',DerivedTable3.CountrygMB4) OR
         SourceB.FieldG = 'Code-pLx6' OR
         ENDS_WITH(SourceA.FieldI,SourceA.FieldH) OR
         ENDS_WITH(SourceB.FieldF,DerivedTable3.CountryoFFA))
      INNER JOIN testdataset.SourceC AS SourceC ON
        ('Address-og8Q' <> DerivedTable3.Feeg8kl AND
         ENDS_WITH(SourceA.FieldG,'Amt-SSd9') OR
         DerivedTable3.CCYO742 NOT LIKE 'Fee-ESXT' AND
         ENDS_WITH('Code-DZpq','Account-lket'))
      INNER JOIN testdataset.SourceD AS SourceD ON
        ('Country-qBjR' <> 'Amt-uoa4' AND
         ENDS_WITH('Fee-xEza',DerivedTable3.FeeqkjX) AND
         STARTS_WITH('Fee-sIcp','CCY-jxhy') OR
         ENDS_WITH('Address-TqVF',SourceA.FieldJ))
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (DerivedTable3.CountrygMB4 LIKE DerivedTable3.CustomereBLI OR
         ENDS_WITH(SourceD.FieldI,'Code-JzAI') AND
         'Amt-aDUE' NOT LIKE SourceD.FieldA OR 'Account-Tqlz' <> 'Fee-F10f')
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (DerivedTable2.AddresscM17 != 'Fee-zfyc' OR
         SourceD.FieldJ <> 'Account-Jdec' AND
         STARTS_WITH(DerivedTable3.FeeqkjX,'CCY-KGdK') OR
         STARTS_WITH('Country-4l71',DerivedTable1.CCYXSkS))
      LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (SourceC.FieldB NOT LIKE SourceB.FieldD OR
         ENDS_WITH('CCY-km5o','Code-9JBF') OR
         ENDS_WITH('Account-tTkC','Customer-QHuJ') AND
         SourceD.FieldE = 'Address-MaSl')
      LEFT JOIN Table27 AS Table27 ON
        ('Customer-fkjf' NOT LIKE Table27.AccountrVGO OR
         ENDS_WITH('Address-sDcZ',DerivedTable3.FeepQGh) AND
         DerivedTable1.CustomerWpfX <> 'Country-Y5ED' AND
         SourceC.FieldB LIKE SourceA.FieldI)
    WHERE
      'CCY-MAxZ' NOT LIKE 'Address-INto' AND
      STARTS_WITH(DerivedTable1.CustomerWrlz,SourceB.FieldE) OR
      STARTS_WITH('Customer-j1do',DerivedTable2.AmtmorU) OR
      'Address-LiuE' = 'Amt-R8P5' AND
      STARTS_WITH(DerivedTable2.AccountctCy,'Address-d4Nn') OR
      'Fee-GZDh' NOT LIKE DerivedTable3.CustomerULyd OR
      ENDS_WITH(DerivedTable1.AccountmNQo,Table27.Code8uGc) AND
      'Amt-8yBR' NOT LIKE 'Customer-fWYr'
  ),
  Table29 AS (
    SELECT
      IF(
        STARTS_WITH('Account-OsTd',DerivedTable5.Customera0qh),
        IF(ENDS_WITH(SourceC.FieldJ,'Amt-7vtg'),'CCY-D8Pg','Account-hV0R'),
        CASE
          WHEN ENDS_WITH(DerivedTable3.CountryoFFA,'Fee-mE8t') THEN
            SourceE.FieldG
          WHEN 'Country-aPU7' LIKE 'Customer-RSVE' THEN
            SourceD.FieldA
          WHEN DerivedTable5.AddressV4AX <> 'CCY-xODr' THEN
            'Country-GC8k'
          ELSE
            DerivedTable4.FeesrBh
        END)
        AS AmtDrPD,
      CASE
        WHEN STARTS_WITH(DerivedTable3.CountrygMB4,'Customer-K2WA') THEN
          CASE
            WHEN ENDS_WITH('Address-mDCL','Customer-V5h4') THEN
              'Amt-0Ykb'
            WHEN STARTS_WITH(DerivedTable3.CCYO742,'Address-l2wK') THEN
              Table27.CodezbdN
            ELSE
              'Customer-x7D4'
          END
        WHEN ENDS_WITH('Address-RJCm',DerivedTable1.CustomerWpfX) THEN
          IF(
            ENDS_WITH('Country-ZtJr',Table27.Amtuba6), 'Address-apMJ',
            SourceA.FieldC)
        ELSE
          CASE
            WHEN
              ENDS_WITH(DerivedTable1.Codeuhr9,Table28.CCYZz2i)
            THEN
              'Customer-vO3Q'
            WHEN
              ENDS_WITH(DerivedTable1.CustomerWrlz,DerivedTable1.CountryV1io)
            THEN
              'Country-NHx7'
            WHEN
              DerivedTable3.FeeT7G9 LIKE DerivedTable4.Country5f1u
            THEN
              'Customer-Cl3A'
            ELSE
              SourceC.FieldB
          END
      END
        AS AccountXney,
      IF(
        SourceC.FieldJ = SourceA.FieldA,
        IF(
          Table28.CustomerD3nz <> DerivedTable1.AccountKwSP, 'Amt-418J',
          Table28.Accountg5SY),
        IF(
          ENDS_WITH(DerivedTable5.Feepmor,'Account-L91S'),
          DerivedTable4.FeexvTE, Table27.CodeDoG3))
        AS FeeavaS,
      CASE
        WHEN STARTS_WITH('CCY-4Svf','Code-6T3j') THEN
          IF(SourceD.FieldG LIKE 'Account-EGeQ',SourceD.FieldJ,SourceB.FieldI)
        WHEN ENDS_WITH('Fee-kmKR','Country-HfQX') THEN
          IF(ENDS_WITH('Address-mmt8','Country-vVlG'),'Fee-XvAN','Amt-wZbe')
        WHEN 'Code-0QoA' LIKE Table28.Account45kz THEN
          IF(
            ENDS_WITH(SourceB.FieldA,'Address-eMec'), 'Account-bL4y',
            DerivedTable4.Country5f1u)
        WHEN 'Fee-2oQy' <> DerivedTable3.CountryoFFA THEN
          CASE
            WHEN ENDS_WITH(DerivedTable1.AccountqcVc,'Amt-iJs6') THEN
              'Fee-hr7S'
            WHEN ENDS_WITH(SourceD.FieldF,DerivedTable4.Addressm87D) THEN
              'Address-1rZL'
            WHEN ENDS_WITH(Table27.CodecTJM,Table28.Amt5uao) THEN
              'Country-2cIS'
            ELSE
              Table28.CCYGKA8
          END
        WHEN 'CCY-UWwg' <> SourceA.FieldE THEN
          IF(
            STARTS_WITH(DerivedTable5.CCYQK2Q,DerivedTable3.Feeg8kl),
            'Amt-aWaM', DerivedTable3.Account4sxv)
        ELSE
          CASE
            WHEN 'Fee-okij' <> DerivedTable4.AccounteutQ THEN
              'CCY-xBUv'
            WHEN 'Address-RQIM' NOT LIKE SourceC.FieldF THEN
              SourceC.FieldC
            WHEN 'Customer-83tV' != 'Address-ytAw' THEN
              Table28.AccountAOUE
            WHEN 'Customer-BGzu' = DerivedTable5.CodePGRC THEN
              'Customer-vdcI'
            WHEN ENDS_WITH(SourceD.FieldA,SourceD.FieldB) THEN
              DerivedTable3.FeeD0Po
            ELSE
              SourceB.FieldG
          END
      END
        AS CustomerSHFG,
      IF(
        'Account-4rNZ' <> SourceE.FieldE,
        CASE
          WHEN Table27.CodeDoG3 LIKE DerivedTable5.AddressV4AX THEN
            'Code-h8CH'
          WHEN STARTS_WITH(SourceA.FieldA,DerivedTable3.FeeD0Po) THEN
            SourceB.FieldJ
          WHEN STARTS_WITH(Table27.FeeelOV,'Code-ihab') THEN
            DerivedTable5.FeeogF5
          WHEN DerivedTable3.FeepQGh <> 'Country-Mi9h' THEN
            DerivedTable4.AccounteutQ
          WHEN 'Address-HNjB' = 'Customer-fokh' THEN
            Table28.AddresssIgw
          ELSE
            Table27.Code8uGc
        END, IF(
               ENDS_WITH(Table28.AccountG3dN,'Account-CWfu'), 'Code-q62J',
               'Account-sLVI'))
        AS Customerpb4F,
      CASE
        WHEN DerivedTable4.CodeJM19 NOT LIKE Table28.AccountG3dN THEN
          IF(DerivedTable3.Feeg8kl = 'Address-UHNF',SourceE.FieldC,'Fee-c5s8')
        WHEN DerivedTable5.AddressV4AX != 'Fee-qrD4' THEN
          CASE
            WHEN DerivedTable4.AddressB6KJ <> 'Fee-Wwbx' THEN
              Table28.AccountG3dN
            WHEN DerivedTable4.AddressWvnP = DerivedTable4.Country8VtM THEN
              'Account-BV3I'
            WHEN DerivedTable4.AddressB6KJ = 'CCY-4iMh' THEN
              'Fee-yQOd'
            WHEN SourceD.FieldA != 'Account-bdAz' THEN
              'Address-1OiV'
            ELSE
              SourceE.FieldB
          END
        WHEN STARTS_WITH('Address-IJMn',DerivedTable5.CCYVD4h) THEN
          IF(
            STARTS_WITH(SourceD.FieldA,'Fee-jdOJ'), Table27.Address9INh,
            SourceE.FieldB)
        WHEN 'Customer-Ln7v' LIKE 'Account-u9Cx' THEN
          IF(
            STARTS_WITH('Address-jEqo','Code-8IP3'), SourceD.FieldC,
            'Account-33S3')
        WHEN 'Country-3KB1' NOT LIKE DerivedTable3.Account4sxv THEN
          IF(STARTS_WITH('Amt-cY1z','Account-pnTG'),'Amt-94jz','Account-wK0I')
        ELSE
          IF(
            DerivedTable3.FeeF6P4 NOT LIKE 'Account-HXI9', SourceA.FieldB,
            DerivedTable3.FeeCUde)
      END
        AS CodeA3oV,
      CASE
        WHEN ENDS_WITH(SourceB.FieldJ,'Fee-2IjK') THEN
          IF('Country-aagS' <> 'Address-UK5M','Fee-WLpn',Table28.CustomerD3nz)
        WHEN 'Code-DK6o' != 'Country-ZS8u' THEN
          IF(
            ENDS_WITH('Address-RdED','Address-Jajp'), SourceE.FieldA,
            SourceA.FieldJ)
        WHEN DerivedTable5.Customera0qh NOT LIKE DerivedTable3.FeeCUde THEN
          IF(
            ENDS_WITH(DerivedTable5.CCY76AX,'CCY-lzuh'), 'Code-iLg5',
            DerivedTable5.AddressV4AX)
        ELSE
          CASE
            WHEN STARTS_WITH(SourceE.FieldC,'Customer-izQC') THEN
              DerivedTable4.FeexvTE
            WHEN DerivedTable1.AccountqcVc = DerivedTable4.CodeXHEv THEN
              'Customer-Ldyj'
            WHEN 'Address-SSOr' <> 'Code-hehA' THEN
              'Account-Kr9X'
            WHEN ENDS_WITH(SourceE.FieldI,DerivedTable1.CCYXSkS) THEN
              SourceA.FieldB
            ELSE
              SourceB.FieldH
          END
      END
        AS FeeWsGO,
      IF(
        'Address-dgNP' = SourceC.FieldB,
        IF(
          STARTS_WITH('Country-qRdI','Customer-0PN6'), DerivedTable3.FeeD0Po,
          'Code-6V1v'), IF(
                          ENDS_WITH('CCY-2Zvv',DerivedTable4.CustomerCDJF),
                          Table28.AccountAOUE, 'CCY-u3XH'))
        AS Codeq664,
      IF(
        ENDS_WITH(DerivedTable3.CodeUeLq,DerivedTable3.AccountViga),
        CASE
          WHEN 'Country-3QHf' NOT LIKE Table27.CustomersVEq THEN
            'Address-QMoK'
          WHEN 'Address-jBW2' != 'Country-8ENq' THEN
            'CCY-oYW7'
          WHEN Table28.AccountAOUE <> SourceA.FieldJ THEN
            'Account-S6cB'
          ELSE
            'Country-S18x'
        END,
        CASE
          WHEN STARTS_WITH(SourceD.FieldH,Table28.Amt5uao) THEN
            SourceE.FieldC
          WHEN ENDS_WITH(DerivedTable3.Code3Xip,DerivedTable4.FeezLpC) THEN
            'Code-JIbA'
          WHEN ENDS_WITH(Table28.AccountAOUE,'Code-n5qY') THEN
            'Address-5gRT'
          WHEN SourceB.FieldJ NOT LIKE Table27.FeeelOV THEN
            SourceE.FieldC
          ELSE
            Table27.FeeelOV
        END)
        AS AccountCCbM,
      IF(
        STARTS_WITH(SourceB.FieldH,SourceC.FieldB),
        CASE
          WHEN ENDS_WITH(DerivedTable4.CCYY6No,'Account-ViRQ') THEN
            DerivedTable4.CodeXHEv
          WHEN 'Fee-R8Ew' NOT LIKE DerivedTable5.CCYQK2Q THEN
            'Amt-60WA'
          WHEN SourceD.FieldD NOT LIKE 'Customer-ayxZ' THEN
            'Address-gRh0'
          WHEN ENDS_WITH(Table27.Account4KpN,DerivedTable5.CCYQK2Q) THEN
            'Code-FTK0'
          WHEN ENDS_WITH(DerivedTable1.AddressoeLk,SourceD.FieldJ) THEN
            'Customer-3l1n'
          ELSE
            'Code-ftb4'
        END, CASE
               WHEN DerivedTable1.Addressrgjd LIKE SourceA.FieldJ THEN
                 'Address-fPh7'
               WHEN STARTS_WITH('CCY-s5qu',DerivedTable4.AccountMw3l) THEN
                 'Customer-LUqU'
               ELSE
                 'Code-13rd'
             END)
        AS AmtjnZC,
      IF(
        ENDS_WITH('Amt-H1UZ','Address-cVek'),
        CASE
          WHEN ENDS_WITH('Country-OcHK',DerivedTable3.Feeg8kl) THEN
            'Country-ZD06'
          WHEN SourceB.FieldG NOT LIKE 'Customer-jrAP' THEN
            Table27.Address9INh
          WHEN ENDS_WITH('Address-1VhY','Address-IIgo') THEN
            Table28.CCYGKA8
          WHEN STARTS_WITH('Customer-xFox',Table27.FeeelOV) THEN
            'Country-Zggq'
          ELSE
            'Fee-tbeM'
        END, IF(
               STARTS_WITH('Code-MvUs',Table28.AddresssIgw),
               DerivedTable3.FeepQGh, 'Address-EY3H'))
        AS Customer7jcO,
      IF(
        DerivedTable3.CustomerLP0M NOT LIKE SourceA.FieldI,
        CASE
          WHEN ENDS_WITH('Amt-MNjd','CCY-c5B0') THEN
            'CCY-e6rl'
          WHEN SourceA.FieldC LIKE Table28.Accountg5SY THEN
            Table28.CustomerD3nz
          WHEN SourceE.FieldG <> 'Customer-au9T' THEN
            DerivedTable4.FeesrBh
          ELSE
            SourceD.FieldG
        END, IF(
               DerivedTable1.AccountKwSP = 'Address-l1U4',
               DerivedTable3.Feeg8kl, SourceD.FieldC))
        AS AddressByZg,
      IF(
        STARTS_WITH(DerivedTable5.AddressV4AX,Table27.Code8uGc),
        IF(
          DerivedTable5.Customera0qh LIKE SourceD.FieldE, SourceD.FieldI,
          'Account-EKai'),
        CASE
          WHEN 'Address-vo0b' <> SourceB.FieldH THEN
            SourceE.FieldI
          WHEN 'Fee-RRpk' = 'Customer-gpTy' THEN
            SourceC.FieldI
          WHEN SourceE.FieldA = DerivedTable4.CodeJM19 THEN
            SourceC.FieldH
          WHEN DerivedTable3.FeeT7G9 LIKE SourceE.FieldG THEN
            'Code-g6Dk'
          WHEN 'CCY-oQaS' != SourceA.FieldD THEN
            DerivedTable5.FeeogF5
          ELSE
            'Account-0w3H'
        END)
        AS FeeaozA,
      IF(
        STARTS_WITH(DerivedTable3.CountryoFFA,'Amt-9EiX'),
        CASE
          WHEN STARTS_WITH('Address-oq9e','Code-6pAT') THEN
            'Customer-lCoP'
          WHEN ENDS_WITH('Code-0tOP','Fee-6g6D') THEN
            'CCY-cHqq'
          ELSE
            'Fee-o6aZ'
        END, IF(
               SourceA.FieldJ NOT LIKE 'Customer-Pytq', SourceC.FieldJ,
               'Address-XXo4'))
        AS AccountziEl,
      IF(
        ENDS_WITH('Amt-nKbw',SourceE.FieldA),
        IF(
          ENDS_WITH('CCY-jETs',SourceE.FieldF), DerivedTable4.AccountMw3l,
          'CCY-9zl9'), IF(
                         STARTS_WITH('Amt-F9mU',Table27.CodeDoG3),
                         SourceD.FieldC, DerivedTable4.AddressB6KJ))
        AS AccountDwMA,
      IF(
        STARTS_WITH('Customer-IzLX',SourceC.FieldA),
        IF(
          SourceB.FieldB != Table27.Accounttxhn, SourceA.FieldG,
          Table27.Fee4lfH), IF(
                              STARTS_WITH('Customer-WOID','Fee-7hdV'),
                              SourceE.FieldE, 'Account-2sZl'))
        AS Amt47sI,
      IF(
        ENDS_WITH(DerivedTable3.FeeD0Po,SourceB.FieldA),
        IF('CCY-gavj' NOT LIKE 'Address-WnVR','Customer-nT8n',Table28.CodeQ1hZ),
        IF(
          'Account-TirW' NOT LIKE DerivedTable5.CodePGRC, 'Fee-te30',
          'Fee-NlQB'))
        AS CountryH9vV,
      IF(
        STARTS_WITH(SourceA.FieldE,'Fee-trpe'),
        IF(
          ENDS_WITH(DerivedTable4.Account1SJF,'Address-zSaP'), 'Account-hcAr',
          Table28.Account45kz),
        IF(
          'Customer-9gYF' LIKE 'Code-JUuD', Table28.AccountAOUE,
          DerivedTable3.Code3Xip))
        AS Address3AEa,
      IF(
        Table28.Amt5uao = DerivedTable5.AddressV4AX,
        IF(DerivedTable5.CCY76AX NOT LIKE 'Code-tzrN','Code-hf6y','CCY-Ydeh'),
        IF(
          STARTS_WITH(Table27.CodeDoG3,SourceB.FieldH), DerivedTable3.FeeF6P4,
          'Account-I0EI'))
        AS FeeZzDd,
      IF(
        STARTS_WITH('CCY-mIll',SourceD.FieldG),
        IF(SourceB.FieldF != 'Customer-U3It',SourceE.FieldI,'Code-Nu5J'),
        CASE
          WHEN ENDS_WITH(Table27.Code8uGc,Table28.AccountG3dN) THEN
            Table28.Accountg5SY
          WHEN DerivedTable5.CountrynpDj NOT LIKE 'Account-dCIL' THEN
            'Address-Q14t'
          WHEN 'Account-WVAC' NOT LIKE 'Fee-iEVZ' THEN
            'CCY-BGZG'
          WHEN 'Account-79kP' <> SourceA.FieldB THEN
            'Account-fSkh'
          ELSE
            DerivedTable3.CustomerLP0M
        END)
        AS Code7k6M
    FROM
      Table28 AS Table28
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH('Code-dUBu','Address-3Gmn') OR
         STARTS_WITH('Code-d5SY',SourceA.FieldD) OR
         'Code-TQ2L' = 'Account-w7oO' OR ENDS_WITH('Amt-KS1d',SourceA.FieldG))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Country-rors',Table28.CCYZz2i) AND
         ENDS_WITH('CCY-Tuz0','Code-DeG1') OR
         STARTS_WITH('Code-itFy','CCY-0VGC') AND
         'Account-h5Qj' = SourceA.FieldD)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        ('Code-BA7y' != SourceA.FieldA AND 'Amt-OUOi' <> 'Account-Vqhe' AND
         SourceA.FieldI NOT LIKE SourceC.FieldA AND
         'Address-ylft' NOT LIKE 'Address-eKPv')
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        ('Fee-0Jor' LIKE SourceC.FieldF OR 'Amt-WD8t' != 'Amt-yO2x' OR
         STARTS_WITH(SourceB.FieldI,'Country-AoZc') AND
         Table28.FeeRfkh = 'Fee-Fkxz')
      INNER JOIN testdataset.SourceE AS SourceE ON
        (SourceC.FieldC = 'CCY-rz14' AND
         STARTS_WITH(SourceA.FieldI,'Code-Qict') OR
         SourceC.FieldD <> 'CCY-tjRD' OR Table28.Amt5uao != SourceB.FieldD)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('Amt-66jT',SourceB.FieldA) OR
         SourceC.FieldG != 'Amt-n9vo' AND
         'Account-V2wY' NOT LIKE SourceA.FieldG AND
         ENDS_WITH(Table28.CCYGKA8,SourceD.FieldB))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH('Country-fUBj','Code-kaZw') OR
         ENDS_WITH('CCY-uI9v',Table28.Accountg5SY) OR
         ENDS_WITH(DerivedTable3.FeeqkjX,Table28.CodeQ1hZ) OR
         'Code-I0MX' <> Table28.CustomerD3nz)
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        ('Fee-GG9C' LIKE DerivedTable3.FeeD0Po OR
         Table28.AddressJUrf LIKE 'Customer-WiWQ' AND
         STARTS_WITH(Table28.Accountg5SY,SourceD.FieldF) OR
         SourceA.FieldB != SourceA.FieldA)
      INNER JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        ('CCY-6eib' LIKE Table28.Amt5uao AND
         STARTS_WITH('Address-h8KW',SourceD.FieldC) AND
         STARTS_WITH('Fee-2R8W',DerivedTable4.AddressB6KJ) AND
         'Account-v6DR' LIKE 'Amt-zXbv')
      RIGHT JOIN Table27 AS Table27 ON
        (SourceD.FieldE = 'Country-PW0t' OR
         STARTS_WITH(SourceA.FieldA,DerivedTable4.CodeJM19) OR
         STARTS_WITH(SourceA.FieldG,'Address-mg52') OR
         STARTS_WITH(DerivedTable4.CodeJM19,Table27.CustomersVEq))
    WHERE
      STARTS_WITH(Table28.CustomerTQGK,DerivedTable1.AddressoeLk) OR
      DerivedTable1.AccountfEJK NOT LIKE 'Code-Eh9i' AND
      ENDS_WITH(SourceA.FieldA,'Amt-MbaH') AND
      ENDS_WITH(SourceA.FieldC,'Customer-M5bW') OR
      SourceE.FieldC NOT LIKE 'Address-4O2J' OR
      ENDS_WITH('Country-8mCS',DerivedTable3.FeeT7G9) AND
      ENDS_WITH(DerivedTable5.AddressV4AX,'Customer-bVKM') AND
      ENDS_WITH(SourceD.FieldH,SourceE.FieldB)
  ),
  Table30 AS (
    SELECT
      IF(
        'Account-o2nz' <> 'Amt-JhiR',
        IF(
          STARTS_WITH(Table26.CountrysK1v,SourceB.FieldF), 'CCY-xZnu',
          'Customer-OgUq'),
        IF(
          STARTS_WITH(DerivedTable3.FeepQGh,'Customer-5KHE'), 'Fee-BHpQ',
          'Account-WevM'))
        AS CustomerwXPa,
      IF(
        STARTS_WITH('Country-uMBJ',DerivedTable1.CountryV1io),
        IF('CCY-FvpJ' <> Table26.AddressCO7c,'Code-6ce4','Country-KZ10'),
        IF(
          ENDS_WITH('Country-xVeR','Customer-QeSu'), DerivedTable4.Country8VtM,
          'Amt-6Whr'))
        AS AmtLLZw,
      IF(
        Table29.AccountCCbM != Table26.CustomerYZsr,
        IF(
          STARTS_WITH('Customer-F2HQ','Amt-bu2d'), Table29.Customer7jcO,
          SourceB.FieldJ),
        CASE
          WHEN SourceE.FieldB LIKE 'Customer-BcbO' THEN
            'Account-QNCX'
          WHEN Table26.FeekVuZ = 'Account-7L06' THEN
            'Customer-BwoJ'
          WHEN ENDS_WITH(Table29.Address3AEa,'Code-Rx8y') THEN
            'Customer-TZ30'
          ELSE
            'Code-8FI3'
        END)
        AS FeeVTxE,
      CASE
        WHEN Table27.CodecTJM != 'Fee-dicH' THEN
          IF(
            'Fee-TKcG' LIKE DerivedTable3.Code3Xip, SourceD.FieldJ,
            SourceE.FieldE)
        WHEN Table27.CodezbdN = SourceB.FieldE THEN
          IF(
            ENDS_WITH(Table27.FeeelOV,'Account-omxv'), Table29.Address3AEa,
            'Customer-vfVp')
        WHEN 'CCY-raUU' NOT LIKE 'Customer-pJVB' THEN
          IF(ENDS_WITH('CCY-fHra','Fee-ZwK9'),'Amt-bzj0',SourceB.FieldE)
        ELSE
          IF(
            ENDS_WITH('Amt-xob5',DerivedTable4.AccounteutQ),
            DerivedTable3.AccountViga, 'Fee-JYqo')
      END
        AS FeeSq5U,
      IF(
        DerivedTable3.CountrygMB4 NOT LIKE 'Address-ZSKI',
        IF(STARTS_WITH('CCY-fwqv','CCY-U7WY'),SourceB.FieldH,SourceD.FieldI),
        CASE
          WHEN ENDS_WITH(Table26.CountryavC2,Table28.Accountg5SY) THEN
            SourceB.FieldF
          WHEN Table26.AccountCVze NOT LIKE DerivedTable3.CountryoFFA THEN
            'Account-0rU3'
          WHEN STARTS_WITH('Account-ZBwe','CCY-Cizf') THEN
            SourceE.FieldF
          WHEN Table26.Feevrju LIKE 'Amt-MbD3' THEN
            'Address-gKXM'
          ELSE
            Table26.CountrysK1v
        END)
        AS CountryD671,
      CASE
        WHEN ENDS_WITH(Table28.AddressJUrf,SourceD.FieldG) THEN
          IF(
            'Country-dvmC' NOT LIKE 'Customer-IyWq', 'Fee-Q6rs',
            DerivedTable5.Feepmor)
        WHEN SourceD.FieldG <> Table26.Countrym03Z THEN
          IF(
            ENDS_WITH(DerivedTable1.AccountKwSP,'Address-xr5Q'), 'Code-KWbI',
            DerivedTable1.CCYXSkS)
        WHEN SourceD.FieldC <> 'Address-baS0' THEN
          IF(
            'Customer-qrDz' <> 'Country-vasx', 'Address-sSnY',
            Table26.CountrysK1v)
        WHEN DerivedTable3.CCYO742 != Table29.FeeZzDd THEN
          IF(Table27.Fee4lfH LIKE 'Amt-sr4w','Code-wap8','Address-t6h3')
        WHEN DerivedTable1.CustomerKrwR LIKE Table26.CustomerRbsW THEN
          IF(
            ENDS_WITH('Amt-fkjS',Table28.CCYZz2i), DerivedTable4.CodeXHEv,
            'Fee-fcQN')
        ELSE
          IF(
            ENDS_WITH(Table29.FeeZzDd,DerivedTable5.CCY76AX), 'Account-Bkw7',
            'Amt-v0wu')
      END
        AS CodeeU6s,
      CASE
        WHEN STARTS_WITH(DerivedTable4.Country8VtM,'Customer-db4h') THEN
          IF(
            STARTS_WITH(DerivedTable5.CCYQK2Q,SourceB.FieldC), 'Address-vq0B',
            'Country-cGzw')
        WHEN 'Code-gjU7' != 'Account-58YM' THEN
          CASE
            WHEN ENDS_WITH('Code-giAF','Country-dVEF') THEN
              DerivedTable3.FeeF6P4
            WHEN STARTS_WITH('Code-w6sq',Table29.FeeZzDd) THEN
              DerivedTable4.CodeJM19
            ELSE
              'Amt-2EDK'
          END
        WHEN ENDS_WITH(Table27.CustomersVEq,Table28.AccountG3dN) THEN
          CASE
            WHEN ENDS_WITH(DerivedTable5.Customera0qh,'Country-TIoM') THEN
              SourceD.FieldH
            WHEN STARTS_WITH(SourceD.FieldG,SourceB.FieldJ) THEN
              DerivedTable1.AccountKwSP
            WHEN SourceD.FieldJ NOT LIKE 'Country-GIpM' THEN
              'Customer-RGPe'
            WHEN Table29.AccountziEl != 'Customer-SJc6' THEN
              Table26.AddressCO7c
            WHEN SourceE.FieldC != 'Country-qeTp' THEN
              'Fee-OMlX'
            ELSE
              Table26.AccountCVze
          END
        WHEN DerivedTable4.AddressWvnP = 'CCY-uNcS' THEN
          IF(STARTS_WITH('Country-2KCh',SourceD.FieldF),'Fee-3jZU','Fee-Zmm2')
        WHEN DerivedTable3.FeeCUde NOT LIKE 'Account-yJ5g' THEN
          IF(
            ENDS_WITH(DerivedTable1.AccountqcVc,'CCY-yVzB'),
            Table27.Accounttxhn, 'Fee-0Dtf')
        ELSE
          IF(
            SourceD.FieldC NOT LIKE Table29.CustomerSHFG, DerivedTable5.FeeogF5,
            DerivedTable3.FeeCUde)
      END
        AS AmtevSg,
      IF(
        ENDS_WITH('Country-CsPD','Customer-G4Df'),
        CASE
          WHEN Table27.Account4KpN LIKE DerivedTable3.FeeCUde THEN
            Table29.AccountDwMA
          WHEN DerivedTable3.CustomereBLI <> DerivedTable3.Account4sxv THEN
            'Customer-qtlm'
          WHEN 'Account-46fo' NOT LIKE SourceD.FieldE THEN
            'Amt-NzAL'
          WHEN Table27.Address9INh <> 'Account-EAa2' THEN
            Table26.AmthWaa
          ELSE
            'CCY-nBto'
        END, IF('CCY-NEZR' != Table27.FeeelOV,'Code-pljp',Table29.AccountDwMA))
        AS CCYRQJD,
      IF(
        STARTS_WITH('Code-xEl2','Country-7u9B'),
        CASE
          WHEN ENDS_WITH(Table29.AccountXney,Table29.FeeaozA) THEN
            DerivedTable5.Customera0qh
          WHEN ENDS_WITH(SourceD.FieldD,'Amt-YPCZ') THEN
            'Amt-nNKK'
          ELSE
            DerivedTable5.Feepmor
        END, IF(
               'Country-YZIm' != DerivedTable1.CountryV1io, SourceB.FieldF,
               Table28.CCYGKA8))
        AS AmtxJAE,
      CASE
        WHEN STARTS_WITH('Code-YKrM','Customer-iMgl') THEN
          IF(
            'Customer-gUDc' LIKE DerivedTable1.AccountKwSP,
            Table26.Customer7qUf, SourceD.FieldE)
        WHEN 'Fee-hacy' = DerivedTable5.CountrynpDj THEN
          IF('Address-CWtP' <> 'Customer-7qvf','Amt-7zcr','Fee-dFGV')
        WHEN ENDS_WITH(DerivedTable3.FeepQGh,DerivedTable4.Country8VtM) THEN
          IF(
            ENDS_WITH('Account-iqZ2',Table29.CustomerSHFG), 'Account-rAh7',
            'Address-HAl2')
        WHEN STARTS_WITH('Amt-niz4',Table29.Codeq664) THEN
          CASE
            WHEN 'Fee-buib' LIKE Table26.CustomerXdHw THEN
              DerivedTable4.FeexvTE
            WHEN 'CCY-rB40' <> DerivedTable3.CountryoFFA THEN
              SourceB.FieldG
            WHEN 'Customer-48k2' != 'Customer-HOTf' THEN
              Table26.FeekVuZ
            ELSE
              SourceB.FieldF
          END
        ELSE
          IF(
            ENDS_WITH(DerivedTable3.CountryoFFA,'Account-Gdbm'),
            Table26.CountryI5Fk, DerivedTable1.AmtR86V)
      END
        AS CCYgnzF,
      CASE
        WHEN DerivedTable3.CodeVHWz <> DerivedTable4.Country8VtM THEN
          IF(
            Table26.FeeDLkv NOT LIKE 'Fee-2FsM', Table29.CustomerSHFG,
            DerivedTable4.Addressm87D)
        WHEN STARTS_WITH(Table28.Amt5uao,'Customer-b2bD') THEN
          IF(
            ENDS_WITH('Country-PewK',DerivedTable3.CountrygMB4), 'Address-OBdD',
            'Account-OVbw')
        ELSE
          IF(
            STARTS_WITH(Table26.AddressCO7c,Table27.FeeelOV), Table26.FeekVuZ,
            DerivedTable5.Customera0qh)
      END
        AS CustomerSKzV,
      CASE
        WHEN ENDS_WITH(DerivedTable3.CodeUeLq,SourceD.FieldD) THEN
          IF(ENDS_WITH('Fee-54br','Country-BRMw'),'Address-sJ89','Code-P4lJ')
        WHEN ENDS_WITH('CCY-8PYl','Fee-5q4i') THEN
          IF(
            STARTS_WITH(DerivedTable3.CountrygPvi,SourceB.FieldH),
            Table29.AccountXney, SourceB.FieldG)
        ELSE
          CASE
            WHEN
              ENDS_WITH('Country-M4rL',Table28.CustomerD3nz)
            THEN
              Table26.CustomerXdHw
            WHEN
              ENDS_WITH(Table28.AccountG3dN,SourceD.FieldD)
            THEN
              'Amt-figt'
            WHEN
              STARTS_WITH(Table29.Customer7jcO,DerivedTable4.AddressB6KJ)
            THEN
              DerivedTable5.Code7INp
            ELSE
              DerivedTable5.Customera0qh
          END
      END
        AS FeeNMYP
    FROM
      testdataset.DerivedTable1 AS DerivedTable1
      INNER JOIN testdataset.SourceB AS SourceB ON
        ('Code-746Z' != SourceB.FieldD AND 'Customer-0iTz' = 'Amt-YEXH' AND
         ENDS_WITH('Address-uBtz','Amt-Wogs') AND
         SourceB.FieldH != DerivedTable1.CCYXSkS)
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (SourceB.FieldC NOT LIKE 'Account-hFBj' OR
         SourceB.FieldD = DerivedTable1.AccountmNQo OR
         STARTS_WITH(SourceD.FieldF,SourceD.FieldB) AND
         STARTS_WITH('Address-buuF',DerivedTable1.AmtR86V))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (SourceB.FieldF = 'Address-00uI' AND
         'CCY-Q7vS' <> DerivedTable1.AmtzEYq OR
         STARTS_WITH(DerivedTable1.CustomerWpfX,DerivedTable1.AddressoeLk) AND
         ENDS_WITH('Country-H23Y','Country-ru7V'))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (DerivedTable3.CodeVHWz NOT LIKE 'CCY-lRxa' OR
         STARTS_WITH(DerivedTable1.AccountmNQo,'Amt-lXIQ') AND
         'Country-W18A' NOT LIKE SourceE.FieldB AND
         ENDS_WITH(SourceE.FieldC,'Customer-CKCF'))
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        ('Customer-AdN7' LIKE 'Customer-2Li5' OR
         STARTS_WITH('Fee-nbBl',DerivedTable1.CCYXSkS) OR
         DerivedTable1.AmtR86V = 'Fee-I93Z' AND
         'Customer-5Wrf' LIKE DerivedTable1.CustomerWpfX)
      LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (DerivedTable5.CCYVD4h != SourceD.FieldG AND
         'Account-ePJb' LIKE DerivedTable4.AccounteutQ OR
         ENDS_WITH(DerivedTable1.AccountmNQo,DerivedTable5.Feepmor) OR
         STARTS_WITH('Country-brPo','Amt-mMJJ'))
      LEFT JOIN Table26 AS Table26 ON
        (STARTS_WITH(SourceD.FieldA,SourceB.FieldI) AND
         'Country-8W1t' != DerivedTable3.CountryoFFA AND
         SourceE.FieldG NOT LIKE 'Account-Tmgm' AND
         'Address-s0xH' != DerivedTable5.CCY76AX)
      INNER JOIN Table27 AS Table27 ON
        (Table26.AddressR2r0 != Table26.AccounthcEB AND
         STARTS_WITH('Amt-ylo5',Table27.AccountrVGO) OR
         ENDS_WITH(Table26.Countrym03Z,Table26.Feevrju) AND
         ENDS_WITH(SourceB.FieldE,DerivedTable4.Account1SJF))
      LEFT JOIN Table28 AS Table28 ON
        (DerivedTable1.AddressoeLk <> DerivedTable3.AccountViga AND
         'Fee-GAgk' = 'Fee-gw1u' AND
         'Country-Jf4T' NOT LIKE 'Customer-YnMz' AND
         ENDS_WITH('Code-M3zZ',SourceE.FieldH))
      LEFT JOIN Table29 AS Table29 ON
        (STARTS_WITH('Code-dj1Q','Fee-kzqy') AND
         DerivedTable5.Customera0qh NOT LIKE 'Country-KsG9' AND
         DerivedTable3.CCYO742 NOT LIKE 'Address-etJx' OR
         STARTS_WITH('Code-aqoG',SourceB.FieldJ))
    WHERE
      STARTS_WITH('CCY-vtGn',Table29.AmtjnZC) AND
      STARTS_WITH('Code-5OUZ',DerivedTable3.FeepQGh) OR
      DerivedTable4.Addressm87D != Table29.Codeq664 OR
      STARTS_WITH('Account-v1O4','Amt-mss4') OR
      'Amt-MLm5' = Table26.CustomerXdHw AND
      STARTS_WITH(DerivedTable4.CodeJM19,'Amt-yrJ4') OR
      STARTS_WITH(Table26.CountryI5Fk,DerivedTable3.FeeF6P4) AND
      'CCY-0wDJ' != Table27.AccountrVGO
  )
SELECT
  IF(
    'Address-KUMf' = DerivedTable5.FeeogF5,
    IF('Customer-JbsO' != SourceE.FieldD,'Account-cSTv','Country-ncoS'),
    IF(
      ENDS_WITH(DerivedTable5.CCY76AX,'Fee-PWez'), Table30.AmtxJAE,
      'Country-HZ8A'))
    AS AddressPIbO,
  IF(
    STARTS_WITH(Table29.AccountziEl,'Account-Sic7'),
    IF(SourceD.FieldI <> 'Code-RWzD','CCY-CrEw',SourceA.FieldJ),
    CASE
      WHEN STARTS_WITH('Country-xGVr','Country-RldV') THEN
        SourceC.FieldE
      WHEN Table26.Feevrju != 'Customer-OZwX' THEN
        SourceE.FieldE
      WHEN 'Country-fEea' = Table26.CodeNAHw THEN
        'Code-D3nJ'
      ELSE
        'Customer-pFfC'
    END)
    AS Address54VO,
  IF(
    STARTS_WITH('Code-ye6w','Customer-GFNc'),
    CASE
      WHEN Table30.CountryD671 = Table27.CodecTJM THEN
        'Fee-IGgo'
      WHEN 'Fee-Iwyi' <> SourceC.FieldD THEN
        'Address-ALkU'
      WHEN 'Country-tRCs' LIKE DerivedTable1.Codeuhr9 THEN
        Table29.AccountDwMA
      WHEN ENDS_WITH('CCY-l1Ar','Account-BGqe') THEN
        DerivedTable5.FeeogF5
      ELSE
        SourceB.FieldB
    END, IF('CCY-QLVX' = 'CCY-zQuN','Code-ULZW','Customer-awYN'))
    AS FeeEU6a,
  CASE
    WHEN Table27.Address9INh = 'CCY-RJew' THEN
      IF(STARTS_WITH('Code-DpzT','Customer-wkiA'),'Address-IRL6','Country-hzoH')
    WHEN SourceB.FieldD != 'CCY-DlXI' THEN
      IF(ENDS_WITH('Country-0aIe','Account-K7bG'),'Country-1ocA','CCY-K7qB')
    ELSE
      IF(
        'Address-P5fq' <> DerivedTable5.Feepmor, DerivedTable5.CCYQK2Q,
        Table26.AmthWaa)
  END
    AS AddressgQNB,
  IF(
    STARTS_WITH('Address-U7X6',Table30.CustomerSKzV),
    IF(Table26.FeeDLkv <> Table27.CustomersVEq,'CCY-E1sK','Account-7qVv'),
    IF(SourceD.FieldE LIKE SourceD.FieldH,'Account-n2w5',Table27.CodezbdN))
    AS CCYp8wn,
  CASE
    WHEN 'Address-cYb1' <> 'Address-Mgb6' THEN
      IF(ENDS_WITH(SourceE.FieldC,'Address-XBox'),'Code-LMlk',SourceE.FieldJ)
    WHEN 'CCY-OX2N' NOT LIKE Table26.Customer7qUf THEN
      CASE
        WHEN Table27.Amtuba6 <> 'Customer-rNwY' THEN
          SourceA.FieldC
        WHEN STARTS_WITH('Country-3Bn1','Account-lc9N') THEN
          Table29.AccountXney
        WHEN DerivedTable5.Customera0qh = 'Code-mAFc' THEN
          SourceB.FieldD
        WHEN STARTS_WITH('Amt-1Aax',DerivedTable5.Code7INp) THEN
          DerivedTable3.CCYO742
        ELSE
          'CCY-E91F'
      END
    WHEN SourceD.FieldB = DerivedTable3.FeeT7G9 THEN
      IF(
        Table27.CodecTJM != 'Address-kN7n', SourceC.FieldH,
        DerivedTable3.FeeD0Po)
    WHEN ENDS_WITH('Country-QTn2','Code-ZlLh') THEN
      IF(
        ENDS_WITH('Country-CuTv',DerivedTable3.Feeg8kl), 'Account-193C',
        'Account-R111')
    WHEN STARTS_WITH('Account-oFa4',SourceE.FieldI) THEN
      CASE
        WHEN ENDS_WITH(DerivedTable3.FeeqkjX,'CCY-gqtf') THEN
          SourceE.FieldE
        WHEN 'Address-iVuO' != Table29.CountryH9vV THEN
          'Code-lDDO'
        ELSE
          'Account-KQ31'
      END
    ELSE
      IF(
        ENDS_WITH(Table26.CustomerRbsW,'Code-UNsw'), 'Country-w6h6',
        'Country-QMzb')
  END
    AS AddressYFBr,
  IF(
    ENDS_WITH(DerivedTable3.FeeD0Po,'Code-UoEb'),
    IF(
      'Amt-lOTA' LIKE DerivedTable3.Codeaqvh, SourceE.FieldH,
      DerivedTable3.FeeF6P4),
    CASE
      WHEN 'Address-KXe5' <> 'Address-EpHj' THEN
        DerivedTable1.CountryV1io
      WHEN ENDS_WITH(SourceC.FieldG,'Address-hIk4') THEN
        Table29.CountryH9vV
      WHEN STARTS_WITH('Fee-IY4x',Table26.AccountJG1r) THEN
        SourceB.FieldA
      WHEN ENDS_WITH(Table26.Countrym03Z,'Amt-EBKj') THEN
        'Address-U6zX'
      ELSE
        'Customer-Tzcp'
    END)
    AS AddressWejH,
  IF(
    'Account-2LFX' != DerivedTable3.CountrygMB4,
    CASE
      WHEN STARTS_WITH(Table29.Codeq664,'Address-yJHd') THEN
        'Address-ZTuO'
      WHEN STARTS_WITH('Country-JAVV','Address-Y2ZE') THEN
        'Address-d7au'
      WHEN Table26.CountryavC2 = DerivedTable1.Addressrgjd THEN
        DerivedTable1.AccountmNQo
      WHEN STARTS_WITH('Customer-Of4X','Address-lz1K') THEN
        Table29.Customer7jcO
      WHEN 'Account-NIum' NOT LIKE 'Amt-vmU5' THEN
        Table26.CustomerYZsr
      ELSE
        'Account-eOHW'
    END, IF(SourceE.FieldD NOT LIKE 'Customer-2Jne','Fee-bhZm','Address-EvxC'))
    AS CodeY4Yb,
  IF(
    ENDS_WITH(Table26.CountryI5Fk,Table29.AccountXney),
    IF(Table29.FeeavaS LIKE 'Account-s92j','Fee-C1MD',DerivedTable3.Feeg8kl),
    IF(
      ENDS_WITH('CCY-Nqe7',Table30.CustomerSKzV), Table29.FeeWsGO,
      Table29.Code7k6M))
    AS Addressp4Y0,
  IF(
    DerivedTable3.FeeF6P4 <> 'CCY-ZqQ0',
    IF(Table29.Code7k6M LIKE SourceD.FieldG,'Country-v8Wg','Amt-MioN'),
    IF(
      STARTS_WITH('Customer-H7Si','Code-Tyqn'), 'CCY-1EFO',
      DerivedTable5.CCYVD4h))
    AS CountryD7gw,
  IF(
    ENDS_WITH(Table30.FeeVTxE,SourceA.FieldD),
    IF(
      ENDS_WITH(Table26.AddressR2r0,SourceB.FieldF), SourceC.FieldB,
      SourceA.FieldJ),
    CASE
      WHEN SourceE.FieldD = DerivedTable5.Customera0qh THEN
        'Amt-5gTm'
      WHEN Table27.FeeelOV <> SourceC.FieldI THEN
        DerivedTable3.CustomerLP0M
      ELSE
        'Customer-cHE3'
    END)
    AS AddressD7PZ,
  IF(
    STARTS_WITH(Table26.CodeNAHw,'Customer-mbfH'),
    IF(
      STARTS_WITH(Table29.FeeZzDd,'Amt-irFh'), 'Customer-srFH',
      Table26.CustomerYZsr),
    IF('CCY-6CST' = Table29.CodeA3oV,'Code-hX8j',SourceB.FieldI))
    AS CustomerxLhn,
  CASE
    WHEN ENDS_WITH('Account-CbyT','Code-ER61') THEN
      IF(
        Table26.AccounthcEB != Table26.CountryI5Fk, Table29.AddressByZg,
        'Fee-MMf2')
    WHEN DerivedTable3.CCYO742 <> DerivedTable5.FeeogF5 THEN
      CASE
        WHEN STARTS_WITH('Customer-rZXu',Table30.AmtevSg) THEN
          'Customer-UHeG'
        WHEN 'Country-mvPW' LIKE DerivedTable3.CodeVHWz THEN
          'Amt-PgxA'
        WHEN ENDS_WITH('Amt-UANT',SourceC.FieldA) THEN
          SourceE.FieldJ
        WHEN 'CCY-gumU' NOT LIKE 'Amt-obBW' THEN
          SourceC.FieldJ
        ELSE
          SourceE.FieldJ
      END
    WHEN 'Fee-hW4I' NOT LIKE 'Customer-NYaa' THEN
      IF(
        ENDS_WITH('Customer-bwdy','Customer-lWnp'), 'Amt-DMgd',
        Table27.Account4KpN)
    WHEN 'Account-BQCk' <> DerivedTable3.FeeF6P4 THEN
      IF(
        DerivedTable5.CodePGRC LIKE SourceE.FieldJ, 'Customer-SekL',
        SourceA.FieldB)
    WHEN ENDS_WITH('Address-JvJ1',Table29.Customer7jcO) THEN
      IF(
        STARTS_WITH(Table29.Customer7jcO,Table30.AmtLLZw), 'Code-GSh3',
        'CCY-EXrY')
    ELSE
      IF(ENDS_WITH(Table29.Customerpb4F,'Code-mMYf'),'Country-4ed5','CCY-LqVl')
  END
    AS FeeFMCK,
  CASE
    WHEN 'Code-MeQx' NOT LIKE DerivedTable1.CustomerWpfX THEN
      IF(STARTS_WITH(SourceE.FieldF,'Account-BvqZ'),'Fee-ehOs',Table30.FeeSq5U)
    WHEN Table30.CustomerwXPa <> 'Address-A9Nz' THEN
      IF(Table27.Code8uGc != 'Fee-tQR6',SourceC.FieldI,'Address-XOiQ')
    WHEN DerivedTable1.AccountqcVc <> 'Country-kTps' THEN
      IF(
        ENDS_WITH(Table26.CountrysK1v,'Address-5eN8'), 'Customer-c7pD',
        'Fee-eg5s')
    ELSE
      IF(ENDS_WITH('Country-qufQ','Country-XZHF'),'Address-bUsa','Fee-wBiM')
  END
    AS CodebnT9,
  IF(
    'Fee-wY7N' = 'Customer-Uoys',
    IF('Country-3dKb' <> Table27.CodezbdN,'Fee-2ozh',Table29.CodeA3oV),
    IF(
      STARTS_WITH('CCY-QuVR','Country-uOE4'), DerivedTable3.CustomerLP0M,
      'Country-TREn'))
    AS CustomerCZg3,
  CASE
    WHEN Table26.CountrysK1v != Table26.Customer7qUf THEN
      IF(
        STARTS_WITH('Address-nAXN',Table29.AmtDrPD), SourceD.FieldB,
        'Country-qDo0')
    WHEN 'Code-bxm8' NOT LIKE SourceC.FieldF THEN
      IF(
        ENDS_WITH(DerivedTable1.CountryV1io,Table26.Feevrju),
        Table26.Countrym03Z, SourceB.FieldC)
    WHEN 'Fee-IIrP' = 'Code-4mwq' THEN
      IF(STARTS_WITH('Customer-q1nL',SourceC.FieldC),'Fee-e0SP','Address-N5QN')
    WHEN DerivedTable3.FeeF6P4 NOT LIKE Table26.FeeNiKJ THEN
      CASE
        WHEN ENDS_WITH(Table26.FeekVuZ,Table26.CountrysK1v) THEN
          Table29.FeeavaS
        WHEN STARTS_WITH(Table27.Code8uGc,'Address-3r1z') THEN
          'Customer-Ykvz'
        WHEN ENDS_WITH(DerivedTable3.Codeaqvh,Table27.CustomersVEq) THEN
          'Amt-J4Sw'
        WHEN STARTS_WITH(DerivedTable1.Codeuhr9,DerivedTable3.FeeF6P4) THEN
          'Account-xCOe'
        WHEN 'Fee-ySwF' <> 'Amt-7op5' THEN
          'Fee-Z0n2'
        ELSE
          'Code-iUVm'
      END
    ELSE
      IF(
        ENDS_WITH('Customer-a7Jt','Customer-cktm'), Table29.FeeZzDd,
        SourceB.FieldJ)
  END
    AS FeeN99q,
  CASE
    WHEN STARTS_WITH(DerivedTable3.FeeD0Po,Table26.CountryavC2) THEN
      IF(SourceB.FieldI <> 'Amt-LbBW',DerivedTable1.AccountmNQo,'Code-brdE')
    WHEN STARTS_WITH(DerivedTable1.AccountqcVc,'CCY-Ji2P') THEN
      CASE
        WHEN ENDS_WITH('Code-XFuQ','Address-TaMh') THEN
          Table29.AccountziEl
        WHEN 'Account-XMFl' LIKE DerivedTable5.Code7INp THEN
          'Country-ypSb'
        WHEN 'Customer-awXE' NOT LIKE Table30.AmtxJAE THEN
          Table27.CustomersVEq
        WHEN Table30.CustomerwXPa LIKE 'Code-wLGr' THEN
          'Address-15Y0'
        ELSE
          'Amt-FC4p'
      END
    WHEN STARTS_WITH('Customer-PXjR','Address-TjkM') THEN
      IF('CCY-D3sk' = SourceD.FieldI,'Fee-l9pA',Table29.FeeavaS)
    ELSE
      IF(ENDS_WITH(SourceC.FieldG,SourceD.FieldC),SourceE.FieldI,'Fee-J3sU')
  END
    AS AmtW2w8,
  IF(
    Table27.CodecTJM = 'Customer-mmrx',
    IF(STARTS_WITH('Amt-eLrw','Address-CxxT'),Table27.FeeelOV,'CCY-sBgP'),
    IF('Amt-LAii' LIKE 'Amt-lcrI',Table26.AddressR2r0,Table29.Codeq664))
    AS AmtCCH1,
  CASE
    WHEN ENDS_WITH('Fee-PpdW',DerivedTable3.CodeUeLq) THEN
      IF(
        DerivedTable1.AccountfEJK != Table29.AmtjnZC, SourceE.FieldB,
        Table30.AmtLLZw)
    WHEN 'Account-osqq' != 'Code-gD2y' THEN
      IF(
        STARTS_WITH('Code-GqXK','Fee-Blxa'), DerivedTable3.CustomerLP0M,
        SourceB.FieldG)
    WHEN ENDS_WITH(Table30.CustomerSKzV,'Customer-Xp8G') THEN
      IF(STARTS_WITH('Amt-HK77','Customer-6pzd'),'Fee-6PLK',SourceD.FieldA)
    WHEN STARTS_WITH(DerivedTable3.Feeg8kl,'Code-Xde1') THEN
      IF(STARTS_WITH('Fee-2OsM','Amt-9Ioe'),'Amt-wVVT','Account-phM4')
    ELSE
      IF(Table29.AccountXney LIKE 'Fee-PTiQ','Customer-2VQL','Account-VLHw')
  END
    AS AddressPpXC,
  IF(
    DerivedTable3.CodeVHWz LIKE 'Account-ta0s',
    IF('CCY-yagw' NOT LIKE SourceA.FieldB,Table29.AccountCCbM,'Code-V4gU'),
    IF(SourceB.FieldB != Table27.Address9INh,SourceB.FieldA,SourceC.FieldH))
    AS Fee8TJM
FROM
  testdataset.DerivedTable1 AS DerivedTable1
  INNER JOIN testdataset.SourceA AS SourceA ON
    (STARTS_WITH('Customer-J003','Customer-qIke') OR
     SourceA.FieldG LIKE 'Customer-5W1E' AND
     ENDS_WITH(DerivedTable1.AmtzEYq,DerivedTable1.AmtzEYq) OR
     ENDS_WITH('Country-5TT4','Country-E6Bp'))
  RIGHT JOIN testdataset.SourceB AS SourceB ON
    (STARTS_WITH(SourceB.FieldD,'Country-eCJx') OR
     STARTS_WITH(SourceA.FieldE,'Country-TZ5x') AND
     ENDS_WITH('Address-Xlwe',DerivedTable1.AccountqcVc) AND
     'Country-TO64' <> 'Address-7mWO')
  LEFT JOIN testdataset.SourceC AS SourceC ON
    (STARTS_WITH('Account-BdKU','Country-OxYD') OR
     'Amt-hJJB' != SourceB.FieldH OR
     DerivedTable1.AmtzEYq != 'Account-xqaC' AND
     ENDS_WITH(DerivedTable1.AccountfEJK,DerivedTable1.AccountKwSP))
  LEFT JOIN testdataset.SourceD AS SourceD ON
    (ENDS_WITH(SourceB.FieldF,'Code-iAkS') AND
     'Fee-xSVz' = DerivedTable1.CustomerWrlz AND
     STARTS_WITH(SourceB.FieldG,SourceD.FieldA) OR
     STARTS_WITH('Customer-84gg',SourceA.FieldG))
  LEFT JOIN testdataset.SourceE AS SourceE ON
    ('CCY-p9ag' <> 'Code-H0jE' OR 'Fee-EBBu' <> 'Code-iJPU' OR
     STARTS_WITH('Account-kEXI','Account-vmNS') AND
     'Code-8bHz' = SourceE.FieldJ)
  LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
    ('Customer-P3h1' <> 'Account-GDz4' AND
     STARTS_WITH(DerivedTable1.CustomerKrwR,'CCY-brOu') AND
     'Code-2S6P' NOT LIKE SourceD.FieldB AND 'Fee-m0Fo' <> 'Fee-Id5N')
  LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
    (STARTS_WITH('Address-pZrV','CCY-S0fa') OR
     STARTS_WITH(DerivedTable1.AccountmNQo,'Account-mwSf') AND
     'Account-LF13' <> DerivedTable3.FeeF6P4 OR
     STARTS_WITH(DerivedTable1.CCYXSkS,'Code-P2Vx'))
  LEFT JOIN Table26 AS Table26 ON
    (STARTS_WITH(SourceC.FieldJ,DerivedTable1.AccountmNQo) OR
     STARTS_WITH('Amt-Fb36',SourceB.FieldF) AND
     SourceE.FieldD NOT LIKE DerivedTable1.AmtzEYq AND
     ENDS_WITH(DerivedTable1.AmtzEYq,'Country-mMvG'))
  INNER JOIN Table27 AS Table27 ON
    (STARTS_WITH(SourceC.FieldH,'Account-BJgg') AND
     DerivedTable3.Code3Xip != Table26.FeeNiKJ AND
     'Fee-kWws' LIKE Table26.CodeNAHw OR
     STARTS_WITH('Country-jGx7','Customer-Oazu'))
  INNER JOIN Table29 AS Table29 ON
    (STARTS_WITH(Table29.FeeWsGO,'Fee-U9KV') AND
     STARTS_WITH('Amt-gIsJ','Code-AbOq') OR
     ENDS_WITH('Code-jy0y','Amt-sDho') AND 'Amt-nF2T' <> Table29.Address3AEa)
  RIGHT JOIN Table30 AS Table30 ON
    ('Country-FaiZ' LIKE SourceA.FieldH AND
     'Country-HSNS' NOT LIKE 'Country-hGnz' OR
     ENDS_WITH(SourceD.FieldD,'Amt-NuqC') OR
     SourceA.FieldG <> Table29.AccountziEl)
WHERE
  DerivedTable1.FeerBIS = DerivedTable3.CustomereBLI AND
  ENDS_WITH(SourceA.FieldB,'Customer-Cg2l') OR
  DerivedTable1.CCYXSkS <> 'Country-3buo' AND
  DerivedTable5.CountrynpDj LIKE DerivedTable3.CodeVHWz AND
  SourceA.FieldG = 'Code-1BhS' OR SourceC.FieldJ = Table29.FeeaozA AND
  ENDS_WITH('Customer-AAFX',DerivedTable1.AccountKwSP) AND
  'Account-TTaj' != SourceA.FieldB