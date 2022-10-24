CREATE OR REPLACE TABLE testdataset.DerivedTable9 AS
WITH
  Table41 AS (
    SELECT
      IF(
        DerivedTable2.Account61kv = DerivedTable8.Account3V3K,
        CASE
          WHEN STARTS_WITH(DerivedTable3.FeeT7G9,DerivedTable3.CountryoFFA) THEN
            DerivedTable2.AddresscM17
          WHEN SourceD.FieldC <> DerivedTable8.Amti3Vx THEN
            DerivedTable7.FeeLG30
          WHEN 'Fee-9wZm' NOT LIKE SourceD.FieldI THEN
            DerivedTable4.FeesrBh
          WHEN ENDS_WITH(SourceE.FieldJ,SourceC.FieldE) THEN
            DerivedTable4.Country5f1u
          ELSE
            DerivedTable7.Amt9aae
        END, IF(
               STARTS_WITH('Country-UEAc','Country-dmkz'), 'Account-wjhY',
               'Fee-2WXN'))
        AS AmtBAXR,
      IF(
        ENDS_WITH(SourceD.FieldE,DerivedTable8.AmtSXKP),
        IF(
          DerivedTable7.AddressgmSt <> SourceC.FieldD, 'Account-MDJB',
          DerivedTable7.CountrypfA6),
        IF('Account-q3Ot' != 'Amt-zEOp',DerivedTable5.CCYVD4h,'Code-nxcn'))
        AS Codel32i,
      IF(
        'Fee-zmEJ' <> DerivedTable5.AddressV4AX,
        IF(DerivedTable2.Amtp4ee LIKE 'Country-Ti9V','Fee-WFlV',SourceE.FieldF),
        IF(
          STARTS_WITH(SourceC.FieldE,'Account-AIDY'), SourceA.FieldH,
          'Fee-4V03'))
        AS CodeRYLk,
      CASE
        WHEN STARTS_WITH('Code-ZMnA',DerivedTable8.CustomerN13F) THEN
          IF(
            ENDS_WITH(DerivedTable5.CodePGRC,DerivedTable7.Codeuiec),
            DerivedTable2.AmtNncS, 'Address-wJAv')
        WHEN STARTS_WITH('CCY-zMAz',SourceA.FieldE) THEN
          IF('Fee-AdFE' <> 'Account-gH6X',SourceA.FieldH,'Country-Uwyf')
        WHEN ENDS_WITH(DerivedTable4.CodeJM19,DerivedTable3.FeeCUde) THEN
          IF(ENDS_WITH('Account-dBEz','Amt-6C2J'),'Amt-KJ4q','Code-axIS')
        WHEN DerivedTable7.CustomertAao LIKE DerivedTable3.CustomerULyd THEN
          IF(
            STARTS_WITH(DerivedTable7.AddressuSTK,SourceC.FieldI),
            'Address-xg1l', 'Account-A1ZM')
        WHEN ENDS_WITH('Account-Bb8j',DerivedTable3.CustomerLP0M) THEN
          IF(
            ENDS_WITH(DerivedTable7.AddressuSTK,'Code-KLY4'), 'Address-b1g0',
            SourceD.FieldI)
        ELSE
          IF(
            ENDS_WITH('Country-hY36','Code-lsIc'), DerivedTable5.FeeogF5,
            'Amt-w4Rw')
      END
        AS AmtWwmn,
      IF(
        ENDS_WITH(DerivedTable5.Code7INp,DerivedTable7.FeeBS3r),
        CASE
          WHEN SourceC.FieldC = DerivedTable7.Address3l5L THEN
            'Account-Pjhz'
          WHEN DerivedTable7.Codeuiec = DerivedTable7.CodeOp8k THEN
            DerivedTable3.AccountViga
          WHEN STARTS_WITH('Amt-5yHG','Country-zgz5') THEN
            'Address-L9XV'
          WHEN STARTS_WITH('CCY-2UUT',DerivedTable3.Account4sxv) THEN
            'Account-nqwG'
          ELSE
            SourceE.FieldE
        END, IF(
               STARTS_WITH(DerivedTable8.Account3V3K,DerivedTable5.Code7INp),
               'Country-jHuv', DerivedTable3.FeeF6P4))
        AS AmtPqvp,
      IF(
        DerivedTable2.AccounttVM1 <> SourceC.FieldE,
        IF(
          ENDS_WITH(DerivedTable2.AmtmorU,'Customer-2eny'),
          DerivedTable3.CCYO742, DerivedTable3.Code3Xip),
        CASE
          WHEN STARTS_WITH('Country-YGQa',SourceD.FieldA) THEN
            SourceD.FieldA
          WHEN ENDS_WITH('Account-LRKP','Code-CxMR') THEN
            'Account-xgmj'
          WHEN SourceC.FieldC <> DerivedTable4.AccounteutQ THEN
            'Amt-F7t1'
          WHEN SourceC.FieldB = 'Fee-2aPK' THEN
            'Address-lSFP'
          WHEN STARTS_WITH(SourceB.FieldC,'CCY-p1cf') THEN
            'CCY-nZ8c'
          ELSE
            SourceB.FieldA
        END)
        AS FeeJhoK,
      CASE
        WHEN ENDS_WITH(DerivedTable5.Code7INp,'Fee-byLP') THEN
          CASE
            WHEN
              'Account-CqqL' != SourceA.FieldE
            THEN
              'CCY-fX42'
            WHEN
              ENDS_WITH('Account-J9H1','Address-rMFn')
            THEN
              'Fee-8Zjd'
            WHEN
              ENDS_WITH('Code-pYje',SourceD.FieldE)
            THEN
              'Code-91E6'
            WHEN
              STARTS_WITH(DerivedTable7.CountryrPg8,DerivedTable3.CustomereBLI)
            THEN
              'Account-EnpX'
            WHEN
              ENDS_WITH(SourceA.FieldH,SourceE.FieldJ)
            THEN
              'CCY-92Wq'
            ELSE
              'Address-pE7X'
          END
        WHEN ENDS_WITH(DerivedTable7.Address3l5L,SourceC.FieldC) THEN
          CASE
            WHEN DerivedTable3.Codeaqvh != 'Fee-REXn' THEN
              'CCY-Yffg'
            WHEN 'Country-bx5d' = 'CCY-39lj' THEN
              'Amt-BZMc'
            WHEN DerivedTable3.CountryoFFA <> 'Amt-Oyox' THEN
              'CCY-8xi5'
            ELSE
              DerivedTable5.AddressV4AX
          END
        WHEN DerivedTable5.CCYVD4h = SourceC.FieldF THEN
          IF(
            STARTS_WITH('Customer-JcHf','Customer-wWjm'), DerivedTable7.CCYZD20,
            'Code-I2uC')
        WHEN 'Address-zwlP' NOT LIKE SourceA.FieldH THEN
          CASE
            WHEN STARTS_WITH('Amt-Qkni','Account-hUj6') THEN
              'Account-c6Na'
            WHEN 'CCY-v9fi' NOT LIKE 'Customer-Aq6X' THEN
              'Code-LzeM'
            WHEN ENDS_WITH(DerivedTable7.CustomertAao,'Country-Ne1o') THEN
              'Code-KtGs'
            WHEN STARTS_WITH('Code-MhKp',SourceA.FieldE) THEN
              'Customer-uxci'
            WHEN DerivedTable7.Amt9aae <> SourceA.FieldH THEN
              'Country-BM38'
            ELSE
              SourceA.FieldF
          END
        ELSE
          CASE
            WHEN STARTS_WITH('Country-rK09',DerivedTable8.CustomerCtTz) THEN
              SourceA.FieldG
            WHEN ENDS_WITH('Amt-iMti','Address-UKnM') THEN
              SourceA.FieldJ
            WHEN SourceA.FieldD <> DerivedTable5.CCYVD4h THEN
              'Amt-vqqV'
            ELSE
              'Account-55hq'
          END
      END
        AS CountryjjK4,
      CASE
        WHEN DerivedTable8.CountryKrJm LIKE 'Account-Y3Za' THEN
          IF(
            ENDS_WITH(DerivedTable3.CountryoFFA,'Amt-3vHd'),
            DerivedTable4.Country5f1u, DerivedTable3.CodeUeLq)
        WHEN ENDS_WITH(DerivedTable7.Address3l5L,SourceB.FieldG) THEN
          IF(
            DerivedTable2.Address7oYI LIKE 'Code-cn9k', 'Code-LqIh',
            'Customer-heJa')
        WHEN DerivedTable4.CodeXHEv LIKE SourceA.FieldH THEN
          CASE
            WHEN
              STARTS_WITH(DerivedTable5.Customera0qh,DerivedTable4.AddressB6KJ)
            THEN
              DerivedTable4.CodeXHEv
            WHEN
              SourceC.FieldB NOT LIKE DerivedTable2.Amtp4ee
            THEN
              DerivedTable2.Amt885g
            WHEN
              'Account-ElVh' = 'Fee-lJ7i'
            THEN
              'Country-fxjK'
            ELSE
              'Account-6UyD'
          END
        WHEN DerivedTable7.Address3l5L NOT LIKE 'Fee-DO3n' THEN
          CASE
            WHEN 'Customer-h1ti' NOT LIKE 'Fee-PbYz' THEN
              SourceA.FieldG
            WHEN STARTS_WITH('Customer-FXUH','Code-TGA6') THEN
              DerivedTable8.CustomerZmFX
            WHEN SourceB.FieldC = SourceA.FieldJ THEN
              DerivedTable4.FeezLpC
            WHEN ENDS_WITH(DerivedTable3.CountrygPvi,SourceC.FieldF) THEN
              DerivedTable2.AccountctCy
            WHEN 'Customer-yrgz' = 'Code-mUVo' THEN
              'Address-ZvmQ'
            ELSE
              'Customer-9SgY'
          END
        WHEN ENDS_WITH('Amt-TS9O','Account-WKU9') THEN
          IF(
            DerivedTable2.Address5RWP <> DerivedTable7.CCYZD20,
            DerivedTable8.CustomerZmFX, 'Customer-gOrh')
        ELSE
          CASE
            WHEN DerivedTable8.CustomerZmFX LIKE DerivedTable4.FeezLpC THEN
              DerivedTable3.CustomereBLI
            WHEN DerivedTable8.FeePN5q = 'Fee-vVQg' THEN
              'Customer-geiY'
            WHEN 'Address-wLnt' LIKE SourceC.FieldF THEN
              SourceA.FieldA
            WHEN 'Code-1K63' != DerivedTable5.Customera0qh THEN
              'Country-sDtf'
            WHEN ENDS_WITH(DerivedTable2.AmtNncS,'Account-6z6q') THEN
              SourceC.FieldJ
            ELSE
              'CCY-f0NR'
          END
      END
        AS FeevP7V,
      IF(
        'Code-LQNW' <> 'Fee-eNCT',
        IF(
          ENDS_WITH('Country-knPI',SourceE.FieldF), SourceE.FieldJ,
          DerivedTable3.CountrygPvi),
        IF(
          STARTS_WITH('Country-ipEu','Account-YP5a'),
          DerivedTable8.CustomerCtTz, 'Fee-1RJG'))
        AS CustomergHH2,
      IF(
        STARTS_WITH(SourceD.FieldJ,'Fee-Czbl'),
        IF(
          DerivedTable8.FeePN5q NOT LIKE 'Account-YaC7', 'Account-q4lv',
          'Account-6qRR'),
        IF(ENDS_WITH('CCY-I7Vk','Amt-WbVi'),'Code-m36B',SourceE.FieldB))
        AS AccountrJ2s,
      IF(
        ENDS_WITH('Country-LgZf','Amt-4Fbn'),
        CASE
          WHEN STARTS_WITH(DerivedTable3.Codeaqvh,'Fee-H6Io') THEN
            DerivedTable2.Address7oYI
          WHEN DerivedTable4.AddressB6KJ NOT LIKE 'Fee-03x4' THEN
            'Country-lM8e'
          WHEN STARTS_WITH(DerivedTable2.AccountrJZf,'Customer-2ZUg') THEN
            DerivedTable7.CountryrPg8
          ELSE
            'Address-BpJL'
        END,
        CASE
          WHEN
            'Account-v67A' != DerivedTable3.FeeF6P4
          THEN
            'Code-Rcxm'
          WHEN
            'Fee-MUxf' != 'Customer-bLfE'
          THEN
            'Country-wkcs'
          WHEN
            STARTS_WITH(SourceC.FieldI,DerivedTable4.CodeJM19)
          THEN
            'Fee-i1uF'
          WHEN
            SourceB.FieldF LIKE 'Customer-2b3g'
          THEN
            DerivedTable3.CCYO742
          WHEN
            ENDS_WITH(DerivedTable8.Country2xzz,DerivedTable3.CountryoFFA)
          THEN
            DerivedTable4.CodeJM19
          ELSE
            'CCY-lvU6'
        END)
        AS Countryb30V,
      CASE
        WHEN ENDS_WITH(SourceB.FieldE,SourceD.FieldJ) THEN
          CASE
            WHEN 'Country-fBXq' <> 'Address-wwxp' THEN
              'Customer-ausf'
            WHEN ENDS_WITH(SourceC.FieldB,'Customer-A47R') THEN
              DerivedTable7.Address3l5L
            WHEN 'Customer-EiU9' <> 'Country-qrOj' THEN
              'Country-jIWW'
            WHEN SourceB.FieldF NOT LIKE 'Address-MtRw' THEN
              'Code-BRc7'
            WHEN 'Account-j8Cw' NOT LIKE 'Account-MlVF' THEN
              SourceE.FieldF
            ELSE
              DerivedTable8.CodeNPzY
          END
        WHEN ENDS_WITH('Country-DQJU','CCY-oy4r') THEN
          IF(
            ENDS_WITH(DerivedTable5.Feepmor,'Country-Kpm4'), 'Customer-Ddwp',
            SourceE.FieldE)
        WHEN SourceE.FieldC LIKE DerivedTable7.CustomertAao THEN
          IF(
            DerivedTable7.CountryrPg8 = 'Customer-CNTL',
            DerivedTable5.Customera0qh, SourceE.FieldJ)
        ELSE
          IF(
            STARTS_WITH('Code-HdX6','Code-jTtl'), SourceB.FieldB,
            DerivedTable3.CodeUeLq)
      END
        AS Amty4cx,
      CASE
        WHEN 'Account-xFo2' <> SourceE.FieldG THEN
          IF(
            STARTS_WITH('Address-NZ7C','CCY-pJHU'), DerivedTable3.FeeD0Po,
            SourceB.FieldC)
        WHEN 'CCY-ro0D' LIKE DerivedTable7.CodeOp8k THEN
          IF('Customer-AmdR' NOT LIKE SourceE.FieldI,'Code-Buev','Fee-uMIF')
        ELSE
          IF(
            ENDS_WITH('Fee-gxW6','Country-duH2'), SourceD.FieldD,
            DerivedTable3.CountrygPvi)
      END
        AS FeegfLn,
      IF(
        STARTS_WITH('Code-sVEe',SourceD.FieldG),
        IF('Country-MQra' NOT LIKE 'Account-AkIt','Account-Qkee','Code-BYvB'),
        CASE
          WHEN ENDS_WITH('Account-YS5g','Account-xEPq') THEN
            DerivedTable5.CodePGRC
          WHEN STARTS_WITH('Address-9gd2','Customer-XlZH') THEN
            'Amt-tUgQ'
          WHEN STARTS_WITH(DerivedTable3.Codeaqvh,DerivedTable3.Feeg8kl) THEN
            'CCY-zjRW'
          ELSE
            DerivedTable3.CodeVHWz
        END)
        AS CountryZozw,
      IF(
        STARTS_WITH('Account-SIm9',SourceC.FieldG),
        CASE
          WHEN SourceB.FieldC = 'Amt-4hg0' THEN
            DerivedTable4.Country8VtM
          WHEN 'Account-mEVv' LIKE 'Code-X3uG' THEN
            'Account-9eRL'
          ELSE
            'Code-jwhP'
        END, IF(
               'Customer-UU2Y' LIKE DerivedTable2.AccountrJZf, 'Country-Mzor',
               SourceC.FieldF))
        AS CustomercFOz,
      IF(
        STARTS_WITH(DerivedTable3.CustomereBLI,SourceD.FieldH),
        IF('Account-3xWs' NOT LIKE SourceA.FieldI,SourceD.FieldE,'Code-lZsm'),
        IF(
          ENDS_WITH(DerivedTable2.Account61kv,'Code-DZT7'),
          DerivedTable2.Account61kv, DerivedTable2.AmtNncS))
        AS Feeg1IH,
      IF(
        'Customer-O6ij' != 'CCY-cZRc',
        IF('Code-qGY8' NOT LIKE 'Code-fNmg',SourceE.FieldF,'Country-ijrD'),
        IF(
          DerivedTable8.CCYeu0i != DerivedTable7.CountryXTJ7,
          DerivedTable3.FeeCUde, 'CCY-VoLt'))
        AS CustomerTBln,
      IF(
        'Country-PdOZ' != DerivedTable8.CCYeu0i,
        IF(
          ENDS_WITH(DerivedTable8.CustomerCtTz,DerivedTable2.Account61kv),
          'Fee-IWj4', 'Fee-Sypm'),
        IF(
          DerivedTable2.Amt885g NOT LIKE SourceE.FieldA, 'Code-ojuJ',
          DerivedTable5.CountrynpDj))
        AS AmtpIj4,
      CASE
        WHEN STARTS_WITH('Customer-D04O','Code-Iyhn') THEN
          CASE
            WHEN STARTS_WITH('CCY-aAWH',DerivedTable7.CountryrPg8) THEN
              DerivedTable3.FeeT7G9
            WHEN ENDS_WITH('Customer-ECfr','Amt-K5st') THEN
              'CCY-xZFH'
            WHEN STARTS_WITH('Customer-sE7A','Address-y9TZ') THEN
              'Fee-oRAv'
            ELSE
              'Country-O9DI'
          END
        WHEN ENDS_WITH(DerivedTable8.CodeNPzY,'Code-gCLA') THEN
          IF('Fee-OYcp' != 'Fee-bf3i','Customer-h42P','Amt-U81j')
        WHEN ENDS_WITH(SourceC.FieldD,DerivedTable4.FeesrBh) THEN
          CASE
            WHEN SourceD.FieldC NOT LIKE SourceA.FieldC THEN
              SourceB.FieldF
            WHEN ENDS_WITH('Fee-XtXv',DerivedTable3.FeeF6P4) THEN
              'Address-ZW43'
            ELSE
              SourceC.FieldC
          END
        WHEN SourceA.FieldJ != 'Amt-phEC' THEN
          IF('CCY-Lfmi' = 'Country-x2nE',DerivedTable2.CodeRziT,'Account-ww3d')
        ELSE
          CASE
            WHEN SourceB.FieldF <> DerivedTable5.Customera0qh THEN
              DerivedTable2.Amt885g
            WHEN STARTS_WITH('CCY-UMGl','CCY-CDpg') THEN
              DerivedTable3.Codeaqvh
            ELSE
              SourceD.FieldH
          END
      END
        AS FeesrLs,
      IF(
        SourceC.FieldI LIKE DerivedTable4.CCYY6No,
        CASE
          WHEN STARTS_WITH('Account-T5tR','Fee-0Qh6') THEN
            'Customer-uuFd'
          WHEN DerivedTable8.AmtSXKP LIKE 'Account-Xx86' THEN
            SourceD.FieldD
          WHEN SourceC.FieldC <> SourceC.FieldD THEN
            DerivedTable5.CCYQK2Q
          ELSE
            SourceB.FieldC
        END, IF('CCY-j2YE' LIKE 'Country-2pjz','Address-XimZ',SourceD.FieldE))
        AS AddressTwoR
    FROM
      testdataset.SourceE AS SourceE
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Country-xHvy' = SourceA.FieldH OR
         STARTS_WITH(SourceA.FieldD,'Amt-CM6N') AND
         'Account-zNqZ' != 'Customer-rLhf' AND
         ENDS_WITH('Code-VGKB',SourceE.FieldH))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (SourceE.FieldB LIKE 'Country-B1jQ' OR
         ENDS_WITH('Amt-Zkrl',SourceB.FieldH) AND
         SourceA.FieldH LIKE 'Address-aUSK' OR
         STARTS_WITH(SourceE.FieldC,'Address-ErK2'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH(SourceA.FieldA,SourceE.FieldF) AND
         'Fee-wdKT' NOT LIKE 'CCY-AZYb' OR
         SourceE.FieldG = SourceB.FieldH OR
         STARTS_WITH(SourceE.FieldG,'Amt-4yNZ'))
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('CCY-JvCQ','Code-r4ht') AND
         ENDS_WITH('Country-9lDp',SourceC.FieldJ) AND
         ENDS_WITH(SourceC.FieldA,SourceB.FieldJ) OR
         ENDS_WITH('Address-gTgC','Address-AZHj'))
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (ENDS_WITH(SourceC.FieldC,SourceB.FieldG) AND
         STARTS_WITH(SourceC.FieldH,SourceB.FieldD) AND
         SourceD.FieldI != 'Address-ahHK' AND
         ENDS_WITH('Country-NanE',DerivedTable2.Address5RWP))
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (SourceD.FieldJ NOT LIKE DerivedTable3.CustomerLP0M OR
         DerivedTable2.AmtNncS NOT LIKE 'CCY-5yIC' AND
         STARTS_WITH('Code-hAD1',SourceA.FieldH) AND
         ENDS_WITH('Amt-TAsG','Fee-FJej'))
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (ENDS_WITH(SourceC.FieldE,SourceB.FieldG) OR
         ENDS_WITH(DerivedTable4.Addressm87D,SourceA.FieldF) AND
         ENDS_WITH(SourceE.FieldJ,'Account-bwBy') OR
         SourceC.FieldH LIKE 'Account-r760')
      INNER JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (SourceD.FieldG = SourceB.FieldE OR
         DerivedTable4.FeexvTE <> 'Country-JMJ7' AND
         ENDS_WITH(DerivedTable5.CCYQK2Q,'Address-LUnw') AND
         'Customer-5GtY' LIKE 'Code-LyrN')
      LEFT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        (DerivedTable4.Country5f1u != DerivedTable7.CustomertAao AND
         ENDS_WITH('Country-9dAM',DerivedTable2.CountrywaWi) OR
         ENDS_WITH(DerivedTable4.Account1SJF,DerivedTable4.CCYY6No) OR
         STARTS_WITH(DerivedTable2.CodeDmva,DerivedTable2.AmtmorU))
      INNER JOIN testdataset.DerivedTable8 AS DerivedTable8 ON
        (SourceB.FieldD = DerivedTable5.CodePGRC OR 'Fee-GCWi' = 'CCY-IYnD' OR
         STARTS_WITH('Fee-I5lZ',DerivedTable4.Country8VtM) AND
         DerivedTable4.AccounteutQ <> 'Customer-9Qyk')
    WHERE
      ENDS_WITH('Fee-5agd','Fee-R3MT') OR
      STARTS_WITH(DerivedTable3.AccountViga,'Address-1lsi') OR
      'CCY-flvR' <> 'CCY-qD1m' OR DerivedTable3.CountryoFFA != 'CCY-Ombo' AND
      'Customer-zFYU' != 'Code-wJ1F' OR
      STARTS_WITH(SourceB.FieldG,DerivedTable3.Feeg8kl) AND
      ENDS_WITH(DerivedTable7.CountryrPg8,DerivedTable7.CCYZD20) OR
      STARTS_WITH(DerivedTable8.Account3V3K,DerivedTable2.CodeDmva)
  ),
  Table42 AS (
    SELECT
      CASE
        WHEN STARTS_WITH(DerivedTable1.CustomerWrlz,'Fee-tQjF') THEN
          CASE
            WHEN DerivedTable5.FeeogF5 = DerivedTable6.AddressPpXC THEN
              SourceD.FieldB
            WHEN DerivedTable3.CodeUeLq != 'Account-KCCQ' THEN
              DerivedTable1.CCYXSkS
            WHEN 'Customer-ClSW' LIKE DerivedTable3.Code3Xip THEN
              SourceA.FieldH
            ELSE
              SourceC.FieldC
          END
        WHEN STARTS_WITH(DerivedTable8.Country2xzz,'CCY-fQxv') THEN
          CASE
            WHEN STARTS_WITH('Account-AEh7',DerivedTable5.CCY76AX) THEN
              'Account-Z4eT'
            WHEN ENDS_WITH('CCY-yuAN','CCY-PR5V') THEN
              DerivedTable5.CCYVD4h
            WHEN 'Code-tZpO' NOT LIKE Table41.AccountrJ2s THEN
              SourceB.FieldH
            WHEN ENDS_WITH(DerivedTable3.FeeCUde,'Country-yhC9') THEN
              DerivedTable3.Feeg8kl
            WHEN STARTS_WITH('CCY-JVmL',DerivedTable6.CountryD7gw) THEN
              'Country-DZ6X'
            ELSE
              'Country-j2W5'
          END
        ELSE
          IF(
            'Customer-TGCk' <> 'Code-dxk0', 'CCY-FMu1',
            DerivedTable1.AccountmNQo)
      END
        AS FeelBjk,
      IF(
        ENDS_WITH(SourceC.FieldE,'Customer-T6hH'),
        IF(
          STARTS_WITH('Amt-Fg1l','Address-7KHf'), 'Account-y8sb',
          DerivedTable8.CCYeu0i),
        CASE
          WHEN SourceB.FieldJ LIKE 'Amt-4aWq' THEN
            DerivedTable6.AddressWejH
          WHEN 'Amt-pHv5' <> 'Code-jR1S' THEN
            SourceA.FieldB
          WHEN STARTS_WITH('Country-9GkG',Table41.AmtpIj4) THEN
            'Amt-2NT2'
          WHEN ENDS_WITH('Account-cTWr','Account-b24W') THEN
            'Fee-RDQD'
          ELSE
            DerivedTable3.CustomerULyd
        END)
        AS AddressYMvu,
      IF(
        DerivedTable8.Amti3Vx != 'Country-xX9o',
        IF(
          ENDS_WITH('Code-qzgY','Account-1MZa'), 'Customer-SGvc',
          Table41.FeesrLs), IF(
                              STARTS_WITH(Table41.FeegfLn,'Address-sg8e'),
                              DerivedTable1.CustomerKrwR, 'Address-pq3q'))
        AS AccountXdsW,
      IF(
        'Fee-cBbM' <> 'Amt-yVcN',
        IF(
          ENDS_WITH(SourceA.FieldG,DerivedTable1.AddressoeLk),
          DerivedTable3.CustomerULyd, 'Customer-Cm8b'),
        IF(
          STARTS_WITH('Address-0Ifh','CCY-cqLv'), DerivedTable6.AmtCCH1,
          'Customer-eVsW'))
        AS CodeH2Z2,
      IF(
        'Address-I5nw' = SourceD.FieldH,
        IF(
          STARTS_WITH(Table41.CustomercFOz,SourceC.FieldD), Table41.CodeRYLk,
          SourceE.FieldI),
        CASE
          WHEN 'Country-Ig6x' LIKE SourceE.FieldC THEN
            DerivedTable3.FeeD0Po
          WHEN 'Customer-uZmv' != DerivedTable8.CustomerZmFX THEN
            Table41.FeeJhoK
          WHEN ENDS_WITH(SourceD.FieldA,DerivedTable1.Addressrgjd) THEN
            DerivedTable3.Code3Xip
          ELSE
            'Fee-b7Lo'
        END)
        AS Codev68w,
      IF(
        STARTS_WITH(SourceC.FieldE,SourceE.FieldH),
        IF(ENDS_WITH(SourceE.FieldJ,'Fee-0fh5'),'Code-Uurk','Code-WiXx'),
        IF(
          STARTS_WITH(DerivedTable8.Customerrbtf,'Fee-lNLu'), SourceD.FieldC,
          'Customer-RVWj'))
        AS AmtKesM,
      IF(
        'Amt-iM6S' NOT LIKE 'Country-wOED',
        CASE
          WHEN STARTS_WITH(SourceE.FieldJ,'Amt-QzJa') THEN
            'Fee-ntHE'
          WHEN ENDS_WITH(DerivedTable6.AddressD7PZ,'Account-0KA1') THEN
            'Amt-KlKH'
          WHEN STARTS_WITH('Account-nSSt','Fee-cCcz') THEN
            'Amt-dgoX'
          WHEN STARTS_WITH(DerivedTable6.AddressPIbO,'Fee-w79Q') THEN
            'Country-t9em'
          WHEN STARTS_WITH('Account-osvs','Fee-MITI') THEN
            'Account-QxUR'
          ELSE
            'Address-rlat'
        END, IF(
               STARTS_WITH(DerivedTable6.CountryD7gw,'CCY-p5rh'),
               'Customer-U49Y', 'Code-sHxA'))
        AS CCYeGiQ,
      IF(
        STARTS_WITH(Table41.CodeRYLk,SourceC.FieldG),
        IF(
          ENDS_WITH(Table41.CountryjjK4,'Fee-scQq'), Table41.Amty4cx,
          'Address-3Qsn'),
        IF(
          DerivedTable6.AddressWejH NOT LIKE DerivedTable5.Customera0qh,
          SourceD.FieldD, Table41.Codel32i))
        AS CustomerOjHu,
      IF(
        ENDS_WITH('Fee-Sb3k','Amt-i1n7'),
        IF('Amt-S8Sx' <> 'Customer-jcE7','CCY-3xQ6',SourceB.FieldJ),
        CASE
          WHEN ENDS_WITH(Table41.CountryjjK4,'Code-lV1k') THEN
            Table41.CountryjjK4
          WHEN 'CCY-GNXr' != DerivedTable1.CustomerKrwR THEN
            Table41.AmtPqvp
          ELSE
            DerivedTable1.CustomerWrlz
        END)
        AS Accountrlei,
      IF(
        STARTS_WITH(DerivedTable6.CodeY4Yb,'CCY-mwj4'),
        IF('Address-5oO6' NOT LIKE 'Country-nezU','Fee-bA1x',SourceD.FieldG),
        IF(
          STARTS_WITH('Address-dtOO',SourceE.FieldA), 'Country-litY',
          'CCY-WMIQ'))
        AS CCYSpNc,
      CASE
        WHEN ENDS_WITH('Address-WytU','Customer-SPG6') THEN
          CASE
            WHEN 'Customer-ygt9' <> SourceE.FieldF THEN
              SourceB.FieldF
            WHEN 'Amt-DDrD' NOT LIKE 'Country-uPc8' THEN
              DerivedTable5.Code7INp
            WHEN 'Address-ihAb' != DerivedTable3.Codeaqvh THEN
              DerivedTable3.FeeD0Po
            ELSE
              'Customer-CE80'
          END
        WHEN ENDS_WITH('Country-uhIW','Amt-FCfe') THEN
          IF(
            'Account-ttUs' LIKE DerivedTable6.AmtW2w8, SourceE.FieldD,
            DerivedTable3.CountrygMB4)
        WHEN STARTS_WITH('CCY-8nWn','Code-OJQz') THEN
          IF(
            DerivedTable3.Feeg8kl NOT LIKE 'Account-79i1',
            DerivedTable1.CustomerWpfX, Table41.AmtPqvp)
        ELSE
          IF(
            STARTS_WITH('Account-22l2',SourceB.FieldD), SourceC.FieldI,
            'Fee-Ou0Q')
      END
        AS CCY5qHh,
      IF(
        STARTS_WITH(Table41.Codel32i,DerivedTable5.CodePGRC),
        IF(
          STARTS_WITH('Amt-oBEe',DerivedTable6.AmtW2w8), 'Customer-QELT',
          SourceA.FieldC), IF(
                             Table41.AccountrJ2s = 'CCY-enVb',
                             DerivedTable1.AddressoeLk, DerivedTable3.CodeUeLq))
        AS Amt7Jph,
      IF(
        STARTS_WITH('Address-AvWV','Country-M8r8'),
        IF(
          STARTS_WITH(SourceE.FieldG,'Customer-IeK0'), Table41.CustomercFOz,
          SourceE.FieldH), IF(
                             ENDS_WITH(SourceA.FieldI,SourceB.FieldC),
                             Table41.CountryZozw, DerivedTable8.CustomerAce4))
        AS AmtGWDU,
      IF(
        'Customer-0yKs' <> DerivedTable1.AmtzEYq,
        CASE
          WHEN 'Customer-u7cX' NOT LIKE 'Customer-ww9Q' THEN
            'CCY-HOIb'
          WHEN ENDS_WITH('Account-3vXU','Code-wBHE') THEN
            SourceD.FieldB
          ELSE
            'Country-iGr4'
        END, IF(
               STARTS_WITH('Fee-ioHQ',DerivedTable5.Feepmor), 'CCY-V8FI',
               SourceC.FieldF))
        AS AccountiqH3
    FROM
      testdataset.DerivedTable8 AS DerivedTable8
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Customer-LqB2' <> DerivedTable8.Amti3Vx AND
         'Address-cKHw' <> 'Account-Eoht' AND
         STARTS_WITH(DerivedTable8.CCYeu0i,'Fee-KAeT') AND
         STARTS_WITH(SourceA.FieldD,'Amt-mI2G'))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('Amt-j8gO','CCY-wcvT') AND
         DerivedTable8.CustomerZmFX != 'Account-6TZs' AND
         SourceB.FieldH <> 'Country-oVX9' OR
         DerivedTable8.Country2xzz NOT LIKE DerivedTable8.Country2xzz)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH(DerivedTable8.FeePN5q,'Account-TzLy') OR
         ENDS_WITH(DerivedTable8.CCYeu0i,'Customer-6XM3') AND
         'Country-fAs2' <> DerivedTable8.CustomerAce4 OR
         DerivedTable8.CodeNPzY <> 'Amt-j3Ki')
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH(DerivedTable8.CountryKrJm,SourceA.FieldA) AND
         ENDS_WITH(SourceD.FieldA,'Fee-uYfX') AND
         STARTS_WITH('Code-Wggv','Country-e8AN') AND
         STARTS_WITH(DerivedTable8.CustomerN13F,'Customer-bFlQ'))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('Amt-64wG',SourceE.FieldI) AND
         SourceB.FieldE LIKE 'Customer-SR7o' AND
         ENDS_WITH('Customer-YYHC',SourceA.FieldD) OR
         'Country-1FGw' = SourceE.FieldE)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (DerivedTable1.CustomerWrlz != SourceD.FieldE AND
         STARTS_WITH('CCY-zKSV',SourceA.FieldC) AND
         ENDS_WITH(DerivedTable1.CustomerWrlz,'Address-5FXZ') OR
         'Account-K54I' != DerivedTable1.CountryV1io)
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH(DerivedTable8.CustomerZmFX,SourceC.FieldB) AND
         DerivedTable8.CodeNPzY != 'Amt-qgTH' AND
         SourceA.FieldE = SourceA.FieldI AND
         STARTS_WITH(DerivedTable8.Account3V3K,SourceE.FieldC))
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (ENDS_WITH(SourceA.FieldF,SourceA.FieldI) OR
         ENDS_WITH('Code-gDbD','Account-5oCf') OR
         'Account-keng' NOT LIKE 'Account-newD' OR
         STARTS_WITH(DerivedTable1.Addressrgjd,SourceB.FieldC))
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (ENDS_WITH('Country-Dy4t',DerivedTable8.Account3V3K) OR
         DerivedTable6.Address54VO LIKE 'Amt-8mBf' AND
         ENDS_WITH('Account-ZBw1','Customer-o1H4') AND
         STARTS_WITH(DerivedTable5.Code7INp,'Account-wgfV'))
      INNER JOIN Table41 AS Table41 ON
        (STARTS_WITH('Customer-ZbzY',DerivedTable3.FeeqkjX) AND
         STARTS_WITH(Table41.AmtPqvp,'Code-EKCj') AND
         'Fee-zz89' NOT LIKE 'Account-284K' OR
         DerivedTable3.Account4sxv LIKE 'Address-xoB0')
    WHERE
      ENDS_WITH('Fee-wgd3','CCY-iDK8') OR 'Customer-JHHX' = 'Country-ODic' OR
      ENDS_WITH(SourceA.FieldE,SourceC.FieldE) OR
      'CCY-7j5X' = 'Customer-zYWu' OR
      ENDS_WITH(Table41.FeevP7V,Table41.FeevP7V) OR
      STARTS_WITH('Address-vRlW',DerivedTable5.CCYVD4h) OR
      SourceB.FieldG != 'Amt-TtrL' OR 'CCY-uAN3' LIKE SourceA.FieldJ
  ),
  Table43 AS (
    SELECT
      IF(
        ENDS_WITH(DerivedTable1.CustomerWrlz,'Customer-vuA4'),
        IF(
          'Code-OG9E' NOT LIKE SourceE.FieldA, 'Code-Rt0L',
          DerivedTable4.AccounteutQ),
        IF(
          Table42.AddressYMvu LIKE DerivedTable4.FeesrBh, 'Amt-z8sC',
          'Amt-dP7y'))
        AS CodeHTTE,
      IF(
        ENDS_WITH(SourceA.FieldC,'Code-TEsB'),
        IF('Customer-uLQi' = 'Amt-GPWm',SourceE.FieldE,'Address-06J9'),
        IF(
          ENDS_WITH('Fee-XhQB',SourceD.FieldC), DerivedTable6.AddressWejH,
          Table42.Amt7Jph))
        AS AddressW27w,
      IF(
        ENDS_WITH('Customer-uFL9',SourceB.FieldD),
        CASE
          WHEN STARTS_WITH(DerivedTable3.AccountViga,SourceD.FieldE) THEN
            'Address-PmYS'
          WHEN ENDS_WITH(DerivedTable8.CodeNPzY,'Address-bxc5') THEN
            DerivedTable4.Country5f1u
          WHEN SourceE.FieldA != DerivedTable2.Account61kv THEN
            SourceC.FieldB
          ELSE
            Table41.Feeg1IH
        END,
        CASE
          WHEN SourceB.FieldA NOT LIKE 'Country-rB6s' THEN
            'Code-ERDw'
          WHEN 'Customer-SGlW' NOT LIKE DerivedTable8.Code6dqw THEN
            'Code-sIrV'
          WHEN DerivedTable6.CustomerxLhn = 'Customer-dAdN' THEN
            'Country-NOqY'
          WHEN ENDS_WITH('Country-IlUE',SourceE.FieldD) THEN
            'Address-WRiN'
          WHEN SourceA.FieldI != DerivedTable8.CCYeu0i THEN
            SourceD.FieldA
          ELSE
            DerivedTable3.CountrygPvi
        END)
        AS CCYPFuO,
      IF(
        DerivedTable5.CCY76AX <> DerivedTable8.CustomerCtTz,
        CASE
          WHEN SourceA.FieldI <> DerivedTable8.CustomerAce4 THEN
            Table41.AmtpIj4
          WHEN ENDS_WITH('Amt-7NVU',SourceC.FieldC) THEN
            'Country-l0sE'
          WHEN STARTS_WITH('Customer-qCEI','Account-TlB6') THEN
            Table41.FeeJhoK
          ELSE
            'Fee-g0Dj'
        END, IF(
               'Fee-j0JL' NOT LIKE DerivedTable6.Address54VO, 'Code-5hky',
               SourceA.FieldI))
        AS AddressIbIV,
      IF(
        DerivedTable8.Account3V3K = DerivedTable6.Fee8TJM,
        IF(
          ENDS_WITH('Address-G9mh',DerivedTable4.CodelvtV), 'CCY-ng6I',
          SourceA.FieldG),
        CASE
          WHEN ENDS_WITH(DerivedTable4.Country8VtM,SourceD.FieldJ) THEN
            'CCY-TA83'
          WHEN DerivedTable1.CountryV1io != DerivedTable4.CCYY6No THEN
            DerivedTable6.CustomerCZg3
          WHEN STARTS_WITH(Table42.AccountiqH3,SourceB.FieldA) THEN
            DerivedTable4.AddressWvnP
          ELSE
            'Country-qhnY'
        END)
        AS AmtfxGr,
      IF(
        ENDS_WITH('Customer-LvHL',DerivedTable5.AddressV4AX),
        IF(ENDS_WITH('Code-Sw5K','Account-INfI'),SourceA.FieldD,'Account-ecOr'),
        CASE
          WHEN 'Address-0v9M' != DerivedTable4.AddressB6KJ THEN
            'Fee-iDPZ'
          WHEN ENDS_WITH('Code-RZvd',DerivedTable1.CountryV1io) THEN
            'Address-ts7S'
          WHEN STARTS_WITH(DerivedTable8.CCYeu0i,Table42.AmtGWDU) THEN
            DerivedTable4.CodeXHEv
          WHEN SourceC.FieldG = 'CCY-L8cj' THEN
            SourceC.FieldG
          WHEN STARTS_WITH('Fee-vAWb',DerivedTable2.AddresscM17) THEN
            DerivedTable2.AmtNncS
          ELSE
            DerivedTable6.CodeY4Yb
        END)
        AS AmtBV6V,
      CASE
        WHEN STARTS_WITH(DerivedTable6.AmtCCH1,SourceD.FieldE) THEN
          IF(
            STARTS_WITH(SourceD.FieldB,'Customer-Bdf1'), 'Account-jtSW',
            DerivedTable2.Amt885g)
        WHEN Table41.CountryZozw <> 'CCY-4PDz' THEN
          IF(
            'Customer-lqK8' NOT LIKE 'Customer-wBTy', DerivedTable4.CCYY6No,
            SourceE.FieldH)
        WHEN ENDS_WITH('Amt-oLdc','Fee-oTkK') THEN
          IF(SourceB.FieldI NOT LIKE 'Address-CLrc','Fee-6hrl',SourceE.FieldA)
        WHEN 'Fee-dkTZ' <> 'Country-Q3s7' THEN
          IF(
            STARTS_WITH('Customer-a5YE',SourceA.FieldF),
            DerivedTable1.AddressoeLk, DerivedTable2.CodeRziT)
        ELSE
          CASE
            WHEN
              DerivedTable5.Code7INp = DerivedTable3.CountrygMB4
            THEN
              DerivedTable2.Account61kv
            WHEN
              STARTS_WITH('Code-fRwG',DerivedTable2.AddressHdnu)
            THEN
              'Country-6zQj'
            WHEN
              STARTS_WITH(DerivedTable3.Account4sxv,DerivedTable8.FeePN5q)
            THEN
              'Code-daaV'
            ELSE
              'CCY-6eWC'
          END
      END
        AS Codei12M,
      IF(
        STARTS_WITH('Account-Rmpz',SourceC.FieldJ),
        IF(
          DerivedTable5.CountrynpDj != 'Country-8ht5', 'CCY-9ksA',
          DerivedTable3.Feeg8kl),
        IF(
          ENDS_WITH(DerivedTable1.CustomerWpfX,'Fee-xFnw'), SourceD.FieldG,
          DerivedTable3.FeeqkjX))
        AS AmtEJG4,
      CASE
        WHEN 'Address-f7Pa' NOT LIKE 'Amt-gtNV' THEN
          IF(
            DerivedTable1.CCYXSkS LIKE 'Code-UWeh', 'CCY-xD0U',
            DerivedTable4.FeexvTE)
        WHEN STARTS_WITH(DerivedTable8.CustomerN13F,Table41.AmtPqvp) THEN
          IF(
            STARTS_WITH('Customer-OdzP','Code-JOBR'), DerivedTable2.Address5RWP,
            'Amt-wa91')
        ELSE
          IF(
            DerivedTable4.CodeJM19 LIKE DerivedTable6.FeeEU6a, Table41.FeegfLn,
            'Country-pc3o')
      END
        AS CountryvFTP,
      IF(
        STARTS_WITH('Account-pmJs',SourceC.FieldA),
        IF(Table41.Feeg1IH LIKE 'Address-rXIk','Address-ScwJ',SourceE.FieldB),
        IF(
          STARTS_WITH(DerivedTable8.CustomerN13F,SourceE.FieldC),
          'Country-QHDK', Table41.Countryb30V))
        AS AmtqYKj,
      IF(
        STARTS_WITH('Code-FW0n',DerivedTable1.AccountmNQo),
        IF(
          SourceA.FieldJ LIKE DerivedTable2.AddresscM17, SourceA.FieldH,
          DerivedTable2.AmtNncS),
        IF(
          ENDS_WITH(DerivedTable4.AddressB6KJ,'Customer-cxEU'), 'Address-gaCp',
          'Code-TKYZ'))
        AS CountryWeL5,
      IF(
        ENDS_WITH('Fee-5bF4','CCY-8zsO'),
        IF(
          STARTS_WITH(DerivedTable1.AmtR86V,DerivedTable3.CustomereBLI),
          'Customer-t1c3', 'Account-T7cy'),
        IF(ENDS_WITH('Customer-biVC','Fee-5ol4'),Table42.Amt7Jph,'Code-Ft7l'))
        AS CountryV3Dr,
      IF(
        STARTS_WITH('Fee-2udg','Account-Wlbo'),
        CASE
          WHEN ENDS_WITH('CCY-xNof','Code-00Vs') THEN
            'Country-rUXR'
          WHEN 'Country-vWZM' NOT LIKE 'Customer-6DLY' THEN
            'Country-DWDd'
          WHEN ENDS_WITH('Fee-wBDn',DerivedTable8.CountryKrJm) THEN
            'Amt-o3NQ'
          WHEN DerivedTable8.Amti3Vx LIKE DerivedTable8.Code6dqw THEN
            'Country-H5qc'
          WHEN STARTS_WITH(DerivedTable2.AddresscM17,'Amt-0PK5') THEN
            Table41.FeeJhoK
          ELSE
            DerivedTable3.FeepQGh
        END, IF(
               ENDS_WITH('Amt-HRMd',DerivedTable4.FeexvTE), 'Address-32Zg',
               'Customer-TA7o'))
        AS Code19oe
    FROM
      testdataset.SourceB AS SourceB
      LEFT JOIN testdataset.SourceA AS SourceA ON
        ('Customer-eHVL' != SourceA.FieldE OR
         STARTS_WITH(SourceB.FieldE,SourceA.FieldH) AND
         SourceB.FieldG NOT LIKE SourceA.FieldH OR
         ENDS_WITH('Address-yoh1',SourceB.FieldB))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH(SourceB.FieldH,SourceA.FieldC) AND
         STARTS_WITH('Fee-C5IG','Fee-sKN1') AND
         STARTS_WITH('Country-FBdz',SourceC.FieldE) OR
         STARTS_WITH(SourceC.FieldJ,SourceC.FieldF))
      INNER JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH('Country-bGu6',SourceC.FieldA) AND
         ENDS_WITH('Customer-0JyD',SourceD.FieldG) OR
         SourceA.FieldB LIKE SourceD.FieldG AND SourceD.FieldE = SourceD.FieldA)
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('Code-aLGJ',SourceB.FieldD) OR
         ENDS_WITH('CCY-wckW','Fee-NJle') AND
         'Address-ydN1' = SourceE.FieldJ AND
         STARTS_WITH('Customer-Tsbw',SourceA.FieldI))
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('Customer-5qnZ','CCY-6dys') AND
         'Account-nNkx' LIKE SourceD.FieldD OR
         'Customer-Kqpb' NOT LIKE 'Fee-c649' AND
         ENDS_WITH('CCY-AYWK',SourceA.FieldH))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Account-BD52' <> DerivedTable2.AmtmorU AND
         STARTS_WITH('Address-lsWh','Fee-SO1e') AND
         STARTS_WITH('Code-sGcP','Account-exWP') AND
         SourceC.FieldF LIKE 'Address-HMTi')
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (ENDS_WITH(SourceA.FieldI,'Country-6ySE') OR
         ENDS_WITH('Code-9aiC',DerivedTable1.CustomerKrwR) AND
         ENDS_WITH(DerivedTable1.AmtR86V,'Fee-wI6D') AND
         DerivedTable1.AmtzEYq = 'Customer-9MrU')
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (DerivedTable2.Amtp4ee LIKE 'Address-QHPD' AND
         STARTS_WITH(SourceC.FieldC,SourceC.FieldI) OR
         DerivedTable1.FeerBIS <> 'Fee-q77e' OR
         'Customer-851J' <> 'Customer-nYVo')
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (DerivedTable4.Account1SJF NOT LIKE DerivedTable4.FeezLpC OR
         ENDS_WITH('Account-tiEU',SourceA.FieldF) OR
         'Country-QVw9' = SourceC.FieldE OR
         ENDS_WITH(DerivedTable1.CustomerWpfX,'Account-UrdQ'))
      RIGHT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (ENDS_WITH('Fee-om6q',DerivedTable2.CodeDmva) AND
         STARTS_WITH('Country-T9Ae','Country-34Fx') OR
         DerivedTable1.AmtzEYq != SourceC.FieldC OR
         STARTS_WITH('Account-0WhZ',DerivedTable3.Code3Xip))
      RIGHT JOIN testdataset.DerivedTable8 AS DerivedTable8 ON
        (STARTS_WITH(DerivedTable3.CodeUeLq,'CCY-KFxO') OR
         SourceE.FieldC = SourceB.FieldH OR
         'Code-je2W' <> 'Customer-hzri' AND 'Code-jJUW' LIKE 'Code-cX8s')
      LEFT JOIN Table41 AS Table41 ON
        (ENDS_WITH(SourceE.FieldC,'Fee-wU4d') AND
         STARTS_WITH(DerivedTable1.AccountqcVc,SourceA.FieldD) OR
         ENDS_WITH(SourceA.FieldB,DerivedTable5.FeeogF5) OR
         DerivedTable4.FeesrBh = 'Customer-SUZr')
      INNER JOIN Table42 AS Table42 ON
        (DerivedTable3.FeeqkjX LIKE 'Amt-gxR7' AND
         'Address-kYVk' LIKE SourceA.FieldE OR
         ENDS_WITH('Address-RkzJ',DerivedTable5.CCY76AX) OR
         Table41.CountryjjK4 != DerivedTable4.FeezLpC)
    WHERE
      DerivedTable8.FeePN5q != 'Account-djMl' OR
      ENDS_WITH(Table41.AccountrJ2s,'Account-MvEd') OR
      DerivedTable2.AddresscM17 != DerivedTable4.CodeJM19 AND
      STARTS_WITH('CCY-lRq4',DerivedTable6.FeeEU6a) OR
      ENDS_WITH('Country-R0zO',DerivedTable1.CountryV1io) OR
      DerivedTable6.AddressPpXC = Table41.Codel32i AND
      STARTS_WITH(Table42.AccountiqH3,'Account-sCWy') AND
      SourceC.FieldF != DerivedTable6.AddressPIbO
  ),
  Table44 AS (
    SELECT
      IF(
        STARTS_WITH('Address-C2YX','Fee-1Hfv'),
        IF(
          'Country-jUVY' != DerivedTable3.CountryoFFA, 'Fee-cB2B',
          SourceD.FieldJ), IF(
                             STARTS_WITH(DerivedTable8.Code6dqw,'Country-rElk'),
                             'Address-D0bD', DerivedTable8.Account3V3K))
        AS CCYvijC,
      IF(
        STARTS_WITH(DerivedTable6.Fee8TJM,Table41.CountryZozw),
        IF('Amt-Szxk' LIKE 'Account-Gwgx','Country-GpwG',SourceE.FieldA),
        CASE
          WHEN 'Country-h4ET' NOT LIKE 'CCY-8Y3g' THEN
            DerivedTable8.CustomerCtTz
          WHEN STARTS_WITH('CCY-xu9k',SourceB.FieldI) THEN
            'Address-ss7F'
          ELSE
            SourceA.FieldF
        END)
        AS CountryLvFX,
      IF(
        ENDS_WITH(DerivedTable4.CCYY6No,'CCY-XqAg'),
        IF(
          STARTS_WITH('Code-RPG5','Customer-1ewf'), DerivedTable3.Code3Xip,
          DerivedTable4.FeezLpC),
        CASE
          WHEN STARTS_WITH(DerivedTable7.FeeLG30,'Amt-lqU5') THEN
            Table41.Feeg1IH
          WHEN STARTS_WITH(Table41.FeeJhoK,DerivedTable4.CodelvtV) THEN
            'Customer-KJPL'
          WHEN 'Code-FfDy' LIKE DerivedTable6.CountryD7gw THEN
            DerivedTable3.Codeaqvh
          WHEN STARTS_WITH('Customer-SKVp',DerivedTable2.AddressHdnu) THEN
            SourceA.FieldG
          ELSE
            Table42.AccountXdsW
        END)
        AS AccountlYRG,
      IF(
        Table41.CustomercFOz <> Table41.FeegfLn,
        IF(
          STARTS_WITH(DerivedTable3.FeeCUde,'CCY-isFv'),
          DerivedTable6.Addressp4Y0, DerivedTable2.AddresscM17),
        IF(
          DerivedTable3.CustomerLP0M = 'Fee-m4Yv', DerivedTable3.CountrygPvi,
          DerivedTable4.CodelvtV))
        AS Fee7Dmr,
      IF(
        ENDS_WITH(DerivedTable2.AccounttVM1,Table42.CCYSpNc),
        CASE
          WHEN ENDS_WITH(DerivedTable4.CCYY6No,'Customer-Y1Ht') THEN
            'Amt-fh4U'
          WHEN 'Address-e7Cg' != 'Account-zrFO' THEN
            DerivedTable2.AmtNncS
          WHEN 'Fee-2yKb' = 'Fee-EPNf' THEN
            SourceB.FieldA
          WHEN 'Fee-3j9L' = 'CCY-CAij' THEN
            'Country-I6gg'
          WHEN SourceB.FieldH <> Table42.CustomerOjHu THEN
            DerivedTable2.AddresscM17
          ELSE
            'CCY-VumV'
        END, IF(
               ENDS_WITH('Address-H5sl',SourceB.FieldC),
               DerivedTable4.AddressB6KJ, SourceC.FieldH))
        AS Amt0aBb,
      IF(
        DerivedTable4.Country8VtM LIKE 'Customer-YJ4n',
        IF(
          STARTS_WITH('Country-xxNs',SourceD.FieldI), 'Code-mFYy',
          'Account-1kA8'),
        CASE
          WHEN 'Account-bJac' NOT LIKE 'Address-alnR' THEN
            Table41.Feeg1IH
          WHEN STARTS_WITH('Customer-gcfc','Fee-rQ5y') THEN
            'Country-fD9Q'
          ELSE
            'Code-6eUM'
        END)
        AS FeeqDdk,
      IF(
        ENDS_WITH('Customer-iHxZ','Country-Ae8X'),
        IF(
          STARTS_WITH(Table42.AccountiqH3,'Code-AC4E'), SourceB.FieldF,
          'Country-H8iX'),
        CASE
          WHEN ENDS_WITH('Country-0YVR','CCY-HSeZ') THEN
            'CCY-pW0h'
          WHEN STARTS_WITH('CCY-Ib8P','Country-EPxB') THEN
            SourceA.FieldG
          ELSE
            'Customer-TRHB'
        END)
        AS Countryg8so,
      IF(
        ENDS_WITH(DerivedTable6.FeeEU6a,SourceD.FieldD),
        IF(
          'Code-AWBa' LIKE SourceB.FieldB, DerivedTable6.AddressYFBr,
          SourceE.FieldJ),
        CASE
          WHEN DerivedTable3.FeeF6P4 NOT LIKE 'Fee-KOOy' THEN
            'Code-Fdvb'
          WHEN STARTS_WITH('Account-ZRkF','CCY-k7Dk') THEN
            'Fee-Np9q'
          ELSE
            Table42.AddressYMvu
        END)
        AS AddressqybV,
      IF(
        ENDS_WITH('Address-27Zq',DerivedTable6.FeeFMCK),
        IF(
          STARTS_WITH(DerivedTable4.FeexvTE,SourceC.FieldC),
          Table42.Accountrlei, 'Fee-Dv7F'),
        CASE
          WHEN ENDS_WITH(SourceE.FieldE,DerivedTable4.AccountMw3l) THEN
            SourceD.FieldB
          WHEN Table41.CountryjjK4 <> 'CCY-VUSY' THEN
            'Customer-D2zl'
          WHEN DerivedTable7.FeeLG30 LIKE Table41.CustomercFOz THEN
            SourceD.FieldH
          WHEN ENDS_WITH('Country-FQXZ',SourceB.FieldD) THEN
            SourceD.FieldG
          ELSE
            DerivedTable6.Address54VO
        END)
        AS Customerkwxm,
      CASE
        WHEN STARTS_WITH('CCY-7Tbr',Table41.AmtWwmn) THEN
          CASE
            WHEN ENDS_WITH('Account-FoUv','Fee-HnVN') THEN
              Table41.FeeJhoK
            WHEN 'CCY-Ctyl' != 'Amt-A7jL' THEN
              DerivedTable3.FeeCUde
            ELSE
              'Customer-MQ5r'
          END
        WHEN 'Account-HTI9' != 'Country-T60Y' THEN
          IF(
            'Account-1Gsz' = DerivedTable6.Address54VO,
            DerivedTable7.AccountKbFf, 'Code-7tf7')
        WHEN 'Country-CdwX' LIKE 'Account-Q1Go' THEN
          IF(STARTS_WITH('Code-lFjw','Customer-s1N1'),'CCY-jNR5','Fee-s3MF')
        WHEN DerivedTable3.FeeD0Po = DerivedTable7.CountryrPg8 THEN
          IF(
            STARTS_WITH(DerivedTable8.Code6dqw,'Account-BwkM'), Table41.Amty4cx,
            'Address-up5x')
        ELSE
          IF(
            STARTS_WITH(DerivedTable3.CustomereBLI,'Customer-yywh'),
            SourceE.FieldE, 'Code-DKrW')
      END
        AS CCY0sfl,
      IF(
        STARTS_WITH(DerivedTable8.CustomerAce4,DerivedTable2.AmtNncS),
        IF(
          'Country-itez' NOT LIKE DerivedTable2.AccounttVM1, 'Code-PfO8',
          SourceE.FieldC), IF(
                             DerivedTable7.CustomertAao != 'Fee-BYSH',
                             'Code-zlcH', Table41.Codel32i))
        AS AccountRBgk,
      IF(
        'Amt-ipgR' <> DerivedTable4.CustomerCDJF,
        IF(
          STARTS_WITH(DerivedTable6.CustomerCZg3,'Address-xKmP'), 'Code-DKuY',
          'Customer-cbz2'), IF(
                              ENDS_WITH('CCY-y2bz',Table42.Accountrlei),
                              DerivedTable7.Address3l5L, 'Address-8OAB'))
        AS CustomerT2dW,
      IF(
        STARTS_WITH('Customer-YKuH',SourceA.FieldD),
        IF(SourceD.FieldE = 'Fee-gM3E','Fee-gCds',SourceA.FieldH),
        CASE
          WHEN STARTS_WITH('Address-dO7W',Table42.Amt7Jph) THEN
            DerivedTable6.AddressgQNB
          WHEN DerivedTable6.AddressgQNB LIKE 'Address-wmhk' THEN
            'Code-6s67'
          WHEN ENDS_WITH(SourceD.FieldE,'Country-kKs8') THEN
            DerivedTable6.FeeN99q
          WHEN 'Customer-RQOD' NOT LIKE 'Country-kT9h' THEN
            'Country-7Glt'
          WHEN DerivedTable8.CustomerAce4 NOT LIKE 'Fee-BcvU' THEN
            SourceE.FieldF
          ELSE
            'Code-cdXw'
        END)
        AS CCYBPEh,
      IF(
        STARTS_WITH(DerivedTable8.Code6dqw,'Account-5wGF'),
        IF(
          STARTS_WITH(DerivedTable2.Amt885g,'Code-EQFB'), 'Code-Fmdh',
          'Account-Ye7q'), IF(
                             STARTS_WITH(DerivedTable8.Code6dqw,'CCY-9tIZ'),
                             'Code-2Ryc', 'Country-bUpO'))
        AS Countryz4Un,
      IF(
        'Address-0vmE' NOT LIKE 'Code-4bn7',
        CASE
          WHEN ENDS_WITH(SourceB.FieldG,'CCY-BBRC') THEN
            Table41.CountryZozw
          WHEN 'CCY-fTij' LIKE DerivedTable3.CountryoFFA THEN
            DerivedTable6.FeeN99q
          WHEN ENDS_WITH('CCY-PD1O','Account-oDpC') THEN
            'Country-VfDu'
          WHEN Table41.CodeRYLk <> DerivedTable2.Account61kv THEN
            'Address-aPSP'
          ELSE
            DerivedTable6.AddressgQNB
        END, IF(
               ENDS_WITH('Customer-ux2c',DerivedTable3.CustomereBLI),
               DerivedTable3.CCYO742, SourceA.FieldE))
        AS CCY1UMf,
      IF(
        'Fee-gHWh' LIKE DerivedTable6.AddressYFBr,
        IF(
          ENDS_WITH('Customer-gsNF','CCY-EiJN'), DerivedTable3.CountrygPvi,
          'Amt-wavU'), IF(
                         ENDS_WITH('Address-iCmb','Fee-vxry'),
                         DerivedTable4.AddressB6KJ, 'Amt-yjaK'))
        AS CCYp7aC
    FROM
      testdataset.DerivedTable2 AS DerivedTable2
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(DerivedTable2.AmtmorU,SourceA.FieldB) OR
         STARTS_WITH(DerivedTable2.AddressHdnu,DerivedTable2.Account61kv) AND
         ENDS_WITH('Amt-Yh1k','Customer-R2Dh') AND
         SourceA.FieldJ <> DerivedTable2.CodeRziT)
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH('Fee-ro4d','Country-NJOV') OR
         SourceB.FieldE <> SourceA.FieldA AND
         'Code-45C9' != SourceB.FieldF AND ENDS_WITH('CCY-lZVN','Account-Vhzz'))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Address-ofeD',SourceC.FieldH) OR
         ENDS_WITH(SourceB.FieldB,'Fee-vZBG') OR
         STARTS_WITH('Code-UpmU',SourceB.FieldC) OR
         STARTS_WITH('Amt-4iVF',SourceC.FieldD))
      INNER JOIN testdataset.SourceD AS SourceD ON
        ('Country-vXO8' LIKE SourceA.FieldC OR
         ENDS_WITH('Code-QWig',DerivedTable2.Account61kv) OR
         ENDS_WITH(DerivedTable2.AmtmorU,SourceD.FieldA) AND
         ENDS_WITH(DerivedTable2.Address5RWP,'Country-wg5L'))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH('Address-oXxJ',SourceD.FieldI) AND
         SourceA.FieldA = 'CCY-R1Ap' OR
         ENDS_WITH(DerivedTable2.AmtNncS,'Account-99x9') OR
         'Customer-74Br' NOT LIKE SourceA.FieldA)
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH(DerivedTable2.CodeDmva,SourceD.FieldG) AND
         'Amt-FpZe' <> 'Country-7arj' OR
         STARTS_WITH(SourceA.FieldG,SourceE.FieldE) AND
         STARTS_WITH(SourceE.FieldA,DerivedTable3.CountrygPvi))
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (SourceE.FieldB <> 'Country-Xytk' AND
         'Country-JfGU' NOT LIKE SourceD.FieldF AND
         'Fee-TYAt' LIKE 'Account-vWyr' OR
         STARTS_WITH(DerivedTable3.CodeVHWz,'Country-Ld7h'))
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (DerivedTable3.CountryoFFA != SourceD.FieldH AND
         'Fee-Bof6' NOT LIKE SourceC.FieldD OR
         STARTS_WITH(DerivedTable6.CountryD7gw,'Fee-vkgm') AND
         SourceD.FieldG NOT LIKE DerivedTable4.Country5f1u)
      INNER JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        (SourceA.FieldC NOT LIKE 'Amt-3qFx' OR
         SourceB.FieldF != DerivedTable3.FeeD0Po OR
         ENDS_WITH(DerivedTable2.AddresscM17,'Account-Ictc') AND
         STARTS_WITH('Fee-PPg3',SourceB.FieldI))
      LEFT JOIN testdataset.DerivedTable8 AS DerivedTable8 ON
        (ENDS_WITH(SourceD.FieldC,'Customer-mqzv') AND
         DerivedTable8.Amti3Vx <> DerivedTable6.AddressWejH AND
         SourceC.FieldG = SourceE.FieldB OR 'Country-HUO1' <> SourceE.FieldI)
      LEFT JOIN Table41 AS Table41 ON
        (STARTS_WITH(SourceE.FieldI,'Account-W2vj') OR
         STARTS_WITH(DerivedTable6.Fee8TJM,DerivedTable7.Amt9aae) OR
         SourceB.FieldD <> DerivedTable3.FeeD0Po AND
         ENDS_WITH(SourceB.FieldD,'CCY-XPGe'))
      LEFT JOIN Table42 AS Table42 ON
        (DerivedTable3.CodeVHWz NOT LIKE 'CCY-guH6' AND
         ENDS_WITH('Customer-osd1',SourceD.FieldE) OR
         SourceC.FieldA = 'Country-hEOb' AND
         'Address-5DLw' != DerivedTable4.CodelvtV)
    WHERE
      'Account-QQAp' != Table41.Amty4cx AND 'CCY-uQwa' = Table41.CustomercFOz OR
      STARTS_WITH(DerivedTable3.FeeqkjX,SourceC.FieldJ) AND
      'Address-vwh0' LIKE DerivedTable4.CodeJM19 AND
      ENDS_WITH('Code-XOqE',DerivedTable7.Codeuiec) AND
      'Country-Y316' <> DerivedTable6.AddressYFBr AND
      STARTS_WITH(SourceE.FieldJ,SourceA.FieldB) AND
      Table41.FeevP7V != DerivedTable4.CodelvtV
  ),
  Table45 AS (
    SELECT
      CASE
        WHEN STARTS_WITH('Fee-kg8b','Fee-XVoN') THEN
          CASE
            WHEN ENDS_WITH(DerivedTable2.Account61kv,'Customer-HkEc') THEN
              'Customer-YA7m'
            WHEN 'Amt-rzRP' <> SourceB.FieldJ THEN
              'Customer-P26L'
            WHEN SourceE.FieldG = 'CCY-TyHD' THEN
              DerivedTable6.AddressWejH
            WHEN ENDS_WITH('Country-9EQB',DerivedTable6.FeeEU6a) THEN
              'Amt-EDn3'
            WHEN ENDS_WITH(SourceE.FieldD,'Amt-tu4P') THEN
              Table42.Accountrlei
            ELSE
              SourceB.FieldC
          END
        WHEN 'Account-qZe9' NOT LIKE 'CCY-1F3V' THEN
          IF(
            ENDS_WITH(DerivedTable7.CustomertAao,DerivedTable5.CCYVD4h),
            SourceB.FieldJ, Table42.AmtKesM)
        WHEN Table42.CCYeGiQ <> SourceA.FieldC THEN
          IF(
            ENDS_WITH(SourceC.FieldC,'Account-oNZG'), 'Code-w1a3',
            SourceE.FieldC)
        WHEN 'CCY-Xq5T' NOT LIKE DerivedTable8.FeePN5q THEN
          IF(
            STARTS_WITH('Address-OmID',DerivedTable4.Addressm87D),
            DerivedTable8.Amti3Vx, 'Country-kv9z')
        ELSE
          CASE
            WHEN STARTS_WITH(Table41.CodeRYLk,DerivedTable7.FeeLG30) THEN
              'Code-Ce5K'
            WHEN STARTS_WITH(SourceB.FieldA,SourceA.FieldF) THEN
              DerivedTable8.Amti3Vx
            WHEN 'Country-ASlz' LIKE 'Address-aZiW' THEN
              Table42.CustomerOjHu
            WHEN DerivedTable8.FeePN5q LIKE 'Code-tGdp' THEN
              SourceE.FieldD
            ELSE
              Table41.AccountrJ2s
          END
      END
        AS CCYJizI,
      CASE
        WHEN Table41.CountryZozw != DerivedTable5.Customera0qh THEN
          IF(
            ENDS_WITH(DerivedTable2.CountrywaWi,DerivedTable6.AddressYFBr),
            DerivedTable5.Feepmor, DerivedTable6.Fee8TJM)
        WHEN STARTS_WITH('Country-uoHg',DerivedTable7.CountryrPg8) THEN
          IF(
            'Customer-s5dx' != 'Code-Flu7', 'Address-GHpy',
            DerivedTable4.CustomerCDJF)
        WHEN 'Account-zmug' = 'Code-DVMc' THEN
          IF(
            DerivedTable2.Amtp4ee NOT LIKE 'Customer-ECks',
            DerivedTable4.AddressB6KJ, 'Country-aoG1')
        WHEN 'Code-nM8b' = 'Amt-9S0C' THEN
          IF(
            STARTS_WITH(DerivedTable4.AddressB6KJ,'Fee-17gh'),
            DerivedTable7.Amt3faU, DerivedTable2.AccountrJZf)
        WHEN ENDS_WITH(DerivedTable6.AddressgQNB,SourceC.FieldJ) THEN
          IF(
            DerivedTable6.Fee8TJM = DerivedTable4.CodeXHEv,
            DerivedTable8.CustomerAce4, 'Amt-vbB7')
        ELSE
          IF(SourceE.FieldA LIKE SourceB.FieldI,'Customer-1Zkr',Table42.CCYeGiQ)
      END
        AS AddressoN83,
      IF(
        'Country-7dZm' LIKE SourceB.FieldE,
        IF(
          SourceA.FieldC <> 'Account-i4gM', DerivedTable4.CCYY6No,
          DerivedTable4.CCYY6No),
        IF(
          ENDS_WITH(DerivedTable6.CCYp8wn,DerivedTable5.CountrynpDj),
          'Address-C6es', 'CCY-EVrZ'))
        AS CountryMklt,
      IF(
        'Fee-Xb1k' = 'Code-KLVL',
        IF(DerivedTable2.CodeRziT = 'CCY-ZYrs','Address-dtiY','Amt-SRZt'),
        IF(
          ENDS_WITH('Account-kxg0','Customer-yJS9'), 'Account-AId3',
          Table41.AmtpIj4))
        AS CodeWCdr,
      CASE
        WHEN 'Country-uuOi' != DerivedTable6.Addressp4Y0 THEN
          IF(
            ENDS_WITH('Customer-mSXR',SourceA.FieldC), SourceD.FieldJ,
            'Amt-wbhU')
        WHEN ENDS_WITH('Code-9tCn','Account-y29d') THEN
          IF(
            ENDS_WITH(SourceD.FieldG,'Account-gJs4'), SourceE.FieldD,
            SourceE.FieldE)
        WHEN 'Fee-lspk' = DerivedTable7.Codeuiec THEN
          IF(
            DerivedTable7.AccountKbFf != 'Code-C8wS', DerivedTable7.Amt9aae,
            'CCY-EdJJ')
        ELSE
          CASE
            WHEN STARTS_WITH(DerivedTable4.FeexvTE,'Code-1P9E') THEN
              DerivedTable8.Amti3Vx
            WHEN ENDS_WITH(SourceD.FieldF,'Amt-CttT') THEN
              'Country-83Ih'
            ELSE
              DerivedTable7.AddressuSTK
          END
      END
        AS CodeIwHc,
      IF(
        ENDS_WITH('Country-E9WF','Fee-j7ED'),
        IF(
          'Country-V2Fz' = Table42.Codev68w, SourceA.FieldF,
          DerivedTable4.CodeXHEv),
        IF(
          ENDS_WITH(DerivedTable4.CodelvtV,'Customer-pitF'), SourceE.FieldD,
          SourceA.FieldB))
        AS Accountx0V0,
      IF(
        ENDS_WITH('Country-5tEl','Amt-etAN'),
        CASE
          WHEN STARTS_WITH(DerivedTable6.CCYp8wn,SourceB.FieldD) THEN
            'Fee-G1IT'
          WHEN Table41.AmtBAXR <> SourceA.FieldB THEN
            DerivedTable4.FeexvTE
          WHEN STARTS_WITH(Table41.CountryjjK4,DerivedTable8.CountryKrJm) THEN
            'Country-ZQA6'
          ELSE
            'CCY-rdD8'
        END, IF(
               STARTS_WITH(SourceA.FieldJ,DerivedTable2.AccountctCy),
               'Account-CuQu', SourceE.FieldH))
        AS AmtnT5r,
      IF(
        ENDS_WITH(DerivedTable2.AccountrJZf,'Address-Ua75'),
        IF('Account-iVUM' LIKE SourceB.FieldJ,SourceD.FieldC,'Account-qXpT'),
        IF(
          STARTS_WITH(Table41.AccountrJ2s,SourceE.FieldG), 'Address-VrkV',
          'Customer-ZAuB'))
        AS AmtSEui,
      IF(
        DerivedTable5.CCYVD4h LIKE SourceE.FieldD,
        IF(
          ENDS_WITH(DerivedTable4.Country8VtM,DerivedTable2.AmtmorU),
          'Country-bONe', 'Customer-uZN1'),
        IF('Customer-Afey' NOT LIKE 'Amt-qKKD','CCY-Wk9Q','Customer-sQlI'))
        AS CodekhtA,
      IF(
        STARTS_WITH('Account-Qd4b',SourceD.FieldI),
        IF(SourceB.FieldI != 'Address-Ynnq','Code-8dBB',SourceC.FieldE),
        CASE
          WHEN STARTS_WITH('Amt-4Kev','Country-XzJB') THEN
            'CCY-BNQK'
          WHEN ENDS_WITH(DerivedTable2.CodeDmva,Table41.FeesrLs) THEN
            DerivedTable7.AddressuSTK
          WHEN 'Account-ph1T' NOT LIKE 'Customer-NSlZ' THEN
            DerivedTable8.CustomerCtTz
          WHEN DerivedTable5.AddressV4AX NOT LIKE SourceD.FieldI THEN
            DerivedTable8.AmtSXKP
          ELSE
            'Account-wBdn'
        END)
        AS AmtHnwJ,
      IF(
        STARTS_WITH(DerivedTable8.CCYeu0i,'Account-QbG5'),
        IF('Country-I7rV' LIKE 'Amt-hrCe','Amt-ZRYN','Account-ULwL'),
        CASE
          WHEN SourceB.FieldH = 'CCY-FJqz' THEN
            DerivedTable6.CCYp8wn
          WHEN STARTS_WITH(DerivedTable7.FeeBS3r,DerivedTable7.AddressuSTK) THEN
            'Customer-CYNb'
          ELSE
            'Country-nvEx'
        END)
        AS Customer5aJ6,
      IF(
        'Account-E4vA' NOT LIKE 'Amt-jBXa',
        IF(
          STARTS_WITH('Country-lRyI','Country-j5hF'), 'Account-ZsNq',
          'Amt-nPEv'),
        CASE
          WHEN DerivedTable7.AddressuSTK LIKE 'Amt-1V2P' THEN
            'Account-FSHc'
          WHEN ENDS_WITH('CCY-p74O','Code-wxMD') THEN
            'Fee-HuPW'
          WHEN ENDS_WITH(Table42.CodeH2Z2,'Country-9D34') THEN
            SourceA.FieldE
          WHEN SourceB.FieldE <> DerivedTable4.Country5f1u THEN
            Table42.Accountrlei
          ELSE
            'Code-Bd5x'
        END)
        AS AddressUZZW,
      CASE
        WHEN DerivedTable6.AmtCCH1 <> 'Amt-p1jq' THEN
          IF(
            SourceE.FieldD NOT LIKE 'Account-4417', DerivedTable8.AmtSXKP,
            DerivedTable5.FeeogF5)
        WHEN DerivedTable5.AddressV4AX LIKE DerivedTable2.Amt885g THEN
          IF(
            ENDS_WITH('CCY-OMv4',DerivedTable4.FeezLpC), SourceE.FieldC,
            'Account-M8Z0')
        WHEN 'Customer-SoPk' = Table42.CustomerOjHu THEN
          IF(
            STARTS_WITH(SourceE.FieldI,'Account-S4ts'), 'Customer-WwRs',
            'Amt-A98K')
        ELSE
          CASE
            WHEN DerivedTable6.AddressgQNB <> DerivedTable6.Address54VO THEN
              DerivedTable5.CCY76AX
            WHEN ENDS_WITH('Code-pTGV','Customer-c2v3') THEN
              DerivedTable4.FeexvTE
            WHEN STARTS_WITH('Fee-MqyO','Amt-MEs1') THEN
              'CCY-E06S'
            WHEN ENDS_WITH(SourceA.FieldE,'Address-xEMT') THEN
              'Code-VXeZ'
            ELSE
              DerivedTable7.AddressgmSt
          END
      END
        AS CountryYQ2V,
      CASE
        WHEN 'Amt-3Ham' <> 'Customer-YZNR' THEN
          CASE
            WHEN SourceC.FieldH LIKE SourceE.FieldF THEN
              DerivedTable6.CountryD7gw
            WHEN STARTS_WITH('Fee-29wH','Fee-czPm') THEN
              'Code-Um06'
            WHEN STARTS_WITH(DerivedTable4.FeesrBh,Table42.CodeH2Z2) THEN
              'Account-jSZi'
            WHEN 'Account-O3Iu' NOT LIKE 'CCY-aEIH' THEN
              'Account-cIgx'
            WHEN SourceD.FieldI NOT LIKE DerivedTable6.AddressPpXC THEN
              'Fee-gW4S'
            ELSE
              SourceD.FieldA
          END
        WHEN ENDS_WITH(DerivedTable4.CustomerCDJF,Table41.FeegfLn) THEN
          CASE
            WHEN STARTS_WITH(Table42.AccountXdsW,'CCY-B0bh') THEN
              'Code-0nR1'
            WHEN SourceC.FieldF != DerivedTable4.CustomerCDJF THEN
              SourceA.FieldI
            ELSE
              DerivedTable8.Code6dqw
          END
        WHEN 'Account-5spQ' <> 'Customer-vM9n' THEN
          IF(
            SourceB.FieldC NOT LIKE DerivedTable2.Amt885g, Table41.AmtPqvp,
            SourceB.FieldE)
        WHEN ENDS_WITH(SourceA.FieldH,Table42.AddressYMvu) THEN
          CASE
            WHEN 'Amt-uG8T' LIKE Table41.AmtpIj4 THEN
              Table41.Amty4cx
            WHEN 'Country-Osry' NOT LIKE SourceE.FieldC THEN
              SourceA.FieldE
            ELSE
              SourceB.FieldC
          END
        ELSE
          IF(
            SourceE.FieldJ LIKE 'Address-VeUv', DerivedTable8.AmtSXKP,
            'Account-hgaG')
      END
        AS CodedFdV,
      IF(
        STARTS_WITH('Customer-hgBa',DerivedTable8.CustomerAce4),
        CASE
          WHEN 'Fee-LbBU' <> 'Customer-f4s6' THEN
            Table42.Codev68w
          WHEN 'Fee-r5Yo' NOT LIKE Table41.FeegfLn THEN
            DerivedTable4.Country5f1u
          ELSE
            SourceB.FieldG
        END, IF(
               STARTS_WITH('Customer-VbwT','Address-EcCL'), 'Fee-UgRq',
               Table41.CustomergHH2))
        AS Customer9QLI,
      IF(
        'CCY-cvFQ' NOT LIKE 'Code-Zje7',
        IF(SourceE.FieldH LIKE 'Address-cnr2',Table42.CCY5qHh,'Country-n0iZ'),
        IF(
          STARTS_WITH('Code-y7q3',DerivedTable7.Amt9aae), 'Address-rLlI',
          DerivedTable4.FeesrBh))
        AS CustomerRZCz
    FROM
      testdataset.SourceA AS SourceA
      INNER JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('CCY-jJYD',SourceA.FieldA) OR
         SourceA.FieldH LIKE 'Code-I2Rd' OR
         STARTS_WITH('Address-QozK',SourceB.FieldF) AND
         ENDS_WITH('Fee-Z1DO','Code-By4u'))
      INNER JOIN testdataset.SourceC AS SourceC ON
        ('Account-StGW' != 'CCY-cUMW' AND SourceB.FieldA <> 'Fee-5CUV' OR
         'Account-ngQH' != 'Account-1MYC' OR
         'Account-Si4R' NOT LIKE 'Country-v7Ty')
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(SourceD.FieldH,SourceD.FieldH) AND
         SourceA.FieldF = SourceD.FieldE OR
         ENDS_WITH('Code-HBxi',SourceD.FieldI) OR
         STARTS_WITH('CCY-6qAi',SourceD.FieldH))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (SourceE.FieldE != SourceC.FieldD AND 'Fee-76DM' = SourceA.FieldJ AND
         ENDS_WITH(SourceE.FieldF,'Amt-w1Io') OR
         STARTS_WITH('Fee-TXXV','CCY-nIum'))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (ENDS_WITH('Customer-0tXN','Fee-Xu5m') OR
         SourceC.FieldA <> SourceB.FieldD OR
         'Address-h2H2' NOT LIKE SourceC.FieldD OR
         SourceB.FieldI LIKE 'Customer-8gO3')
      LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH(SourceD.FieldB,'Address-WkhC') AND
         DerivedTable2.CountrywaWi <> DerivedTable2.AccounttVM1 OR
         STARTS_WITH(DerivedTable4.FeezLpC,DerivedTable4.AccounteutQ) AND
         'Country-jVZ7' NOT LIKE 'Fee-minp')
      LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (ENDS_WITH(SourceA.FieldA,'Amt-P14i') AND
         ENDS_WITH('Account-1zRM',SourceA.FieldB) OR
         STARTS_WITH('Account-x4zn',DerivedTable2.AmtmorU) AND
         ENDS_WITH(DerivedTable2.Address5RWP,DerivedTable2.AddressHdnu))
      INNER JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (STARTS_WITH(SourceB.FieldJ,'CCY-kiIr') OR
         STARTS_WITH('Amt-Rbc4',DerivedTable6.CustomerCZg3) AND
         'Country-Z8cG' NOT LIKE 'Customer-3PRj' OR
         STARTS_WITH(DerivedTable4.FeezLpC,SourceC.FieldE))
      LEFT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        ('Country-NT4a' != DerivedTable4.Addressm87D OR
         STARTS_WITH(SourceA.FieldH,DerivedTable4.FeesrBh) OR
         DerivedTable2.AccounttVM1 NOT LIKE DerivedTable6.FeeN99q OR
         ENDS_WITH('Customer-9xms',SourceB.FieldC))
      INNER JOIN testdataset.DerivedTable8 AS DerivedTable8 ON
        (ENDS_WITH(SourceE.FieldD,SourceE.FieldA) AND
         DerivedTable4.FeexvTE <> SourceC.FieldC AND
         STARTS_WITH(SourceB.FieldC,'Code-JoCd') OR
         ENDS_WITH(DerivedTable8.Account3V3K,DerivedTable7.Amt3faU))
      LEFT JOIN Table41 AS Table41 ON
        ('Address-0V73' != SourceE.FieldG AND
         'Address-vAhm' NOT LIKE SourceB.FieldF OR
         STARTS_WITH(DerivedTable2.AmtmorU,DerivedTable5.Customera0qh) OR
         'Customer-nImT' <> 'Amt-j0wl')
      LEFT JOIN Table42 AS Table42 ON
        (ENDS_WITH('Customer-URXm','CCY-30mT') AND
         ENDS_WITH('Country-q9mM','Address-aQqt') AND
         DerivedTable7.CountryrPg8 != 'Fee-YOsH' AND
         STARTS_WITH(SourceB.FieldE,'Country-UBEl'))
    WHERE
      STARTS_WITH(SourceB.FieldI,'Account-HeFN') OR
      DerivedTable8.AmtSXKP <> 'Customer-Oe4k' AND
      'Address-Ybtp' <> 'CCY-QLPQ' AND ENDS_WITH(SourceB.FieldF,'Code-ctL5') AND
      'Amt-kNWL' NOT LIKE 'Country-WGlB' AND 'Code-mHOr' NOT LIKE 'Fee-kME0' AND
      'CCY-XvW0' NOT LIKE 'Fee-HciK' AND
      STARTS_WITH(DerivedTable7.CodeOp8k,DerivedTable5.CodePGRC)
  )
SELECT
  IF(
    'Address-zkw6' NOT LIKE DerivedTable1.CustomerKrwR,
    IF(ENDS_WITH(SourceA.FieldD,SourceC.FieldH),'CCY-GrDa','Account-uSDA'),
    CASE
      WHEN 'Address-RARF' LIKE DerivedTable7.CCYZD20 THEN
        'CCY-boZF'
      WHEN 'Amt-wzvn' NOT LIKE SourceE.FieldB THEN
        SourceC.FieldC
      WHEN DerivedTable1.AccountmNQo LIKE 'Code-tdYq' THEN
        Table44.Countryg8so
      ELSE
        Table42.Codev68w
    END)
    AS Account0k3h,
  IF(
    STARTS_WITH('Code-uew3','Customer-iWNK'),
    IF(
      DerivedTable1.Codeuhr9 != DerivedTable8.Country2xzz,
      DerivedTable4.CodelvtV, Table45.CodekhtA),
    CASE
      WHEN 'Address-h1as' = 'CCY-64qT' THEN
        'Fee-wUzY'
      WHEN 'Customer-tojV' LIKE 'Country-fYJX' THEN
        Table45.CodekhtA
      ELSE
        Table45.CodeIwHc
    END)
    AS AddressytfD,
  CASE
    WHEN 'Customer-OR30' <> 'Fee-tSC6' THEN
      IF(STARTS_WITH('Amt-24pj',SourceE.FieldB),'Customer-iW4X','Country-b6UH')
    WHEN 'Customer-h3vs' LIKE 'CCY-336y' THEN
      CASE
        WHEN STARTS_WITH('Customer-aNca','Customer-YCEr') THEN
          Table41.FeegfLn
        WHEN DerivedTable5.CCYVD4h = Table44.Customerkwxm THEN
          'Customer-e6BF'
        WHEN STARTS_WITH('Address-NIME','Address-Q6wP') THEN
          'Country-KarN'
        WHEN 'Address-eMZt' != 'Fee-UMH1' THEN
          'Account-bXQt'
        WHEN 'Customer-O6eX' NOT LIKE Table43.AmtBV6V THEN
          'Customer-3wfm'
        ELSE
          'Customer-3gtw'
      END
    ELSE
      CASE
        WHEN STARTS_WITH('Account-v6KZ','Address-Ot3Q') THEN
          DerivedTable7.Amt3faU
        WHEN ENDS_WITH('Code-vTKg',DerivedTable7.CodeOp8k) THEN
          Table45.CountryYQ2V
        WHEN ENDS_WITH(Table41.AmtpIj4,'Code-8R9h') THEN
          DerivedTable8.Country2xzz
        WHEN Table41.FeeJhoK LIKE Table43.AmtEJG4 THEN
          Table41.Countryb30V
        ELSE
          'Code-ifYl'
      END
  END
    AS Addressa13E,
  IF(
    'Code-eJHV' != Table45.CodedFdV,
    CASE
      WHEN Table41.Feeg1IH <> 'Fee-PZRN' THEN
        Table44.CCY1UMf
      WHEN 'Customer-z96l' NOT LIKE Table43.Code19oe THEN
        DerivedTable8.FeePN5q
      ELSE
        'Country-inc9'
    END, IF(
           ENDS_WITH(Table43.CountryvFTP,DerivedTable4.Country5f1u),
           'Account-15Zs', SourceE.FieldF))
    AS CountryFgI6,
  IF(
    ENDS_WITH(SourceA.FieldD,'Country-MetQ'),
    IF('Address-KHy6' <> 'Country-QthB','Code-ZFT8','Customer-KAqS'),
    IF(
      ENDS_WITH(DerivedTable7.Amt3faU,'Account-Rrxi'), 'Account-1D5x',
      'CCY-bncH'))
    AS Feep9G7,
  IF(
    'Account-FCtv' <> DerivedTable7.FeeLG30,
    IF(
      Table45.Customer9QLI NOT LIKE SourceC.FieldE, DerivedTable4.Country5f1u,
      DerivedTable7.Amt9aae),
    IF(
      Table44.AddressqybV NOT LIKE Table43.Codei12M, DerivedTable1.FeerBIS,
      SourceB.FieldG))
    AS CCYWBFC,
  IF(
    'CCY-8jmR' = 'Country-knR2',
    IF(SourceE.FieldA NOT LIKE Table41.AmtBAXR,SourceC.FieldE,SourceB.FieldD),
    CASE
      WHEN STARTS_WITH('Country-PLr9',Table42.CustomerOjHu) THEN
        SourceB.FieldG
      WHEN Table45.Customer9QLI != Table41.AddressTwoR THEN
        'Account-4Gwd'
      ELSE
        Table44.CCY1UMf
    END)
    AS AccountuwuN,
  IF(
    ENDS_WITH('Amt-bmpb',SourceA.FieldI),
    IF(
      STARTS_WITH('Country-nBBR',DerivedTable8.Country2xzz),
      Table42.Accountrlei, 'Country-TdHg'),
    IF('Address-pqj4' != 'CCY-UpmC',SourceE.FieldH,'CCY-4AWK'))
    AS AmtoKfh,
  IF(
    ENDS_WITH('Country-AuGR',SourceA.FieldC),
    IF(
      ENDS_WITH(Table43.AmtqYKj,'CCY-GMsK'), Table45.Customer5aJ6,
      'Customer-H3p5'), IF(
                          STARTS_WITH(Table42.CCYSpNc,'CCY-eQ87'),
                          Table44.AddressqybV, 'Address-hR4k'))
    AS AmtEPPv,
  IF(
    STARTS_WITH(SourceD.FieldE,'Fee-ybof'),
    CASE
      WHEN Table43.CountryvFTP <> 'Account-464E' THEN
        'Fee-tGg7'
      WHEN DerivedTable4.FeezLpC = 'Amt-u8r8' THEN
        Table45.CodeWCdr
      ELSE
        DerivedTable7.CCYZD20
    END, IF(
           ENDS_WITH(SourceD.FieldC,Table43.AddressW27w), 'Account-71cr',
           SourceB.FieldC))
    AS Customer1EZM,
  IF(
    ENDS_WITH(SourceE.FieldB,DerivedTable4.FeesrBh),
    IF('Amt-BmkD' = 'Amt-YN7P','Account-YPTI',DerivedTable1.Codeuhr9),
    IF(
      STARTS_WITH(Table41.FeevP7V,'Customer-VBp1'), Table44.Countryg8so,
      'Customer-22oV'))
    AS CCYwXVu,
  IF(
    'Amt-A59P' NOT LIKE DerivedTable1.CustomerWrlz,
    IF(
      STARTS_WITH(DerivedTable1.AccountKwSP,Table41.AmtPqvp),
      DerivedTable5.CodePGRC, 'Customer-BQDO'),
    CASE
      WHEN Table45.CustomerRZCz NOT LIKE Table43.AmtqYKj THEN
        'Fee-l4Rv'
      WHEN STARTS_WITH('Customer-Y06m','Amt-0wUb') THEN
        Table42.AccountiqH3
      WHEN ENDS_WITH('Account-4JyD',DerivedTable1.CountryV1io) THEN
        'Address-6Zej'
      WHEN ENDS_WITH(DerivedTable7.AddressgmSt,'CCY-t3HQ') THEN
        'CCY-7AiP'
      ELSE
        'Address-sfFP'
    END)
    AS Country3Ui7,
  IF(
    'Customer-grN8' = 'Amt-gH5m',
    IF(ENDS_WITH(Table43.Codei12M,'Country-23W6'),'Amt-qg66','Country-Xzfi'),
    IF(
      STARTS_WITH(Table41.CustomergHH2,Table43.AmtBV6V), 'Customer-07ov',
      'Account-TQo1'))
    AS AmtMlgZ,
  IF(
    Table41.CountryZozw LIKE 'Fee-mc9Q',
    CASE
      WHEN ENDS_WITH(DerivedTable4.CCYY6No,DerivedTable4.AccounteutQ) THEN
        SourceD.FieldG
      WHEN 'Fee-IdJG' <> 'Fee-9c1v' THEN
        'Customer-ra9A'
      ELSE
        DerivedTable4.AccountMw3l
    END, IF(
           STARTS_WITH(DerivedTable8.CustomerAce4,'Code-Y64W'), 'CCY-KjH7',
           DerivedTable7.FeeBS3r))
    AS Country3N9I
FROM
  testdataset.SourceD AS SourceD
  RIGHT JOIN testdataset.SourceA AS SourceA ON
    (SourceA.FieldF != 'Code-emOC' AND
     STARTS_WITH(SourceA.FieldI,SourceA.FieldH) AND
     SourceD.FieldD LIKE SourceA.FieldB AND
     STARTS_WITH(SourceD.FieldB,'Account-a5wI'))
  LEFT JOIN testdataset.SourceB AS SourceB ON
    (SourceB.FieldH != 'Amt-oJ7g' OR 'Customer-xWk1' = SourceA.FieldF OR
     'Country-juFq' != 'Country-4xVc' AND SourceD.FieldH <> SourceA.FieldJ)
  INNER JOIN testdataset.SourceC AS SourceC ON
    (SourceD.FieldD != 'Address-GPm5' AND SourceD.FieldJ <> 'CCY-XVy1' AND
     'Amt-6Klf' <> 'Address-ICls' AND 'Code-zyMg' = 'Code-QPbJ')
  RIGHT JOIN testdataset.SourceE AS SourceE ON
    (STARTS_WITH('Account-kH4v','Amt-qEVD') OR
     ENDS_WITH(SourceD.FieldE,'Customer-b9BI') OR
     STARTS_WITH(SourceE.FieldE,'Customer-DYH4') OR
     'Account-N7xM' NOT LIKE 'Account-xLMm')
  LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
    (SourceD.FieldI NOT LIKE 'CCY-g7Zt' AND
     STARTS_WITH(SourceE.FieldC,'Address-WCHE') OR
     'Amt-ODXM' LIKE 'Account-Mur5' OR STARTS_WITH('Code-DH8x','Code-8ljt'))
  INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
    (STARTS_WITH('Fee-L1Ha',SourceD.FieldE) OR
     ENDS_WITH(SourceC.FieldB,DerivedTable4.CodelvtV) OR
     SourceD.FieldE <> 'Account-ixpl' OR
     ENDS_WITH(DerivedTable4.AccounteutQ,'Country-ecQA'))
  LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
    (ENDS_WITH('CCY-Sdje','Country-SXaG') OR SourceD.FieldA != SourceD.FieldC OR
     'Fee-XKVZ' <> 'Code-wsuP' AND 'Fee-GRxt' != 'CCY-l4Gm')
  LEFT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
    ('Fee-AhF8' <> 'Customer-3AZJ' AND SourceE.FieldG LIKE SourceD.FieldA OR
     'Customer-tJfZ' = DerivedTable1.CustomerWrlz OR
     SourceE.FieldB LIKE SourceE.FieldA)
  INNER JOIN testdataset.DerivedTable8 AS DerivedTable8 ON
    (SourceC.FieldG NOT LIKE 'Account-KgD2' OR
     ENDS_WITH('Fee-1rEV','Code-RqwL') OR
     DerivedTable5.AddressV4AX LIKE DerivedTable8.Amti3Vx AND
     STARTS_WITH(SourceE.FieldA,DerivedTable8.CustomerAce4))
  LEFT JOIN Table41 AS Table41 ON
    (DerivedTable1.AmtzEYq <> Table41.CustomercFOz AND
     STARTS_WITH('Customer-Mpf5','Code-hVcC') AND
     STARTS_WITH('Customer-pNOa','Country-Jt4g') AND
     STARTS_WITH(DerivedTable4.FeexvTE,'Account-PckE'))
  RIGHT JOIN Table42 AS Table42 ON
    (ENDS_WITH(SourceB.FieldJ,'Account-chql') AND
     SourceB.FieldB <> 'Account-LFtY' AND
     DerivedTable7.FeeLG30 NOT LIKE Table42.CustomerOjHu AND
     ENDS_WITH('Code-fmxK',DerivedTable8.CustomerN13F))
  RIGHT JOIN Table43 AS Table43 ON
    ('Customer-qr2C' <> DerivedTable5.CCYVD4h OR
     SourceE.FieldJ = Table41.AmtWwmn AND
     DerivedTable8.CustomerZmFX = 'Account-ryht' AND
     SourceD.FieldG NOT LIKE 'Code-mKep')
  RIGHT JOIN Table44 AS Table44 ON
    (Table44.AccountRBgk NOT LIKE 'Fee-Z3hk' AND
     STARTS_WITH(SourceB.FieldD,'Account-9zVO') AND
     SourceD.FieldD != DerivedTable1.Addressrgjd AND
     'Country-Fho3' LIKE 'Amt-ayxM')
  INNER JOIN Table45 AS Table45 ON
    ('Amt-QwGy' = DerivedTable1.CCYXSkS AND
     'Country-1ceX' NOT LIKE Table45.CodeWCdr AND
     Table45.AmtSEui != DerivedTable7.Codeuiec AND
     ENDS_WITH('Code-cUt8','CCY-iyQ9'))
WHERE
  Table41.FeesrLs <> SourceD.FieldI OR ENDS_WITH('Fee-bN1i','Fee-2Fqa') OR
  ENDS_WITH(SourceB.FieldJ,DerivedTable7.Amt3faU) OR
  SourceE.FieldJ <> SourceD.FieldI OR
  Table41.AmtPqvp = 'Account-TJbK' OR
  STARTS_WITH(SourceA.FieldA,Table42.CCY5qHh) OR
  STARTS_WITH(Table44.CCY1UMf,SourceD.FieldI) OR
  Table44.CountryLvFX <> DerivedTable1.AccountfEJK