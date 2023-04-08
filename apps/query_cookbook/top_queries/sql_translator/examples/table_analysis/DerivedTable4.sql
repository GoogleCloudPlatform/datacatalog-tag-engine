CREATE OR REPLACE TABLE testdataset.DerivedTable4 AS
WITH
  Table16 AS (
    SELECT
      IF(
        STARTS_WITH('Amt-H3Kk','Address-3AOG'),
        IF(SourceD.FieldH = SourceA.FieldH,SourceE.FieldG,'Code-tSIB'),
        IF(
          ENDS_WITH(DerivedTable2.CodeDmva,'Code-VqNP'), SourceC.FieldA,
          DerivedTable2.AmtmorU))
        AS AddressFAql,
      IF(
        'Amt-BYgL' <> 'Account-3rkj',
        CASE
          WHEN SourceD.FieldC <> DerivedTable2.AddressHdnu THEN
            SourceC.FieldJ
          WHEN ENDS_WITH('Address-rz5N','Code-VmQO') THEN
            DerivedTable1.AmtR86V
          WHEN 'Fee-osPN' = SourceC.FieldB THEN
            SourceE.FieldB
          ELSE
            DerivedTable1.FeerBIS
        END, IF(
               ENDS_WITH('CCY-MQkE','Customer-fuhr'), 'Address-cfqV',
               DerivedTable1.AccountmNQo))
        AS Amt6J7p,
      IF(
        'Account-7OhQ' LIKE 'Account-RNMr',
        IF('CCY-hqqG' != 'Country-WHqh','Country-oM19',SourceC.FieldG),
        IF(
          STARTS_WITH(SourceA.FieldJ,'Country-TC6t'), SourceC.FieldG,
          'Customer-y7tF'))
        AS Code5tEA,
      IF(
        ENDS_WITH(DerivedTable2.AmtmorU,SourceE.FieldD),
        IF(
          'Address-SfXO' NOT LIKE DerivedTable1.AccountKwSP, SourceA.FieldB,
          'Account-1Mb1'), IF(
                             STARTS_WITH('Fee-lTsl','Amt-96Oy'),
                             'Customer-nzk0', DerivedTable2.AmtNncS))
        AS CountryiYbf,
      IF(
        DerivedTable1.AmtzEYq LIKE 'Account-yyIB',
        IF(DerivedTable1.CCYXSkS = 'Fee-OcSU','CCY-mAoV',SourceC.FieldG),
        IF(STARTS_WITH('Amt-D4le','Account-KUlG'),'Amt-QnqN','Address-49z5'))
        AS AddressjCYy,
      IF(
        DerivedTable2.Address7oYI NOT LIKE SourceE.FieldJ,
        CASE
          WHEN SourceC.FieldI <> DerivedTable1.AccountqcVc THEN
            'CCY-XDaq'
          WHEN 'Fee-wEJA' LIKE SourceA.FieldA THEN
            'Amt-LYZQ'
          WHEN ENDS_WITH(DerivedTable2.AccounttVM1,'Country-s32A') THEN
            SourceD.FieldE
          ELSE
            SourceC.FieldI
        END, IF(
               STARTS_WITH(SourceD.FieldD,DerivedTable1.AccountKwSP),
               SourceA.FieldC, SourceC.FieldJ))
        AS CountryI2wJ,
      IF(
        SourceE.FieldG != 'CCY-9j9l',
        IF(
          STARTS_WITH('Country-rJjr','Country-2wxB'), DerivedTable1.Addressrgjd,
          'Code-NnaG'), IF(
                          SourceA.FieldE = DerivedTable1.Codeuhr9,
                          'Country-OETU', 'Country-jDJC'))
        AS CustomeroVEf,
      IF(
        ENDS_WITH('CCY-LtvH','Account-ZptO'),
        IF(
          SourceC.FieldG NOT LIKE 'Customer-NKYP', SourceE.FieldH,
          'Account-NK5n'),
        IF('Amt-Kk12' <> SourceD.FieldD,'Account-X4lt','CCY-htGp'))
        AS Account26Bc,
      IF(
        STARTS_WITH(DerivedTable2.CodeRziT,'CCY-u854'),
        IF(
          ENDS_WITH(DerivedTable1.Codeuhr9,'Account-RrUn'), 'Account-xCiH',
          'Fee-qkQ5'), IF(
                         STARTS_WITH('Account-uGTN','Country-r7Aj'), 'CCY-ZYoq',
                         DerivedTable2.AmtmorU))
        AS Countryql5z,
      IF(
        'Country-bsol' NOT LIKE 'Amt-M27d',
        CASE
          WHEN
            'Fee-ZO8i' != DerivedTable2.AmtNncS
          THEN
            DerivedTable2.AccounttVM1
          WHEN
            'Customer-eqY2' LIKE 'Code-akgz'
          THEN
            'Country-l9xE'
          WHEN
            STARTS_WITH('Customer-yUoq',SourceC.FieldJ)
          THEN
            'Fee-LF8m'
          WHEN
            ENDS_WITH(DerivedTable1.AccountmNQo,DerivedTable1.AddressoeLk)
          THEN
            'Code-XSTX'
          ELSE
            DerivedTable2.AccounttVM1
        END, IF(
               SourceD.FieldD NOT LIKE DerivedTable1.Codeuhr9,
               DerivedTable1.AccountKwSP, 'CCY-8XPD'))
        AS CodenEXh,
      IF(
        DerivedTable2.AccountrJZf <> 'Address-TjDT',
        IF(
          'Amt-dSBK' NOT LIKE SourceC.FieldH, 'Fee-PcNZ',
          DerivedTable1.AccountqcVc),
        IF(
          SourceE.FieldI != DerivedTable1.Codeuhr9, 'Account-sy3r',
          'Address-19s7'))
        AS Fee91Qg,
      IF(
        'Account-Ehr5' = DerivedTable1.CustomerKrwR,
        IF(
          STARTS_WITH(SourceA.FieldF,'Code-eVXz'), DerivedTable1.AccountqcVc,
          'Country-vpfF'),
        CASE
          WHEN ENDS_WITH(SourceE.FieldE,DerivedTable1.FeerBIS) THEN
            DerivedTable2.AccountrJZf
          WHEN ENDS_WITH(SourceA.FieldC,SourceC.FieldA) THEN
            'Customer-jiYg'
          WHEN SourceE.FieldC = SourceA.FieldJ THEN
            SourceE.FieldH
          WHEN ENDS_WITH('Address-guYq','Address-fLGh') THEN
            'Account-kQnj'
          WHEN STARTS_WITH(SourceE.FieldE,DerivedTable2.CodeDmva) THEN
            'Customer-MFPX'
          ELSE
            'Country-6B1t'
        END)
        AS AmtEAxo,
      IF(
        'Fee-I5Cu' != SourceD.FieldF,
        IF(
          ENDS_WITH('Code-nWPl','Customer-HJoY'), 'Country-fkuT',
          DerivedTable1.CustomerKrwR),
        IF(
          ENDS_WITH(DerivedTable2.AccountctCy,DerivedTable2.CountrywaWi),
          DerivedTable2.AmtmorU, DerivedTable2.AccountctCy))
        AS FeeLFId,
      IF(
        STARTS_WITH('Country-ByYa','Country-zT3C'),
        IF(
          ENDS_WITH(DerivedTable2.AccounttVM1,SourceD.FieldJ),
          DerivedTable1.AccountKwSP, SourceA.FieldG),
        CASE
          WHEN STARTS_WITH(SourceE.FieldJ,'Amt-8NkW') THEN
            SourceE.FieldC
          WHEN ENDS_WITH('Amt-pTTe',SourceA.FieldH) THEN
            'Amt-1YqL'
          WHEN ENDS_WITH('Code-hkSM','Customer-lOHI') THEN
            SourceC.FieldI
          WHEN ENDS_WITH('Code-vXz9','Customer-dr8c') THEN
            'Code-IGws'
          WHEN 'Code-71Fy' != DerivedTable1.CustomerWpfX THEN
            'Country-1MgO'
          ELSE
            SourceD.FieldJ
        END)
        AS CCYe5Sb,
      IF(
        'Account-SYn0' != DerivedTable1.Codeuhr9,
        IF('CCY-3Y7k' = 'Code-GC7I','Fee-C6wX',SourceA.FieldA),
        IF(SourceE.FieldA != 'Fee-U16Q',SourceD.FieldI,'CCY-RIIV'))
        AS AccountAYDB,
      CASE
        WHEN STARTS_WITH(SourceC.FieldE,'Amt-qbL5') THEN
          IF(
            'Customer-es4r' = DerivedTable2.AccountrJZf, SourceE.FieldD,
            'Customer-Uzql')
        WHEN ENDS_WITH('Fee-iHNM',SourceD.FieldG) THEN
          IF(
            'Code-7HoW' NOT LIKE 'Address-AFtK', DerivedTable1.CCYXSkS,
            SourceA.FieldE)
        ELSE
          CASE
            WHEN STARTS_WITH('Address-9JVj','Country-LpZn') THEN
              DerivedTable2.CodeDmva
            WHEN SourceE.FieldD LIKE DerivedTable1.CustomerKrwR THEN
              'Amt-ZuNa'
            WHEN STARTS_WITH('Amt-OrI8','Country-f4Xi') THEN
              'Country-X89l'
            ELSE
              'Country-qDif'
          END
      END
        AS CCYu27f,
      IF(
        ENDS_WITH('Amt-RAp7','Account-JpuB'),
        CASE
          WHEN ENDS_WITH('Country-mFQ1',DerivedTable2.CountrywaWi) THEN
            'CCY-sCy5'
          WHEN 'Fee-mdje' != 'Code-nVlx' THEN
            SourceA.FieldA
          WHEN STARTS_WITH('Address-3uxC',SourceD.FieldA) THEN
            'Customer-5Ue9'
          WHEN DerivedTable2.AccountctCy LIKE SourceA.FieldI THEN
            DerivedTable1.AccountqcVc
          ELSE
            'Account-BvAt'
        END, IF(
               ENDS_WITH('Amt-mBsL','Code-YPaO'), 'Account-qh5L',
               DerivedTable2.CodeRziT))
        AS CodeyeDB,
      IF(
        DerivedTable1.AccountfEJK <> 'Code-dO4s',
        IF('Account-JrMM' NOT LIKE 'Fee-JyGQ','Amt-tIED','Amt-wnjy'),
        CASE
          WHEN ENDS_WITH(SourceC.FieldE,DerivedTable2.AccountrJZf) THEN
            'Account-9YMm'
          WHEN 'Fee-l81y' != SourceD.FieldJ THEN
            'Code-d1wD'
          ELSE
            SourceA.FieldD
        END)
        AS CodeBPIQ,
      IF(
        ENDS_WITH(SourceE.FieldH,DerivedTable1.AddressoeLk),
        CASE
          WHEN 'Customer-Ka8M' = DerivedTable1.FeerBIS THEN
            DerivedTable1.CustomerWpfX
          WHEN 'Fee-8MsW' <> 'Amt-9dV4' THEN
            DerivedTable1.AmtR86V
          WHEN DerivedTable1.CustomerWrlz LIKE 'Address-9H6l' THEN
            SourceD.FieldE
          WHEN 'Account-wv41' <> DerivedTable2.Amt885g THEN
            DerivedTable1.CustomerWrlz
          ELSE
            'Account-lKZE'
        END, IF('Address-Qzhi' LIKE SourceA.FieldF,'Code-znkc',SourceE.FieldI))
        AS Code4AeN
    FROM
      testdataset.SourceD AS SourceD
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(SourceD.FieldF,'Fee-QE5g') OR
         ENDS_WITH(SourceA.FieldG,SourceA.FieldC) OR
         STARTS_WITH('Customer-kIZB',SourceD.FieldI) OR
         'Country-htda' != SourceA.FieldI)
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Fee-ZhiB','Code-WMh2') OR
         STARTS_WITH('Code-wE5C','Fee-Hk0o') OR
         'Code-88Nj' LIKE SourceA.FieldI AND
         ENDS_WITH('Fee-j66S',SourceC.FieldJ))
      INNER JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH(SourceE.FieldB,SourceC.FieldD) OR
         ENDS_WITH(SourceC.FieldE,'Code-83OU') AND
         SourceC.FieldI <> 'Account-mlz2' OR
         ENDS_WITH(SourceD.FieldB,'Fee-Xm4A'))
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH(SourceC.FieldD,'Fee-9VPS') AND
         'Amt-8Xsi' LIKE SourceA.FieldA AND
         'Address-Udw2' NOT LIKE 'CCY-v6G4' OR
         DerivedTable1.CustomerWrlz != 'Address-YVaC')
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (STARTS_WITH('Customer-7VKN',SourceD.FieldB) AND
         ENDS_WITH(SourceE.FieldE,SourceD.FieldA) AND
         STARTS_WITH('CCY-ZpHa','Code-wjcB') AND
         STARTS_WITH('Country-BAGd','Country-L6Qb'))
    WHERE
      ENDS_WITH('Amt-kENu','Address-PXAB') OR
      SourceC.FieldA NOT LIKE SourceC.FieldG AND
      'Address-IS11' != 'Account-Dkkt' OR 'Code-Xc6f' != 'Customer-rLIl' AND
      SourceC.FieldJ LIKE DerivedTable1.AccountmNQo AND
      STARTS_WITH('Address-3dk1','CCY-zuhg') OR
      STARTS_WITH(SourceA.FieldF,'Fee-IejH') AND
      DerivedTable2.Account61kv <> SourceC.FieldF
  ),
  Table17 AS (
    SELECT
      IF(
        STARTS_WITH(Table16.Account26Bc,DerivedTable3.FeeCUde),
        CASE
          WHEN SourceE.FieldA NOT LIKE Table16.CodenEXh THEN
            'Account-vtgA'
          WHEN STARTS_WITH(SourceC.FieldE,DerivedTable3.FeeT7G9) THEN
            Table16.CodeBPIQ
          ELSE
            'Fee-wUbH'
        END, IF(
               STARTS_WITH('Country-VM47',DerivedTable2.CodeDmva), 'Code-S1J8',
               Table16.Code4AeN))
        AS AddressyGDx,
      IF(
        'Code-jMdU' != SourceB.FieldA,
        IF(
          ENDS_WITH(SourceB.FieldJ,'Code-oE4I'), DerivedTable3.CustomerULyd,
          'Code-zZYA'),
        IF(SourceD.FieldJ <> 'Fee-9538','Account-4CAr',DerivedTable3.Feeg8kl))
        AS Code9zfL,
      IF(
        ENDS_WITH(DerivedTable2.Amtp4ee,'Amt-6HMk'),
        CASE
          WHEN 'Amt-fAkK' <> 'Amt-zNks' THEN
            'Code-Qwsu'
          WHEN STARTS_WITH('Code-c4ys','Country-dTjA') THEN
            'Fee-FBao'
          WHEN 'Fee-dHSz' <> 'CCY-utiS' THEN
            'Customer-I3y1'
          ELSE
            'CCY-B6aw'
        END, IF(
               STARTS_WITH('Country-dQH4','Account-dqh6'), SourceE.FieldH,
               'Fee-Uhat'))
        AS FeeZaer,
      IF(
        'Account-8jeD' = DerivedTable3.FeeD0Po,
        IF(
          DerivedTable2.AmtmorU NOT LIKE 'Fee-icNS', DerivedTable2.Amtp4ee,
          'Fee-WZUy'),
        IF('Customer-zy7Z' LIKE 'Amt-4RiX','Fee-57MF','Account-FRyl'))
        AS Customero58L,
      CASE
        WHEN STARTS_WITH(SourceE.FieldJ,Table16.Code4AeN) THEN
          IF(
            DerivedTable2.AccountctCy NOT LIKE SourceA.FieldE,
            DerivedTable3.Feeg8kl, 'Address-Umrn')
        WHEN SourceE.FieldI LIKE Table16.CCYe5Sb THEN
          IF(
            ENDS_WITH('CCY-lYjn',DerivedTable3.CountrygMB4), SourceA.FieldJ,
            'Amt-kXfC')
        WHEN DerivedTable3.CountrygMB4 != 'CCY-Ujdj' THEN
          CASE
            WHEN ENDS_WITH(SourceA.FieldB,'CCY-J36p') THEN
              'Country-0SnW'
            WHEN STARTS_WITH('CCY-zDWj',SourceC.FieldB) THEN
              DerivedTable3.FeepQGh
            WHEN ENDS_WITH('Fee-tQgJ',Table16.Code4AeN) THEN
              'Customer-IO3r'
            ELSE
              DerivedTable3.Feeg8kl
          END
        WHEN STARTS_WITH('Fee-VMZY',SourceE.FieldB) THEN
          IF(
            STARTS_WITH('CCY-pjqg','Fee-3E8s'), Table16.CustomeroVEf,
            SourceD.FieldE)
        WHEN DerivedTable3.CountrygPvi <> DerivedTable3.FeeCUde THEN
          IF(
            'CCY-bU6z' <> 'Country-Saem', DerivedTable2.Address5RWP,
            DerivedTable2.CountrywaWi)
        ELSE
          IF(Table16.CodeBPIQ = SourceB.FieldB,SourceC.FieldH,'Country-F10G')
      END
        AS AccountgFkW,
      IF(
        DerivedTable3.FeepQGh = 'Code-L2nC',
        CASE
          WHEN STARTS_WITH('Code-ozhm','Amt-tYR1') THEN
            'Fee-e376'
          WHEN ENDS_WITH(SourceB.FieldD,Table16.Account26Bc) THEN
            SourceD.FieldF
          WHEN STARTS_WITH('CCY-Jn9I','Country-Luxt') THEN
            'Code-s5Sb'
          ELSE
            DerivedTable1.CCYXSkS
        END, IF(
               ENDS_WITH('Address-YoSd',SourceB.FieldI), DerivedTable3.FeeT7G9,
               DerivedTable3.FeeCUde))
        AS CodeJcKJ,
      CASE
        WHEN 'Customer-UlH9' NOT LIKE 'Account-26yD' THEN
          IF(
            'Customer-g7Hi' LIKE 'Customer-lorI', SourceB.FieldA,
            DerivedTable1.FeerBIS)
        WHEN 'Amt-Af9U' LIKE SourceA.FieldG THEN
          IF(ENDS_WITH('CCY-sAIY','Customer-FCaB'),'CCY-fN0z','Country-9DGr')
        WHEN 'Country-lIrX' <> SourceB.FieldC THEN
          IF(
            STARTS_WITH('CCY-7yAh',SourceA.FieldA), SourceC.FieldC,
            'Account-njqJ')
        WHEN SourceD.FieldA LIKE 'CCY-kOPq' THEN
          IF(
            'Account-vbhK' NOT LIKE 'Country-7deA', SourceD.FieldF,
            'Country-TgtG')
        ELSE
          IF(
            STARTS_WITH(DerivedTable3.CodeUeLq,Table16.AddressjCYy), 'CCY-aDfb',
            'Address-wG2z')
      END
        AS CodehRk8,
      IF(
        STARTS_WITH('CCY-NzhP','CCY-jKsF'),
        IF(SourceB.FieldC = 'CCY-oq6w','Account-ryTW','Code-IRIg'),
        CASE
          WHEN STARTS_WITH(SourceA.FieldD,'Address-4uor') THEN
            Table16.AddressjCYy
          WHEN STARTS_WITH(SourceB.FieldE,'Account-aLCm') THEN
            DerivedTable3.CCYO742
          WHEN ENDS_WITH(Table16.AddressjCYy,DerivedTable1.FeerBIS) THEN
            SourceC.FieldB
          WHEN SourceE.FieldG != DerivedTable3.AccountViga THEN
            SourceC.FieldB
          WHEN STARTS_WITH('Code-cKiP',SourceB.FieldA) THEN
            'Code-CESZ'
          ELSE
            'Address-ytUR'
        END)
        AS AddressFydA,
      IF(
        STARTS_WITH(SourceB.FieldA,'Fee-brTv'),
        IF('CCY-8reD' = SourceD.FieldD,'Fee-lgud','CCY-mvfp'),
        IF(
          SourceC.FieldJ = Table16.CCYu27f, DerivedTable2.AmtmorU,
          SourceB.FieldG))
        AS CodeStRR,
      IF(
        'Address-tkl4' = DerivedTable2.AccountctCy,
        CASE
          WHEN ENDS_WITH(SourceC.FieldF,'CCY-SXxR') THEN
            DerivedTable1.AmtR86V
          WHEN 'Amt-gn6Q' = 'Code-Yuap' THEN
            'Customer-Hh8Q'
          WHEN STARTS_WITH(Table16.FeeLFId,'Country-XB9o') THEN
            'Customer-lBWU'
          WHEN STARTS_WITH('Country-Fh02','Country-NnSw') THEN
            Table16.CountryiYbf
          ELSE
            SourceB.FieldD
        END, CASE
               WHEN STARTS_WITH('Address-q53W',DerivedTable3.Feeg8kl) THEN
                 Table16.CCYe5Sb
               WHEN STARTS_WITH(DerivedTable2.AccounttVM1,SourceA.FieldG) THEN
                 'Amt-vGTJ'
               ELSE
                 SourceB.FieldJ
             END)
        AS Code9kal,
      IF(
        ENDS_WITH(SourceC.FieldD,DerivedTable2.AccounttVM1),
        IF(
          STARTS_WITH(DerivedTable3.CustomerLP0M,Table16.CodeyeDB),
          DerivedTable3.FeepQGh, 'Fee-pdJr'),
        IF(
          ENDS_WITH(SourceA.FieldI,SourceA.FieldH), SourceE.FieldA,
          Table16.Account26Bc))
        AS AmtLUPW,
      CASE
        WHEN 'CCY-nw8L' NOT LIKE DerivedTable1.Codeuhr9 THEN
          IF(SourceE.FieldC NOT LIKE 'Fee-a9JO',Table16.CCYu27f,'Amt-xQ20')
        WHEN ENDS_WITH(Table16.CountryI2wJ,'CCY-86pD') THEN
          IF(
            DerivedTable2.Account61kv <> 'Account-ZhoH', SourceC.FieldF,
            DerivedTable1.CCYXSkS)
        WHEN 'Code-Sh8x' != SourceA.FieldH THEN
          IF('Code-ZZ24' NOT LIKE 'Address-YRrh','Account-kv4I','Amt-Vksv')
        WHEN ENDS_WITH(DerivedTable3.CustomerULyd,SourceA.FieldI) THEN
          IF(
            ENDS_WITH(SourceB.FieldH,'Country-PHbT'), SourceE.FieldI,
            'Code-xDKY')
        WHEN 'Customer-2D7n' NOT LIKE 'Account-Cjln' THEN
          IF(
            ENDS_WITH(SourceC.FieldG,DerivedTable2.AccounttVM1),
            'Customer-9BL6', 'Customer-pO09')
        ELSE
          IF(SourceE.FieldA != 'Country-qrpM',SourceC.FieldB,'Amt-rvmw')
      END
        AS FeewsRv,
      IF(
        'Account-aVSR' LIKE Table16.Account26Bc,
        IF(
          SourceE.FieldG NOT LIKE DerivedTable2.CodeRziT, 'Address-z5md',
          'Customer-tIUG'),
        CASE
          WHEN Table16.AddressjCYy <> 'Country-RS4v' THEN
            'Address-fbdw'
          WHEN SourceC.FieldC = 'CCY-BnqG' THEN
            Table16.Countryql5z
          ELSE
            'Code-RtfG'
        END)
        AS CodenDQe,
      IF(
        STARTS_WITH(SourceD.FieldA,'Amt-fNKf'),
        IF(
          DerivedTable1.CountryV1io NOT LIKE 'CCY-SNH1', DerivedTable3.FeeT7G9,
          SourceA.FieldF), IF(
                             STARTS_WITH('Address-xibX','Code-Gzwx'),
                             'Address-kUMd', DerivedTable1.AccountqcVc))
        AS FeeKZMF,
      IF(
        Table16.CountryI2wJ != 'Account-59Nc',
        IF(
          ENDS_WITH('CCY-JG8N','Account-OSNH'), SourceC.FieldH,
          DerivedTable3.CCYO742),
        IF(
          ENDS_WITH(DerivedTable1.AccountmNQo,DerivedTable3.CountryoFFA),
          'Address-FaLg', 'Amt-MUNz'))
        AS FeepnbR,
      IF(
        'Address-lnqd' <> 'Country-Du0b',
        CASE
          WHEN SourceB.FieldJ = 'Fee-sbHQ' THEN
            SourceD.FieldI
          WHEN STARTS_WITH('Account-ldYF','CCY-UzUv') THEN
            'Fee-8caJ'
          WHEN DerivedTable1.AmtR86V != 'Address-UHlt' THEN
            'Amt-TKvW'
          WHEN STARTS_WITH('Code-8Pyx','Account-ZleC') THEN
            DerivedTable1.CCYXSkS
          ELSE
            'Account-nMX8'
        END,
        CASE
          WHEN
            STARTS_WITH(SourceE.FieldH,'Country-i9GL')
          THEN
            'Country-70DN'
          WHEN
            'CCY-PsML' != SourceA.FieldB
          THEN
            'Address-tTLW'
          WHEN
            ENDS_WITH(DerivedTable1.AccountKwSP,DerivedTable3.CustomereBLI)
          THEN
            'CCY-9vFH'
          WHEN
            STARTS_WITH(Table16.CodenEXh,SourceB.FieldA)
          THEN
            Table16.CCYe5Sb
          WHEN
            STARTS_WITH('Country-Uz63','Amt-rtby')
          THEN
            SourceE.FieldA
          ELSE
            'Fee-PVVT'
        END)
        AS AccountwHE6,
      CASE
        WHEN ENDS_WITH('Fee-MWbc',SourceD.FieldJ) THEN
          IF(
            STARTS_WITH('Code-VBTv',DerivedTable2.CodeRziT), SourceC.FieldG,
            'Account-ryk3')
        WHEN 'Account-jUxJ' NOT LIKE SourceB.FieldF THEN
          IF(
            STARTS_WITH(Table16.CountryI2wJ,'Fee-w6dl'), 'Address-htND',
            SourceD.FieldC)
        ELSE
          IF(ENDS_WITH(SourceA.FieldE,'Country-BXSj'),'CCY-jTdi','Address-Mvqc')
      END
        AS CCYnSYx,
      IF(
        DerivedTable3.CountrygMB4 != SourceB.FieldA,
        IF(
          ENDS_WITH('Country-thaU','Customer-9ekA'), 'CCY-iq3g',
          Table16.CodenEXh),
        IF('Code-6aWw' NOT LIKE 'CCY-cxNB','Customer-jF0V','Account-IhHA'))
        AS CountryIzU9
    FROM
      testdataset.SourceC AS SourceC
      INNER JOIN testdataset.SourceA AS SourceA ON
        (SourceA.FieldC LIKE 'Amt-K4fT' AND SourceC.FieldG = SourceA.FieldD OR
         ENDS_WITH('Account-VVBi',SourceA.FieldI) OR
         STARTS_WITH('Customer-PerL','Amt-1kKU'))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceA.FieldH,'Fee-jBIe') OR
         ENDS_WITH('CCY-K6iX',SourceA.FieldJ) AND
         STARTS_WITH('Fee-xv6M','Account-0CCS') AND
         'Customer-Elpp' NOT LIKE SourceC.FieldH)
      INNER JOIN testdataset.SourceD AS SourceD ON
        (SourceB.FieldC NOT LIKE 'Address-wHj3' OR
         'Code-fIqX' <> SourceA.FieldB OR
         'Amt-nRxM' NOT LIKE 'Address-8z70' AND 'Address-XgW9' = SourceC.FieldF)
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (SourceE.FieldI NOT LIKE 'CCY-hrPr' OR
         STARTS_WITH('Account-oTRG','Country-Mk4d') AND
         ENDS_WITH('Account-ZBvg','Account-xpsq') OR
         SourceA.FieldA NOT LIKE SourceE.FieldG)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Country-dmfQ','Customer-sWtH') AND
         SourceD.FieldC = SourceD.FieldH OR
         ENDS_WITH(DerivedTable1.Addressrgjd,SourceD.FieldJ) OR
         'Address-oQaR' != SourceA.FieldF)
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Country-BmGJ' NOT LIKE 'Fee-qhYu' AND
         ENDS_WITH('Fee-mRYv','Customer-yWSO') OR
         'Code-cqYj' LIKE 'Customer-rAyw' OR
         'Address-dLXM' LIKE 'Customer-pUNr')
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (DerivedTable1.AmtzEYq != SourceB.FieldI AND
         STARTS_WITH('Customer-lkkb',SourceA.FieldI) AND
         DerivedTable2.AddresscM17 NOT LIKE SourceB.FieldG AND
         ENDS_WITH('Fee-lcQr','Customer-jyNc'))
      RIGHT JOIN Table16 AS Table16 ON
        (DerivedTable1.AccountqcVc LIKE 'Customer-mLfP' AND
         ENDS_WITH('Address-l8xn','Account-4hWh') AND
         STARTS_WITH('Account-KGCa','Account-YAOy') OR
         STARTS_WITH('Fee-fWdI',DerivedTable3.FeepQGh))
    WHERE
      'Country-HvRP' = 'CCY-R5AA' AND
      STARTS_WITH(SourceB.FieldH,'Address-VNBj') OR
      DerivedTable2.AddressHdnu != SourceB.FieldC AND
      STARTS_WITH('CCY-16WV','Account-6YxX') OR
      'Account-iMCj' <> DerivedTable1.AccountKwSP OR
      STARTS_WITH(DerivedTable2.CodeRziT,'Country-Eszj') AND
      STARTS_WITH(DerivedTable1.AccountmNQo,'Amt-7xrw') OR
      STARTS_WITH(Table16.CodenEXh,'Amt-Ppei')
  ),
  Table18 AS (
    SELECT
      IF(
        ENDS_WITH(DerivedTable1.FeerBIS,'Fee-dv8z'),
        IF(
          SourceB.FieldE LIKE Table16.AddressjCYy, Table16.Amt6J7p,
          DerivedTable3.FeepQGh),
        IF(
          'CCY-LKdI' NOT LIKE DerivedTable1.AccountKwSP, 'Fee-xkv4',
          'Address-NA7v'))
        AS CodeGxmG,
      IF(
        'Country-Tj8g' != DerivedTable1.AddressoeLk,
        IF(
          ENDS_WITH(DerivedTable3.FeeF6P4,DerivedTable2.CountrywaWi),
          'Account-xYsu', SourceA.FieldI),
        IF(
          ENDS_WITH(SourceB.FieldA,'Customer-nYM2'), Table16.CodeBPIQ,
          'Amt-ZjsL'))
        AS CodedDLV,
      IF(
        'Address-UONm' NOT LIKE 'Fee-5nel',
        CASE
          WHEN Table16.CountryI2wJ LIKE DerivedTable2.CountrywaWi THEN
            Table16.Code5tEA
          WHEN ENDS_WITH(SourceA.FieldH,Table16.CodenEXh) THEN
            'Code-pRPy'
          WHEN DerivedTable3.CustomerULyd != DerivedTable3.CustomerULyd THEN
            DerivedTable3.CCYO742
          ELSE
            Table16.Code5tEA
        END, IF(
               DerivedTable3.CountrygMB4 != SourceA.FieldJ, 'Fee-ADIL',
               SourceB.FieldH))
        AS AmtyjS3,
      IF(
        ENDS_WITH('Account-7Y0N',SourceB.FieldG),
        CASE
          WHEN SourceD.FieldE NOT LIKE DerivedTable1.FeerBIS THEN
            SourceB.FieldD
          WHEN STARTS_WITH('Country-ITtP','Account-zlJE') THEN
            DerivedTable3.CustomereBLI
          WHEN 'Code-fflU' != 'Address-dZfQ' THEN
            DerivedTable1.Codeuhr9
          WHEN STARTS_WITH(DerivedTable1.CountryV1io,SourceA.FieldF) THEN
            'Address-qqqR'
          ELSE
            SourceD.FieldH
        END, IF(
               Table16.CountryI2wJ LIKE DerivedTable2.AccountrJZf,
               SourceA.FieldC, 'Code-f9rj'))
        AS AccountGvoo,
      IF(
        Table16.CountryI2wJ = SourceE.FieldE,
        CASE
          WHEN 'Code-qocu' != 'Code-SGwi' THEN
            'Customer-vUlM'
          WHEN STARTS_WITH(DerivedTable3.CodeVHWz,'CCY-n7BR') THEN
            'Customer-H5i9'
          WHEN SourceE.FieldG <> Table16.Amt6J7p THEN
            DerivedTable2.Amtp4ee
          WHEN DerivedTable3.CountrygMB4 <> DerivedTable3.CodeUeLq THEN
            SourceB.FieldC
          ELSE
            DerivedTable3.Code3Xip
        END,
        CASE
          WHEN ENDS_WITH('CCY-DiAb',Table16.Countryql5z) THEN
            'CCY-QzTZ'
          WHEN SourceE.FieldE <> SourceE.FieldB THEN
            'Amt-E08s'
          WHEN 'Address-3jsU' <> 'Code-n2qV' THEN
            Table16.Fee91Qg
          WHEN ENDS_WITH('Amt-iGrZ',SourceD.FieldG) THEN
            'Account-bo6N'
          ELSE
            Table16.Countryql5z
        END)
        AS CodefXu0,
      IF(
        'CCY-kQwC' != DerivedTable3.FeepQGh,
        IF(
          STARTS_WITH(DerivedTable1.Codeuhr9,DerivedTable1.Addressrgjd),
          DerivedTable1.AmtzEYq, 'Country-d7FH'),
        IF(
          ENDS_WITH(DerivedTable3.FeeD0Po,'Account-mVRw'),
          DerivedTable3.CodeVHWz, 'Amt-MvdH'))
        AS FeeU7V2,
      IF(
        ENDS_WITH(Table16.AddressFAql,'Code-ScBm'),
        IF('Country-tjyL' != Table16.Code4AeN,'CCY-9KwF','Code-obPE'),
        CASE
          WHEN SourceA.FieldE NOT LIKE SourceE.FieldI THEN
            SourceA.FieldE
          WHEN 'Code-rcjg' LIKE 'Customer-YxZk' THEN
            'Code-WpkR'
          WHEN STARTS_WITH(DerivedTable3.Feeg8kl,SourceB.FieldH) THEN
            'Account-qEEV'
          ELSE
            SourceE.FieldG
        END)
        AS Address0pyc,
      IF(
        'Code-KWcJ' LIKE 'Country-gSV6',
        CASE
          WHEN SourceA.FieldJ != 'Address-5JZa' THEN
            'Code-luBt'
          WHEN 'Code-2G2v' != SourceB.FieldH THEN
            'Amt-BY4J'
          WHEN STARTS_WITH('Customer-Kx1d',Table16.CodeyeDB) THEN
            Table16.AddressjCYy
          ELSE
            'Customer-bBaY'
        END,
        CASE
          WHEN STARTS_WITH(Table16.CodeBPIQ,DerivedTable2.CountrywaWi) THEN
            SourceE.FieldG
          WHEN ENDS_WITH(SourceD.FieldJ,SourceD.FieldF) THEN
            'Account-DvEE'
          WHEN Table16.CCYe5Sb NOT LIKE 'Account-B1E3' THEN
            DerivedTable3.CodeVHWz
          WHEN STARTS_WITH(SourceD.FieldF,'Amt-NpVS') THEN
            'Address-dYbO'
          ELSE
            SourceA.FieldE
        END)
        AS CodeO1XF,
      IF(
        DerivedTable2.CodeDmva LIKE 'Customer-1quE',
        CASE
          WHEN 'Code-dKYI' <> 'Amt-V3Rl' THEN
            'Country-dLOM'
          WHEN 'Fee-bni7' LIKE 'Code-PvUA' THEN
            DerivedTable3.Codeaqvh
          WHEN ENDS_WITH(DerivedTable1.AmtR86V,Table16.Amt6J7p) THEN
            SourceE.FieldJ
          ELSE
            'Account-BbvO'
        END, CASE
               WHEN SourceB.FieldB LIKE 'Code-6jiF' THEN
                 'Amt-MU7R'
               WHEN DerivedTable1.AddressoeLk = 'Address-T4BJ' THEN
                 DerivedTable1.Codeuhr9
               WHEN 'Country-35eT' = 'Customer-qDOa' THEN
                 'Address-ht4D'
               ELSE
                 'CCY-nIA6'
             END)
        AS CCYLfD9,
      IF(
        STARTS_WITH(DerivedTable3.Codeaqvh,Table16.CountryiYbf),
        IF(
          ENDS_WITH('Customer-y9sH','Country-vMNI'), 'Customer-wM1k',
          'Amt-Qsrg'),
        CASE
          WHEN STARTS_WITH(SourceE.FieldH,'CCY-jMe9') THEN
            SourceB.FieldF
          WHEN 'Code-QO5j' NOT LIKE DerivedTable2.Address5RWP THEN
            SourceE.FieldJ
          WHEN STARTS_WITH(DerivedTable2.Amtp4ee,SourceD.FieldJ) THEN
            'Account-3UJz'
          WHEN 'Amt-ptpq' LIKE 'Customer-gdLP' THEN
            DerivedTable1.AccountfEJK
          WHEN STARTS_WITH(SourceE.FieldB,'Country-JZ8s') THEN
            'Amt-umIA'
          ELSE
            'Fee-WJQz'
        END)
        AS FeeZQMH,
      CASE
        WHEN 'Country-wRou' LIKE 'Amt-qOyX' THEN
          IF(
            ENDS_WITH(SourceA.FieldC,DerivedTable2.CodeRziT), 'Address-jLtd',
            'Customer-qdlq')
        WHEN ENDS_WITH(DerivedTable1.Addressrgjd,DerivedTable1.Addressrgjd) THEN
          IF(
            SourceA.FieldI NOT LIKE 'Code-SlLS', DerivedTable1.Addressrgjd,
            SourceA.FieldB)
        WHEN 'Code-SFk6' LIKE DerivedTable1.CustomerKrwR THEN
          IF('Address-AgaM' NOT LIKE 'Fee-AWyS','Account-TVZy',SourceA.FieldI)
        WHEN 'Address-s2NX' LIKE DerivedTable1.CustomerWrlz THEN
          IF(
            STARTS_WITH(DerivedTable2.Address7oYI,'Account-19P1'),
            DerivedTable3.CodeVHWz, 'Address-hERg')
        WHEN STARTS_WITH('Customer-JIYq',SourceE.FieldE) THEN
          CASE
            WHEN 'Country-3WBi' <> 'Account-4u4i' THEN
              SourceD.FieldB
            WHEN 'Amt-GGtH' != SourceE.FieldJ THEN
              DerivedTable1.AccountKwSP
            WHEN SourceD.FieldA != 'Account-bUBQ' THEN
              SourceE.FieldG
            WHEN ENDS_WITH(SourceB.FieldJ,'Account-qZt2') THEN
              DerivedTable2.AmtNncS
            ELSE
              Table16.AccountAYDB
          END
        ELSE
          IF(
            STARTS_WITH('CCY-oQlX','Customer-aiJF'), DerivedTable1.AccountKwSP,
            'CCY-TWsy')
      END
        AS CustomeriCu9,
      IF(
        DerivedTable3.CustomerLP0M LIKE DerivedTable2.AccounttVM1,
        CASE
          WHEN STARTS_WITH('Code-NLbY',Table16.AddressjCYy) THEN
            'Amt-aOVb'
          WHEN 'Customer-tCwp' != 'Code-7tz9' THEN
            'Code-VT3C'
          WHEN STARTS_WITH('Address-QKFr',Table16.AccountAYDB) THEN
            'Code-FmKi'
          WHEN STARTS_WITH(SourceD.FieldA,'CCY-f9cQ') THEN
            DerivedTable3.FeepQGh
          WHEN STARTS_WITH(SourceA.FieldI,'Customer-Jbfk') THEN
            SourceA.FieldA
          ELSE
            SourceE.FieldD
        END, IF(
               DerivedTable1.CountryV1io LIKE 'CCY-9Dqv', 'Code-9TJ1',
               'Country-JdMe'))
        AS AddressFaqI,
      IF(
        ENDS_WITH('CCY-Phna',SourceE.FieldB),
        CASE
          WHEN SourceD.FieldJ NOT LIKE Table16.CodeBPIQ THEN
            'Country-056i'
          WHEN DerivedTable3.FeeT7G9 LIKE SourceD.FieldF THEN
            'Country-5UHO'
          WHEN STARTS_WITH('Account-st1J','Amt-iotc') THEN
            SourceE.FieldH
          WHEN 'Customer-Wkbo' LIKE Table16.CodeBPIQ THEN
            'Code-iSNz'
          ELSE
            Table16.FeeLFId
        END,
        CASE
          WHEN STARTS_WITH(DerivedTable1.CustomerWrlz,SourceA.FieldB) THEN
            Table16.Amt6J7p
          WHEN ENDS_WITH('Amt-ouT9','Customer-UMYU') THEN
            'Account-34CH'
          WHEN 'Code-KRtg' NOT LIKE SourceB.FieldI THEN
            Table16.AddressjCYy
          WHEN ENDS_WITH(DerivedTable2.Address7oYI,'Customer-BYKo') THEN
            'Fee-Ys5t'
          ELSE
            'Fee-FASC'
        END)
        AS CCY2LEe,
      CASE
        WHEN 'Account-AgVT' NOT LIKE 'Code-81lp' THEN
          IF('Code-r8ub' NOT LIKE SourceB.FieldI,'CCY-3SN6','Amt-ywIg')
        WHEN STARTS_WITH('Code-FaHy','Account-jYvU') THEN
          IF(
            STARTS_WITH(Table16.Code4AeN,'Address-2Oq0'), 'Code-Heqi',
            Table16.CountryI2wJ)
        WHEN STARTS_WITH(DerivedTable1.CustomerKrwR,SourceE.FieldJ) THEN
          CASE
            WHEN ENDS_WITH('Account-GN1Q',SourceB.FieldF) THEN
              DerivedTable2.AccountctCy
            WHEN 'Customer-G0KB' NOT LIKE DerivedTable1.CountryV1io THEN
              SourceD.FieldE
            ELSE
              DerivedTable1.Addressrgjd
          END
        WHEN DerivedTable1.AccountfEJK = DerivedTable2.AccountctCy THEN
          IF(
            STARTS_WITH(DerivedTable1.CustomerWpfX,'Amt-2zON'), 'Amt-AUTS',
            SourceE.FieldE)
        ELSE
          IF(
            ENDS_WITH('Country-Y2Tu',SourceE.FieldG), 'Code-L6Nz',
            DerivedTable1.CountryV1io)
      END
        AS AddressTikV,
      IF(
        DerivedTable3.AccountViga = SourceD.FieldH,
        CASE
          WHEN DerivedTable2.AccounttVM1 = Table16.Code4AeN THEN
            SourceE.FieldE
          WHEN 'Account-K9DV' <> 'Address-KoGT' THEN
            'Fee-CU3m'
          WHEN ENDS_WITH(DerivedTable1.AccountmNQo,Table16.AddressjCYy) THEN
            'Account-UkQM'
          ELSE
            'Customer-5xUA'
        END, IF(STARTS_WITH('Amt-AQGE','Code-IVQ2'),'CCY-OtJu',SourceB.FieldJ))
        AS CCY0fSg,
      CASE
        WHEN 'Country-rNen' NOT LIKE 'Customer-gFs3' THEN
          IF(
            SourceE.FieldG = DerivedTable1.Addressrgjd,
            DerivedTable1.CountryV1io, 'CCY-Etkf')
        WHEN Table16.Account26Bc NOT LIKE DerivedTable2.Amt885g THEN
          IF('Code-RRqb' = 'Code-9i28',SourceB.FieldD,'Amt-sMHR')
        ELSE
          IF(
            STARTS_WITH(DerivedTable3.CustomerLP0M,'Address-oLKj'),
            DerivedTable1.Codeuhr9, SourceA.FieldJ)
      END
        AS Customernueg,
      IF(
        'Fee-dVaU' <> 'Customer-kBhl',
        IF(
          ENDS_WITH('Address-AAX4','Country-ZVdU'), SourceD.FieldE,
          DerivedTable3.FeeD0Po),
        CASE
          WHEN STARTS_WITH(DerivedTable1.CustomerKrwR,'Code-eFUA') THEN
            DerivedTable2.Address5RWP
          WHEN SourceA.FieldD <> 'Customer-4Z29' THEN
            DerivedTable1.CustomerWrlz
          WHEN SourceE.FieldG <> 'Country-0tic' THEN
            'Address-iJfU'
          ELSE
            'Fee-lMKA'
        END)
        AS CCYLBVC,
      IF(
        STARTS_WITH(SourceB.FieldA,'Account-0swv'),
        CASE
          WHEN
            ENDS_WITH(DerivedTable3.AccountViga,DerivedTable3.CustomerULyd)
          THEN
            DerivedTable2.AmtmorU
          WHEN
            STARTS_WITH(DerivedTable3.FeeF6P4,'Code-8wz3')
          THEN
            Table16.CountryiYbf
          WHEN
            'Fee-QCAA' <> 'Country-tiw9'
          THEN
            DerivedTable2.AddressHdnu
          WHEN
            'Country-0eEf' <> Table16.Account26Bc
          THEN
            'Amt-XVdN'
          WHEN
            ENDS_WITH(DerivedTable2.CountrywaWi,SourceA.FieldE)
          THEN
            'Customer-NEM9'
          ELSE
            DerivedTable3.Codeaqvh
        END, IF(
               'Customer-fmbp' != DerivedTable2.AmtmorU,
               DerivedTable2.AccountrJZf, SourceD.FieldD))
        AS CountryLHs8,
      IF(
        'Account-IaQu' <> Table16.Countryql5z,
        IF(
          'Address-tuWn' = DerivedTable1.AccountKwSP, SourceE.FieldF,
          DerivedTable1.CountryV1io),
        IF(
          ENDS_WITH(DerivedTable3.FeeCUde,'Country-OVyw'), 'Code-tT9p',
          SourceB.FieldJ))
        AS AddressLgaF
    FROM
      testdataset.SourceE AS SourceE
      INNER JOIN testdataset.SourceA AS SourceA ON
        (SourceE.FieldG LIKE SourceA.FieldI OR
         SourceA.FieldH LIKE 'Code-acbZ' OR
         STARTS_WITH(SourceA.FieldB,SourceA.FieldE) AND
         STARTS_WITH('CCY-TbnF',SourceE.FieldG))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        ('Address-Pkxq' != 'Amt-6bve' OR
         STARTS_WITH('Address-LbIU','Customer-eALl') OR
         STARTS_WITH('Amt-uw1R','Customer-TX2I') OR
         STARTS_WITH('Country-370N',SourceB.FieldH))
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH(SourceB.FieldJ,'Customer-kF9w') OR
         ENDS_WITH(SourceA.FieldG,'Account-aVoR') OR
         STARTS_WITH(SourceD.FieldE,'Code-VVmr') OR
         'Amt-zjxp' NOT LIKE 'Account-DWnf')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (STARTS_WITH('Amt-VkrK',DerivedTable1.AddressoeLk) OR
         ENDS_WITH(SourceD.FieldC,'Amt-LbhO') AND
         ENDS_WITH(SourceE.FieldC,SourceD.FieldB) AND
         SourceA.FieldF NOT LIKE DerivedTable1.AccountqcVc)
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Amt-CEqo' != 'CCY-FBp3' AND
         'Amt-pO9f' NOT LIKE DerivedTable1.FeerBIS OR
         ENDS_WITH('Amt-Vvqf','Address-a1aJ') AND
         STARTS_WITH(SourceD.FieldJ,'Account-vbD5'))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH('Customer-9u7Y',SourceD.FieldI) OR
         ENDS_WITH('Address-zAQ2',SourceB.FieldA) OR
         STARTS_WITH(DerivedTable2.AccountctCy,DerivedTable3.CodeUeLq) AND
         'CCY-8vKr' <> DerivedTable2.Amtp4ee)
      LEFT JOIN Table16 AS Table16 ON
        (STARTS_WITH('Customer-T9gQ','Account-TaQG') AND
         ENDS_WITH('CCY-dI0e','Code-PjQt') AND
         'Fee-xUHb' LIKE 'Fee-vubu' AND STARTS_WITH('Amt-j0sh','Amt-435n'))
    WHERE
      ENDS_WITH(Table16.Code4AeN,DerivedTable1.CustomerWrlz) AND
      ENDS_WITH('Fee-mT2c','CCY-Np8p') AND
      SourceA.FieldB LIKE SourceB.FieldF AND
      DerivedTable2.Account61kv NOT LIKE DerivedTable2.AmtmorU OR
      ENDS_WITH(SourceB.FieldJ,'Country-UX9P') OR
      ENDS_WITH(SourceD.FieldI,SourceA.FieldG) AND
      STARTS_WITH(DerivedTable2.Amt885g,Table16.CCYu27f) AND
      'CCY-GZOs' LIKE 'Amt-UZxM'
  ),
  Table19 AS (
    SELECT
      IF(
        ENDS_WITH(DerivedTable2.Address7oYI,'Address-rwff'),
        IF(SourceE.FieldI != 'Account-TGEX',Table18.CodeGxmG,'Code-OEye'),
        IF(
          ENDS_WITH('Fee-2i2V',DerivedTable2.CountrywaWi), 'Amt-yylm',
          Table17.CodeJcKJ))
        AS FeecGpI,
      CASE
        WHEN STARTS_WITH('Address-TFbr','Amt-EXxc') THEN
          IF(
            ENDS_WITH('Account-of1h',DerivedTable1.Codeuhr9),
            DerivedTable1.AmtR86V, 'Account-7gj7')
        WHEN 'Code-U8xu' != 'Fee-c8ZT' THEN
          IF('Address-bllq' = 'Code-7Vzo',SourceC.FieldB,'Country-Icvv')
        WHEN STARTS_WITH(Table18.CodeGxmG,Table18.AddressLgaF) THEN
          CASE
            WHEN ENDS_WITH(DerivedTable2.CodeRziT,'Country-YPTx') THEN
              'Fee-C4YV'
            WHEN ENDS_WITH(DerivedTable2.Address5RWP,'Code-4MBk') THEN
              DerivedTable3.CustomereBLI
            WHEN ENDS_WITH('Country-VPCv','Country-Luam') THEN
              SourceE.FieldF
            WHEN STARTS_WITH(Table16.Amt6J7p,DerivedTable1.AccountfEJK) THEN
              'Address-kpza'
            WHEN 'Customer-hWRh' NOT LIKE SourceB.FieldJ THEN
              'Account-2eYW'
            ELSE
              'Code-46Ym'
          END
        WHEN ENDS_WITH('Address-sZ8D','Fee-ZtvM') THEN
          IF('Amt-ofHN' != 'Fee-Lu8t','Code-5A1Y',SourceA.FieldH)
        WHEN STARTS_WITH(DerivedTable3.CountrygPvi,Table18.AddressLgaF) THEN
          IF(
            DerivedTable2.Amt885g LIKE 'Country-aZFZ', 'Country-Epr7',
            DerivedTable3.Codeaqvh)
        ELSE
          IF(
            'Code-5eEk' NOT LIKE DerivedTable3.CustomereBLI, 'CCY-Z1cD',
            DerivedTable3.Feeg8kl)
      END
        AS CountryvAmQ,
      IF(
        SourceC.FieldC NOT LIKE DerivedTable2.CodeDmva,
        CASE
          WHEN 'Code-Pel7' = 'Amt-ervg' THEN
            'Amt-jOzw'
          WHEN STARTS_WITH(SourceA.FieldJ,'Account-3UFl') THEN
            'Code-epRC'
          WHEN DerivedTable2.Account61kv <> 'Code-3MY2' THEN
            'Amt-4Jzc'
          WHEN 'Country-SQSR' <> DerivedTable1.Codeuhr9 THEN
            'Amt-HwBp'
          WHEN STARTS_WITH('Code-Xetd','CCY-C7Zm') THEN
            DerivedTable1.AccountmNQo
          ELSE
            Table16.Code4AeN
        END, IF(
               Table18.CCYLBVC != Table16.FeeLFId, DerivedTable2.CountrywaWi,
               'Country-sipr'))
        AS FeemHiU,
      IF(
        STARTS_WITH(SourceE.FieldA,'Fee-FeYc'),
        CASE
          WHEN STARTS_WITH(SourceD.FieldJ,Table17.CodeStRR) THEN
            Table17.CodeJcKJ
          WHEN ENDS_WITH('CCY-21pZ','Amt-Iytm') THEN
            SourceC.FieldA
          WHEN ENDS_WITH('Address-NG5Q','Amt-1WwX') THEN
            'Code-gsB1'
          WHEN ENDS_WITH(Table18.CodeGxmG,Table18.CountryLHs8) THEN
            Table18.CodefXu0
          ELSE
            'Amt-8v6A'
        END, IF('Code-cb2m' <> 'Customer-tOT0','Amt-uDUv',SourceB.FieldF))
        AS AccountR47D,
      CASE
        WHEN 'Country-Sm8W' <> 'Amt-sUDM' THEN
          IF(Table18.AddressLgaF <> 'Code-e0uR',Table17.FeeZaer,'CCY-sGyp')
        WHEN 'Code-o6jn' <> 'Fee-zo7y' THEN
          IF(
            STARTS_WITH(DerivedTable3.Feeg8kl,DerivedTable2.CountrywaWi),
            'Customer-FpZV', Table18.FeeZQMH)
        ELSE
          IF(
            DerivedTable3.CodeUeLq NOT LIKE Table16.Countryql5z, 'Account-RdQc',
            SourceE.FieldH)
      END
        AS AmtMQl2,
      IF(
        STARTS_WITH(SourceB.FieldG,'Fee-rB4N'),
        IF(
          STARTS_WITH(Table16.CCYe5Sb,'Address-Mk3V'), 'Fee-Ulfn',
          DerivedTable3.FeeD0Po),
        IF(SourceE.FieldA <> 'Fee-P35u','Amt-QocV','Address-T97C'))
        AS CCYnI4M,
      IF(
        DerivedTable3.FeeD0Po LIKE Table18.AccountGvoo,
        IF(
          STARTS_WITH('Country-60IR','Account-Wbjp'), DerivedTable3.FeeqkjX,
          DerivedTable1.AccountqcVc),
        IF(
          'Fee-8V0K' <> DerivedTable2.AmtmorU, DerivedTable1.CustomerWrlz,
          Table16.Countryql5z))
        AS AccountQJN6,
      IF(
        Table18.Customernueg LIKE 'Amt-ecqm',
        CASE
          WHEN STARTS_WITH(Table18.CCYLBVC,'CCY-VCbz') THEN
            Table16.CodenEXh
          WHEN 'Country-TJRI' != Table16.Countryql5z THEN
            DerivedTable2.Address7oYI
          WHEN 'Amt-kpT4' <> 'Country-0982' THEN
            SourceE.FieldA
          WHEN ENDS_WITH(DerivedTable3.CountrygMB4,'CCY-SoZB') THEN
            Table16.Amt6J7p
          WHEN SourceC.FieldF NOT LIKE 'Amt-bRKa' THEN
            'CCY-PfgF'
          ELSE
            'Country-cjWv'
        END, IF(
               'Fee-1v73' = 'Account-u60z', Table18.FeeU7V2,
               DerivedTable3.CountrygMB4))
        AS Address7HdX,
      IF(
        DerivedTable2.Amtp4ee LIKE 'Address-EPER',
        IF(
          DerivedTable1.AccountmNQo NOT LIKE Table17.Code9kal, Table16.CCYe5Sb,
          Table16.CCYe5Sb), IF(
                              DerivedTable3.FeepQGh = SourceD.FieldE,
                              Table16.AmtEAxo, Table18.CodefXu0))
        AS CCYn6Qy,
      IF(
        STARTS_WITH(DerivedTable3.FeeqkjX,'Code-sLGp'),
        IF(
          STARTS_WITH(SourceA.FieldB,'Account-N0Ct'), 'CCY-gzl6',
          'Customer-LCgF'),
        IF('Address-gFuJ' <> 'Address-YdCO','CCY-7d55','Amt-wGfm'))
        AS Amttall,
      IF(
        DerivedTable3.AccountViga LIKE Table18.CCYLfD9,
        CASE
          WHEN STARTS_WITH('CCY-XwPX',DerivedTable2.CodeDmva) THEN
            'Address-cHpz'
          WHEN 'Customer-U2Ni' <> DerivedTable1.AccountmNQo THEN
            Table17.CodenDQe
          WHEN STARTS_WITH(SourceA.FieldA,Table16.AmtEAxo) THEN
            Table18.CCYLfD9
          ELSE
            'Account-L5qL'
        END, IF(
               DerivedTable2.AccountrJZf != SourceB.FieldA,
               DerivedTable3.CountryoFFA, Table17.CodeJcKJ))
        AS AmtnOPL
    FROM
      testdataset.SourceE AS SourceE
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(SourceE.FieldJ,SourceA.FieldJ) OR
         ENDS_WITH('Fee-fL1F',SourceE.FieldC) OR
         'Account-Z3Wz' <> 'Customer-QMtu' AND 'Code-4BrC' != 'Account-vdqS')
      INNER JOIN testdataset.SourceB AS SourceB ON
        ('Country-CCIG' NOT LIKE SourceA.FieldG AND
         STARTS_WITH(SourceA.FieldH,SourceB.FieldA) AND
         ENDS_WITH('Code-v4sG',SourceB.FieldG) OR
         ENDS_WITH('Amt-hzHM','Account-nedr'))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (SourceA.FieldD NOT LIKE SourceE.FieldB OR
         ENDS_WITH(SourceC.FieldF,'Address-aLVJ') AND
         SourceC.FieldF NOT LIKE SourceB.FieldE OR
         ENDS_WITH('Address-DSPm',SourceB.FieldB))
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Account-jM60','Code-35a8') OR
         ENDS_WITH(SourceB.FieldF,'Country-oDjq') OR
         SourceE.FieldG <> 'Code-rcz2' OR
         'Address-o6yj' NOT LIKE SourceA.FieldC)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        ('Code-MDxq' LIKE SourceE.FieldC OR
         DerivedTable1.CustomerWrlz != 'Country-xtVF' OR
         'Country-2eOL' NOT LIKE 'Customer-sy4u' OR
         STARTS_WITH(SourceD.FieldH,SourceE.FieldI))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (ENDS_WITH(DerivedTable1.CCYXSkS,SourceC.FieldA) AND
         ENDS_WITH('Country-m0NA',DerivedTable1.AccountKwSP) AND
         ENDS_WITH('Fee-Agoj','Account-gyn8') OR
         SourceA.FieldD NOT LIKE 'Amt-ppJr')
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (STARTS_WITH(SourceD.FieldH,'CCY-9TQR') AND
         ENDS_WITH('CCY-GMTT',SourceD.FieldE) AND
         STARTS_WITH(DerivedTable3.Codeaqvh,DerivedTable3.CustomereBLI) OR
         SourceE.FieldB <> DerivedTable2.AmtNncS)
      LEFT JOIN Table16 AS Table16 ON
        (SourceA.FieldE = SourceC.FieldE AND
         DerivedTable1.AccountqcVc LIKE 'Country-NhIY' AND
         STARTS_WITH('Fee-UECr','Account-AJ4Y') AND
         STARTS_WITH('Customer-uaxe',DerivedTable1.Codeuhr9))
      RIGHT JOIN Table17 AS Table17 ON
        (ENDS_WITH(SourceE.FieldB,'Code-eYw9') AND
         STARTS_WITH('Customer-cApy','Amt-EoZ0') AND
         STARTS_WITH('Country-TVhe','Amt-ebJZ') OR 'CCY-RVZv' LIKE 'CCY-Q7MA')
      RIGHT JOIN Table18 AS Table18 ON
        (STARTS_WITH(Table16.AccountAYDB,SourceA.FieldH) OR
         SourceC.FieldA NOT LIKE DerivedTable3.CountrygMB4 OR
         Table18.CCYLfD9 = SourceB.FieldJ AND
         STARTS_WITH('CCY-DeSQ','Code-jGLE'))
    WHERE
      'Account-rOwd' != 'Address-1Kp1' AND
      ENDS_WITH('Customer-auHc',DerivedTable1.AccountqcVc) AND
      ENDS_WITH(SourceA.FieldJ,'Account-MIvd') AND
      Table17.Customero58L NOT LIKE 'CCY-Jjra' AND
      ENDS_WITH(SourceC.FieldA,Table17.FeeKZMF) AND
      ENDS_WITH('Fee-ghg6',Table18.Customernueg) OR
      ENDS_WITH(Table18.AddressLgaF,Table16.AddressjCYy) OR
      STARTS_WITH('Customer-Kizp',DerivedTable1.CustomerKrwR)
  ),
  Table20 AS (
    SELECT
      IF(
        STARTS_WITH(Table18.CCYLfD9,DerivedTable2.AmtNncS),
        CASE
          WHEN DerivedTable3.AccountViga = SourceC.FieldA THEN
            'Country-Zf9m'
          WHEN 'CCY-0OFX' != 'Code-Jus3' THEN
            'Address-CC95'
          WHEN STARTS_WITH(SourceE.FieldB,DerivedTable1.Addressrgjd) THEN
            DerivedTable1.CustomerKrwR
          WHEN 'Customer-VvEx' NOT LIKE 'Address-w15S' THEN
            'Amt-gN3J'
          ELSE
            'Country-Oyuq'
        END, IF(
               DerivedTable2.Account61kv NOT LIKE 'Customer-3x3l',
               SourceA.FieldD, Table19.AccountQJN6))
        AS CCYSSq4,
      IF(
        SourceB.FieldB != SourceC.FieldC,
        IF(
          'Account-bqg7' LIKE SourceC.FieldE, 'Customer-DDYc',
          DerivedTable2.Account61kv),
        CASE
          WHEN SourceC.FieldH != 'Customer-21nm' THEN
            Table16.CodeBPIQ
          WHEN STARTS_WITH(SourceB.FieldB,SourceE.FieldB) THEN
            'Address-CtVw'
          WHEN STARTS_WITH(Table18.CCYLBVC,Table18.CodedDLV) THEN
            SourceD.FieldG
          WHEN STARTS_WITH(DerivedTable2.AddresscM17,'Address-ZI9W') THEN
            'CCY-I5M1'
          WHEN DerivedTable3.CustomerULyd != 'Customer-gvSi' THEN
            'Customer-CEYa'
          ELSE
            Table19.Amttall
        END)
        AS Customerr1SJ,
      IF(
        STARTS_WITH(SourceD.FieldJ,'CCY-yvIP'),
        IF(
          ENDS_WITH(DerivedTable3.FeeCUde,'CCY-HboQ'), SourceA.FieldA,
          'Fee-XdFx'),
        IF(DerivedTable1.FeerBIS != 'Fee-jKhg',Table16.Amt6J7p,Table16.CCYe5Sb))
        AS FeegADO,
      CASE
        WHEN STARTS_WITH('CCY-xRVt',SourceB.FieldB) THEN
          IF(
            Table16.CustomeroVEf != 'Address-X1VX', SourceA.FieldG,
            SourceD.FieldC)
        WHEN SourceC.FieldH NOT LIKE DerivedTable3.CCYO742 THEN
          CASE
            WHEN DerivedTable2.AccountrJZf NOT LIKE Table18.CustomeriCu9 THEN
              'Fee-B1cm'
            WHEN ENDS_WITH('CCY-LwcK','Code-gHED') THEN
              'Fee-f7fP'
            WHEN 'Address-lxxd' = DerivedTable1.AccountKwSP THEN
              Table19.FeemHiU
            ELSE
              'Customer-2sqd'
          END
        ELSE
          CASE
            WHEN STARTS_WITH('Customer-zaDU','Address-FhWG') THEN
              'Fee-Trzv'
            WHEN STARTS_WITH(Table18.CodefXu0,'Account-SbHa') THEN
              SourceC.FieldJ
            WHEN 'Customer-vgM3' = DerivedTable1.Codeuhr9 THEN
              'Amt-jNOr'
            WHEN STARTS_WITH(SourceE.FieldJ,Table16.Countryql5z) THEN
              DerivedTable3.CountrygPvi
            ELSE
              'CCY-C7HH'
          END
      END
        AS AddressMTza,
      IF(
        Table16.AccountAYDB LIKE 'Customer-wLeF',
        IF(
          ENDS_WITH(Table18.FeeU7V2,Table16.CountryiYbf),
          DerivedTable1.CustomerWrlz, Table18.AddressLgaF),
        CASE
          WHEN ENDS_WITH(SourceE.FieldJ,DerivedTable3.CustomerLP0M) THEN
            'Country-gZPY'
          WHEN STARTS_WITH('Code-MoIB',SourceD.FieldJ) THEN
            'Code-bFz4'
          WHEN STARTS_WITH(SourceA.FieldA,DerivedTable3.FeeT7G9) THEN
            'Code-b4yQ'
          WHEN ENDS_WITH(DerivedTable2.AccounttVM1,'Fee-uTWX') THEN
            'Code-vw83'
          ELSE
            'Code-yHoq'
        END)
        AS AccountSWgd,
      CASE
        WHEN DerivedTable2.CountrywaWi <> 'Customer-jMFR' THEN
          IF(
            ENDS_WITH(Table16.CountryI2wJ,Table18.AmtyjS3), 'Customer-x5JO',
            'CCY-ehSN')
        WHEN SourceD.FieldF <> 'Amt-IaOe' THEN
          IF(
            'Address-rX6y' <> Table18.Address0pyc, 'Address-6BVg',
            DerivedTable2.Account61kv)
        WHEN STARTS_WITH(DerivedTable2.Address7oYI,Table16.AccountAYDB) THEN
          CASE
            WHEN DerivedTable3.AccountViga != Table19.CCYn6Qy THEN
              'Code-0s2w'
            WHEN STARTS_WITH('Amt-KK9w',SourceE.FieldH) THEN
              'Customer-wbPX'
            WHEN ENDS_WITH('Customer-1Do9','Address-yqlp') THEN
              DerivedTable3.FeeF6P4
            WHEN ENDS_WITH('Code-hRNn',SourceB.FieldF) THEN
              'Address-Ggix'
            ELSE
              'Code-vvu3'
          END
        WHEN STARTS_WITH('Code-GsjX','Address-VnVf') THEN
          IF('Account-ii9o' != SourceD.FieldH,SourceB.FieldB,SourceE.FieldG)
        WHEN SourceD.FieldF LIKE 'Address-qvA1' THEN
          IF(
            STARTS_WITH(DerivedTable2.CodeDmva,SourceD.FieldC), 'Address-dQAq',
            SourceC.FieldA)
        ELSE
          IF('Country-rc6q' NOT LIKE 'CCY-S1b4',SourceB.FieldA,'Address-SDvR')
      END
        AS FeeQ8Gw,
      IF(
        ENDS_WITH('Account-h7Gg','Code-uXd6'),
        IF(
          DerivedTable3.CountryoFFA NOT LIKE DerivedTable1.AccountqcVc,
          'Customer-nfKT', 'Address-vUEg'),
        CASE
          WHEN Table19.AccountR47D = Table16.AmtEAxo THEN
            SourceC.FieldI
          WHEN 'Account-KYSd' = 'Address-YMOQ' THEN
            SourceB.FieldB
          WHEN 'Country-ezWB' NOT LIKE 'Address-g7pR' THEN
            DerivedTable3.FeeD0Po
          WHEN ENDS_WITH('Customer-egYl',DerivedTable3.FeeCUde) THEN
            'Fee-jjvY'
          ELSE
            Table16.CodenEXh
        END)
        AS Countryed0j,
      IF(
        ENDS_WITH('Amt-cbrs','Account-AKet'),
        IF(
          Table19.FeemHiU <> DerivedTable2.AccountctCy, 'CCY-GBDG',
          SourceA.FieldA),
        IF(
          STARTS_WITH(Table16.AddressjCYy,DerivedTable1.CCYXSkS),
          Table18.CodedDLV, SourceC.FieldD))
        AS AccountbcBq,
      CASE
        WHEN ENDS_WITH(Table18.CustomeriCu9,'Amt-aS8L') THEN
          IF(
            Table19.AmtMQl2 <> DerivedTable3.Account4sxv, Table19.AccountR47D,
            'Address-jsm7')
        WHEN 'Customer-w2Zj' LIKE Table16.AddressFAql THEN
          IF(
            'CCY-E5Jn' LIKE Table18.CCY0fSg, 'Customer-u0EM',
            DerivedTable3.Account4sxv)
        ELSE
          IF(
            Table18.AmtyjS3 NOT LIKE SourceC.FieldC, DerivedTable3.CustomerULyd,
            Table18.AddressTikV)
      END
        AS AddressUUEQ,
      IF(
        STARTS_WITH(DerivedTable2.Account61kv,SourceD.FieldH),
        IF(
          ENDS_WITH('Customer-GPAn',DerivedTable2.AccountrJZf), 'Address-kE47',
          'Country-RHEi'),
        IF(SourceC.FieldD NOT LIKE SourceD.FieldJ,'Amt-MRP0',Table19.CCYn6Qy))
        AS Customerp0vT,
      IF(
        'CCY-9rXK' NOT LIKE 'Account-om1e',
        IF(
          STARTS_WITH(SourceA.FieldF,'Amt-sG6z'), 'Address-kMcK',
          SourceD.FieldA), IF(
                             DerivedTable2.AddressHdnu = SourceD.FieldB,
                             SourceB.FieldD, 'Customer-KUhF'))
        AS CustomerRAuR,
      IF(
        ENDS_WITH('Amt-5xyf',DerivedTable1.CustomerWpfX),
        IF(SourceB.FieldE = SourceD.FieldI,'Fee-xTeZ',DerivedTable3.FeeF6P4),
        IF(
          STARTS_WITH('Address-6XNz',Table18.AddressLgaF), SourceA.FieldG,
          DerivedTable3.CustomereBLI))
        AS FeeTJJp,
      IF(
        ENDS_WITH('Account-wUpQ','Country-C8ja'),
        IF(SourceD.FieldF NOT LIKE 'Fee-VBfJ','Amt-0wnt',Table18.CCY0fSg),
        IF(
          STARTS_WITH(SourceD.FieldC,'Amt-elVJ'), SourceD.FieldH,
          'Address-N0ma'))
        AS CCY3eE8,
      IF(
        'Amt-GSnY' LIKE SourceD.FieldJ,
        IF(
          STARTS_WITH('Code-vLox','Address-2oXE'), DerivedTable3.CCYO742,
          Table16.AmtEAxo),
        IF(SourceD.FieldH != 'Country-O90U','Amt-O8rP','Country-rXtd'))
        AS AddressKihx,
      IF(
        STARTS_WITH('Amt-OsGH',DerivedTable3.Code3Xip),
        IF(
          'Country-pPSl' NOT LIKE 'Address-8KXi', DerivedTable3.Feeg8kl,
          'Customer-laBd'), IF(
                              DerivedTable2.Amt885g = 'Amt-loak',
                              Table19.AccountR47D, SourceA.FieldJ))
        AS CountryUFLs
    FROM
      testdataset.DerivedTable2 AS DerivedTable2
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Address-ywqN' <> SourceA.FieldI AND
         ENDS_WITH(DerivedTable2.CountrywaWi,DerivedTable2.CodeDmva) OR
         STARTS_WITH('Country-kzeA',DerivedTable2.CountrywaWi) OR
         ENDS_WITH(DerivedTable2.CodeDmva,'Customer-Boek'))
      INNER JOIN testdataset.SourceB AS SourceB ON
        ('CCY-jAzc' != DerivedTable2.Address7oYI AND
         'Address-dJJc' != 'Address-nvHl' OR
         SourceB.FieldA = 'Address-cxKc' AND
         SourceA.FieldE NOT LIKE SourceB.FieldI)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (ENDS_WITH('Code-Tm5a',SourceA.FieldD) AND
         SourceA.FieldC LIKE 'Country-2Rup' OR
         'Amt-bIzz' LIKE SourceA.FieldI AND
         ENDS_WITH('Customer-W1ne',SourceC.FieldI))
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(SourceD.FieldJ,DerivedTable2.AccountrJZf) OR
         STARTS_WITH('Fee-1utJ','Code-bQq5') AND
         ENDS_WITH('Country-hllS','Amt-hOYV') AND
         'Customer-u3d6' != SourceD.FieldC)
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH('Country-V9lX',SourceA.FieldH) AND
         ENDS_WITH(DerivedTable2.AccountctCy,SourceE.FieldF) AND
         SourceC.FieldG <> DerivedTable2.AmtmorU OR
         SourceB.FieldD NOT LIKE SourceC.FieldA)
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('Code-hRHI','Code-A93W') AND
         ENDS_WITH(DerivedTable1.AmtzEYq,SourceD.FieldH) OR
         STARTS_WITH('Customer-fL5X','CCY-muEU') AND
         STARTS_WITH('Country-8YUq','Account-DFzy'))
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        ('CCY-C1Ji' = 'Fee-Q78w' AND
         DerivedTable2.CodeRziT LIKE 'Country-uK07' AND
         ENDS_WITH(DerivedTable2.Address7oYI,DerivedTable3.FeeqkjX) OR
         DerivedTable2.Amtp4ee = 'Amt-Yi0n')
      INNER JOIN Table16 AS Table16 ON
        (STARTS_WITH('Country-sUO7','Amt-Tvfb') AND
         ENDS_WITH('CCY-KZOy',SourceB.FieldA) AND
         STARTS_WITH('Code-Oaob','Fee-kEky') AND
         Table16.CodenEXh = DerivedTable3.AccountViga)
      INNER JOIN Table18 AS Table18 ON
        (ENDS_WITH(DerivedTable1.CustomerWrlz,'Country-ztlE') AND
         STARTS_WITH(DerivedTable3.Account4sxv,'Address-UppQ') AND
         'Code-645f' = DerivedTable3.CustomerLP0M AND
         DerivedTable3.Code3Xip <> Table18.Customernueg)
      RIGHT JOIN Table19 AS Table19 ON
        (ENDS_WITH('Code-RNJl','Fee-SxsH') AND
         STARTS_WITH(Table16.CountryI2wJ,'Code-Lx0f') OR
         Table16.CodeBPIQ = 'CCY-Frqr' OR SourceE.FieldG = SourceD.FieldC)
    WHERE
      ENDS_WITH('CCY-i7F8',SourceB.FieldA) OR
      ENDS_WITH(DerivedTable2.Address5RWP,DerivedTable2.Amtp4ee) OR
      ENDS_WITH('Code-3PRO','Country-Pu3E') OR
      'Account-RPP3' = DerivedTable1.Codeuhr9 AND
      ENDS_WITH(SourceB.FieldG,'Code-qn6J') OR
      ENDS_WITH('Country-vzqR','Code-VnRH') AND
      SourceB.FieldB <> Table18.Customernueg OR
      ENDS_WITH('Customer-Frw4',DerivedTable2.AddressHdnu)
  )
SELECT
  IF(
    STARTS_WITH(SourceD.FieldG,SourceD.FieldG),
    IF(
      ENDS_WITH(DerivedTable2.Amtp4ee,Table20.CCYSSq4), 'Amt-yzSU',
      SourceE.FieldA), IF(
                         Table20.AddressUUEQ LIKE SourceD.FieldE, 'Fee-30Mq',
                         DerivedTable2.AmtNncS))
    AS AccounteutQ,
  IF(
    'Account-Eaix' = 'Code-UvFt',
    CASE
      WHEN ENDS_WITH('Code-f9xP',DerivedTable3.FeeD0Po) THEN
        'Country-HouB'
      WHEN STARTS_WITH('Country-Ruys','Code-AlB2') THEN
        SourceA.FieldD
      WHEN STARTS_WITH(Table20.CountryUFLs,'Country-LZqC') THEN
        'Customer-yuDb'
      WHEN ENDS_WITH('Amt-7C3j','Address-JRAe') THEN
        'Country-zb9b'
      ELSE
        Table17.FeeKZMF
    END, IF('Country-FAiP' NOT LIKE 'CCY-jJzX','Fee-PNnT','Country-fjae'))
    AS CodelvtV,
  IF(
    'Fee-9Lds' LIKE 'Country-1LcY',
    CASE
      WHEN ENDS_WITH(Table17.AccountgFkW,'CCY-X5VM') THEN
        SourceC.FieldH
      WHEN ENDS_WITH('Account-VqeM','Country-BKlW') THEN
        'Amt-QS3S'
      ELSE
        DerivedTable3.FeeCUde
    END, IF(STARTS_WITH(SourceC.FieldB,'Amt-1CJb'),'Fee-yFYd','Code-TTtD'))
    AS CustomerCDJF,
  IF(
    'CCY-qbAv' = Table17.AccountgFkW,
    CASE
      WHEN STARTS_WITH(Table17.AccountgFkW,DerivedTable3.FeeF6P4) THEN
        DerivedTable2.CodeRziT
      WHEN STARTS_WITH(Table16.CodeyeDB,Table16.AmtEAxo) THEN
        SourceE.FieldC
      ELSE
        'Address-VjIj'
    END, IF(
           'Account-tXai' NOT LIKE 'Account-D5Tf', 'Customer-jZBQ',
           Table18.AccountGvoo))
    AS AddressWvnP,
  IF(
    STARTS_WITH(Table18.Customernueg,'CCY-GgiC'),
    IF(
      ENDS_WITH('Amt-mtbu',DerivedTable2.AccounttVM1), 'Amt-ydsE',
      DerivedTable3.FeeqkjX),
    IF(
      ENDS_WITH('Customer-YShs',DerivedTable3.FeeD0Po), 'Account-U18c',
      Table19.AccountQJN6))
    AS CCYY6No,
  IF(
    STARTS_WITH('Customer-M2sV','Account-1ZBH'),
    CASE
      WHEN DerivedTable2.AmtNncS NOT LIKE SourceC.FieldE THEN
        'Customer-mYeA'
      WHEN 'Fee-f111' LIKE Table18.CodefXu0 THEN
        'Customer-EL1A'
      WHEN STARTS_WITH('Customer-ZHSK','Amt-MH35') THEN
        'Country-9y2x'
      WHEN 'Address-qNh3' != 'Fee-174d' THEN
        'CCY-Cf6Y'
      WHEN Table20.AddressMTza NOT LIKE Table17.CodeJcKJ THEN
        Table17.Code9zfL
      ELSE
        'Account-10jU'
    END,
    IF(STARTS_WITH('Account-snp3',Table19.Address7HdX),'Fee-h1NP','CCY-inbM'))
    AS Country8VtM,
  IF(
    'Account-MAsJ' <> Table17.AccountgFkW,
    IF(
      'Customer-P2GH' NOT LIKE Table20.CustomerRAuR, Table18.CodedDLV,
      'Customer-7Qi6'),
    CASE
      WHEN 'Amt-ctdJ' <> 'CCY-k2Fj' THEN
        'Amt-GATr'
      WHEN 'Fee-nGvE' <> DerivedTable3.Feeg8kl THEN
        'Customer-EYVG'
      WHEN ENDS_WITH('Amt-G15H','Code-RXBY') THEN
        Table17.FeeZaer
      WHEN 'Customer-jYZe' NOT LIKE Table16.CustomeroVEf THEN
        'Address-eD77'
      ELSE
        SourceC.FieldJ
    END)
    AS AccountMw3l,
  IF(
    'Code-9uQk' != 'Code-sYfm',
    IF(
      STARTS_WITH(Table18.CCYLBVC,SourceC.FieldH), SourceD.FieldE,
      'Address-CBgs'), IF(
                         SourceA.FieldH = SourceA.FieldJ,
                         DerivedTable2.Address5RWP, SourceE.FieldI))
    AS FeezLpC,
  IF(
    SourceA.FieldA <> 'Account-cGy5',
    CASE
      WHEN 'CCY-eQil' != 'Customer-cMWv' THEN
        SourceE.FieldF
      WHEN STARTS_WITH(SourceA.FieldD,'Account-QHgV') THEN
        'Customer-BFLN'
      WHEN 'CCY-CwCj' LIKE 'Country-Wd7g' THEN
        'Customer-moQ7'
      WHEN Table16.CCYu27f LIKE SourceE.FieldA THEN
        'CCY-tGhs'
      WHEN STARTS_WITH(DerivedTable2.AmtNncS,Table18.CCYLfD9) THEN
        'Customer-qzdW'
      ELSE
        'Address-GPrK'
    END, IF(
           STARTS_WITH('Account-Jd8Y',Table17.AddressyGDx), 'Country-SbtS',
           'Code-U10A'))
    AS FeexvTE,
  IF(
    'Customer-WYpn' NOT LIKE 'Address-J8kA',
    CASE
      WHEN 'Country-33v9' = 'Amt-02qr' THEN
        DerivedTable3.CodeUeLq
      WHEN Table16.CustomeroVEf NOT LIKE 'Country-GbYD' THEN
        Table17.CodenDQe
      WHEN Table17.CodeJcKJ <> Table20.Customerp0vT THEN
        Table20.FeeQ8Gw
      WHEN ENDS_WITH(SourceC.FieldB,DerivedTable2.Address5RWP) THEN
        'Account-Qlpi'
      ELSE
        'CCY-3sz7'
    END, IF(
           'Fee-hMvN' <> 'Account-iTNY', DerivedTable2.CodeRziT,
           DerivedTable2.CodeDmva))
    AS Addressm87D,
  CASE
    WHEN STARTS_WITH('Amt-5b2g',Table18.CCY0fSg) THEN
      IF('Amt-SBuQ' != Table20.AddressKihx,Table19.FeecGpI,'Code-WdXr')
    WHEN 'Fee-HU4g' = 'Address-OiFo' THEN
      IF(
        STARTS_WITH('Amt-HnhW',SourceA.FieldA), Table17.AddressFydA,
        'Customer-Ggnl')
    ELSE
      IF(STARTS_WITH('Country-hgUf','Amt-i3Wn'),Table17.Customero58L,'CCY-Wkxh')
  END
    AS CodeXHEv,
  CASE
    WHEN STARTS_WITH('Customer-oekw',Table20.FeeQ8Gw) THEN
      IF(
        ENDS_WITH('Address-6kxn',Table20.FeeTJJp), Table17.FeepnbR,
        'Address-xSGO')
    WHEN SourceD.FieldH != 'Country-MRdy' THEN
      IF(
        'Account-8mWl' LIKE DerivedTable2.AccountrJZf, SourceE.FieldE,
        'Country-EI6m')
    WHEN 'Fee-N0gW' = Table20.AddressUUEQ THEN
      IF(Table20.Customerp0vT != 'Country-Wtts','Customer-K6Pj','Address-5K4I')
    WHEN 'Customer-0CeS' LIKE 'Customer-IVeu' THEN
      IF(
        Table16.CountryiYbf != DerivedTable2.Address5RWP, SourceC.FieldE,
        'CCY-6nph')
    ELSE
      IF(
        DerivedTable2.AddresscM17 = 'Address-DnO5', DerivedTable3.CodeVHWz,
        SourceA.FieldJ)
  END
    AS AddressB6KJ,
  IF(
    Table20.CustomerRAuR != 'CCY-X4o8',
    IF(STARTS_WITH('Amt-rxIZ','Country-t42a'),Table18.CodeO1XF,Table20.FeeQ8Gw),
    IF(
      STARTS_WITH(SourceA.FieldA,'Country-LJrs'), Table16.Code5tEA,
      SourceC.FieldH))
    AS FeesrBh,
  IF(
    'Country-wBhe' != SourceE.FieldF,
    IF('Country-sHL1' = 'Fee-L9j5',SourceA.FieldF,'Amt-gBcf'),
    IF('Amt-EO1J' != 'Code-Y7Rt','Customer-GOGe',Table17.CCYnSYx))
    AS CodeJM19,
  CASE
    WHEN DerivedTable2.Address5RWP NOT LIKE DerivedTable2.AmtmorU THEN
      IF(
        DerivedTable3.FeeCUde <> Table17.CCYnSYx, 'CCY-QuT5',
        DerivedTable2.AmtmorU)
    WHEN STARTS_WITH('CCY-ipUB','Account-pBT8') THEN
      IF(
        ENDS_WITH(DerivedTable2.AccountctCy,SourceA.FieldA), 'CCY-mqwc',
        DerivedTable2.AmtNncS)
    ELSE
      IF(
        STARTS_WITH(DerivedTable2.AccounttVM1,'Country-qxiQ'), 'Country-zAaQ',
        'Code-ykcH')
  END
    AS Account1SJF,
  IF(
    STARTS_WITH('Country-yHbk','Fee-Ckz0'),
    IF(
      ENDS_WITH(Table19.FeecGpI,Table19.Amttall), Table18.AccountGvoo,
      SourceD.FieldB), IF(
                         DerivedTable2.AmtNncS LIKE Table17.CCYnSYx,
                         DerivedTable3.FeeF6P4, Table16.AccountAYDB))
    AS Country5f1u
FROM
  testdataset.SourceD AS SourceD
  INNER JOIN testdataset.SourceA AS SourceA ON
    (ENDS_WITH(SourceA.FieldE,SourceA.FieldG) OR
     STARTS_WITH('Account-2FTJ',SourceA.FieldG) AND
     'CCY-g2Xa' != SourceD.FieldB AND 'Address-X3Ag' NOT LIKE 'Code-FGDc')
  RIGHT JOIN testdataset.SourceC AS SourceC ON
    (SourceA.FieldE <> SourceD.FieldG OR 'Address-N98d' = 'Account-UB88' OR
     ENDS_WITH('CCY-426j',SourceD.FieldA) OR 'Fee-trfn' LIKE SourceC.FieldI)
  RIGHT JOIN testdataset.SourceE AS SourceE ON
    ('Country-msWm' <> SourceD.FieldE AND
     STARTS_WITH('Fee-BbMI',SourceA.FieldA) AND
     'Amt-mOXz' <> 'Fee-RWod' OR ENDS_WITH(SourceE.FieldD,SourceD.FieldC))
  INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
    ('Address-Kj30' NOT LIKE SourceA.FieldB OR
     ENDS_WITH(SourceD.FieldC,SourceA.FieldH) AND
     'CCY-Atrj' != 'Amt-rwrD' AND
     ENDS_WITH(DerivedTable2.AccountctCy,'Customer-Klxr'))
  INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
    (SourceC.FieldB != 'Fee-1nC2' AND
     ENDS_WITH('Code-yJZc',DerivedTable3.Feeg8kl) AND
     'Country-Yi8b' != 'Address-iu0Q' OR STARTS_WITH('Account-VC1H','Amt-zgNX'))
  RIGHT JOIN Table16 AS Table16 ON
    ('Address-LmnT' <> DerivedTable3.CountrygPvi AND
     STARTS_WITH('Account-YEzo',DerivedTable3.CustomerLP0M) OR
     ENDS_WITH(DerivedTable3.FeeqkjX,SourceE.FieldG) OR
     STARTS_WITH('Account-7wHr',DerivedTable3.FeeD0Po))
  RIGHT JOIN Table17 AS Table17 ON
    (STARTS_WITH(Table17.CodenDQe,SourceA.FieldA) AND
     'Amt-i6RI' = 'Customer-Sei2' AND
     'CCY-q8ef' = Table16.FeeLFId OR ENDS_WITH(SourceA.FieldE,SourceC.FieldJ))
  INNER JOIN Table18 AS Table18 ON
    ('Code-hap7' = 'Customer-jn4L' AND
     ENDS_WITH('Address-Q4bC',Table16.CustomeroVEf) AND
     'Fee-b1FP' NOT LIKE Table17.FeeKZMF OR
     ENDS_WITH(DerivedTable2.AmtNncS,SourceA.FieldC))
  LEFT JOIN Table19 AS Table19 ON
    (SourceA.FieldE <> 'Account-zzfq' AND
     ENDS_WITH('Account-QAgL',DerivedTable2.AddresscM17) AND
     STARTS_WITH('Code-5xHS','Fee-Htal') AND 'Amt-hPWl' <> 'Fee-9BFI')
  RIGHT JOIN Table20 AS Table20 ON
    (STARTS_WITH(SourceA.FieldA,'CCY-1Mhr') AND 'Amt-U04u' = Table19.FeemHiU AND
     ENDS_WITH('Address-drkR',Table20.CCYSSq4) OR
     ENDS_WITH('Code-GT2m',Table16.AddressjCYy))
WHERE
  ENDS_WITH('Code-rQEE','Code-HEoY') AND
  ENDS_WITH('Account-8yJb',Table20.AddressMTza) AND
  'Amt-1NI3' <> SourceE.FieldB AND ENDS_WITH(Table16.CountryiYbf,'Amt-8O6d') OR
  ENDS_WITH(Table16.AmtEAxo,'Amt-Aukw') OR SourceD.FieldE LIKE 'Fee-CEcS' AND
  Table17.CountryIzU9 = SourceE.FieldI AND
  ENDS_WITH('Code-9Wxz',Table17.FeeZaer)