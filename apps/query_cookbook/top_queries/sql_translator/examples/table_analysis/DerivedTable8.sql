CREATE OR REPLACE TABLE testdataset.DerivedTable8 AS
WITH
  Table36 AS (
    SELECT
      CASE
        WHEN STARTS_WITH(DerivedTable4.CustomerCDJF,SourceB.FieldC) THEN
          IF(
            'Fee-ZQ4B' = 'Address-MDAq', DerivedTable4.CCYY6No,
            DerivedTable4.CodelvtV)
        WHEN STARTS_WITH(DerivedTable3.Codeaqvh,'Amt-wUwE') THEN
          CASE
            WHEN 'Account-Uh6H' <> SourceD.FieldF THEN
              DerivedTable5.CountrynpDj
            WHEN STARTS_WITH(SourceC.FieldJ,'Country-ZrFc') THEN
              SourceD.FieldD
            WHEN 'Code-5Gge' != 'Amt-jfZt' THEN
              'Customer-PH6v'
            WHEN ENDS_WITH(DerivedTable2.Account61kv,SourceE.FieldA) THEN
              'CCY-A4YG'
            WHEN DerivedTable7.CodeOp8k != SourceD.FieldJ THEN
              DerivedTable4.FeexvTE
            ELSE
              DerivedTable3.CountrygPvi
          END
        WHEN SourceB.FieldA = 'Country-diZ5' THEN
          IF(
            STARTS_WITH('Code-1Gm0',DerivedTable5.FeeogF5), 'CCY-dHev',
            'Code-q6CF')
        WHEN DerivedTable4.CodeJM19 LIKE 'Amt-uSwW' THEN
          IF(
            ENDS_WITH('Customer-qN4o','Fee-9mHZ'), 'Address-3ZAG',
            DerivedTable4.CCYY6No)
        WHEN ENDS_WITH(DerivedTable3.FeeT7G9,DerivedTable2.AddressHdnu) THEN
          IF(
            ENDS_WITH(SourceD.FieldD,'Customer-ItwV'), DerivedTable4.FeesrBh,
            'Amt-KPlJ')
        ELSE
          IF(
            ENDS_WITH(SourceC.FieldA,DerivedTable3.CountryoFFA), SourceE.FieldD,
            'Country-zLGh')
      END
        AS CountryrZ1u,
      IF(
        STARTS_WITH(DerivedTable3.FeepQGh,DerivedTable5.CCYQK2Q),
        IF(SourceA.FieldD = DerivedTable3.FeeT7G9,'Address-3PkI','Amt-ksby'),
        IF(DerivedTable2.CodeDmva = SourceA.FieldJ,'Amt-eolh','CCY-pdsG'))
        AS FeeKF3x,
      IF(
        'Address-vqWE' = DerivedTable4.CustomerCDJF,
        CASE
          WHEN DerivedTable5.FeeogF5 = 'CCY-V9oz' THEN
            SourceC.FieldE
          WHEN 'Customer-yccL' != SourceB.FieldI THEN
            'Country-2QME'
          ELSE
            DerivedTable4.Addressm87D
        END, IF(
               'Country-mDrO' NOT LIKE DerivedTable2.AccountrJZf,
               DerivedTable2.Account61kv, DerivedTable4.CodeJM19))
        AS CustomerpfFm,
      IF(
        STARTS_WITH('Account-Ia1Y','Amt-SEfG'),
        IF(
          DerivedTable4.AccountMw3l NOT LIKE SourceC.FieldG, 'Address-DZ8t',
          'Account-z1lu'),
        CASE
          WHEN STARTS_WITH('Amt-KEjM','Code-u3rY') THEN
            'Country-Ha3f'
          WHEN 'Amt-kppf' != DerivedTable7.CCYZD20 THEN
            'Account-JPiw'
          WHEN SourceA.FieldF <> DerivedTable4.CCYY6No THEN
            'Account-gWZx'
          WHEN STARTS_WITH(DerivedTable2.Amtp4ee,'Account-BXoI') THEN
            'Fee-wYuA'
          WHEN SourceB.FieldF <> SourceA.FieldC THEN
            'CCY-QkIp'
          ELSE
            SourceB.FieldF
        END)
        AS CodeuSJZ,
      IF(
        SourceC.FieldE = SourceC.FieldC,
        IF(
          SourceE.FieldF LIKE 'Account-ZWzn', DerivedTable3.Code3Xip,
          SourceC.FieldH),
        CASE
          WHEN STARTS_WITH(DerivedTable4.AddressWvnP,DerivedTable7.Amt3faU) THEN
            'Account-kquw'
          WHEN ENDS_WITH(SourceA.FieldF,DerivedTable5.AddressV4AX) THEN
            'Address-UpFy'
          ELSE
            SourceC.FieldA
        END)
        AS FeelElc,
      IF(
        STARTS_WITH('CCY-OVAd','CCY-5knb'),
        CASE
          WHEN STARTS_WITH(SourceE.FieldH,SourceB.FieldC) THEN
            'Account-xoEQ'
          WHEN 'Account-NCkR' <> DerivedTable4.Country5f1u THEN
            'Country-pTTZ'
          ELSE
            'Customer-wyV7'
        END, CASE
               WHEN 'Country-OdHz' <> 'Account-yXMx' THEN
                 SourceC.FieldI
               WHEN STARTS_WITH(DerivedTable3.AccountViga,SourceD.FieldA) THEN
                 'Country-DwbU'
               WHEN STARTS_WITH('CCY-a0aV',DerivedTable7.FeeBS3r) THEN
                 'Fee-T5Km'
               ELSE
                 'Customer-JMp5'
             END)
        AS CCYgOyc,
      IF(
        ENDS_WITH(DerivedTable4.Country5f1u,DerivedTable2.Address5RWP),
        CASE
          WHEN
            SourceA.FieldH = 'Amt-WBPc'
          THEN
            SourceD.FieldF
          WHEN
            STARTS_WITH(DerivedTable7.CountryXTJ7,DerivedTable7.AddressuSTK)
          THEN
            'CCY-cRl8'
          WHEN
            DerivedTable7.AddressgmSt = 'CCY-AGwg'
          THEN
            'Account-nNqr'
          WHEN
            ENDS_WITH('Account-kjOZ','Fee-jTMG')
          THEN
            'Country-TkPr'
          ELSE
            'Address-yyS9'
        END, IF(
               DerivedTable3.CustomerLP0M <> 'Fee-04DM', SourceC.FieldE,
               DerivedTable3.FeeD0Po))
        AS CodezIk3,
      IF(
        STARTS_WITH(SourceC.FieldE,DerivedTable3.FeeT7G9),
        IF('Code-avgN' = 'Amt-V7Qr','Amt-5wf0',DerivedTable3.AccountViga),
        IF('Fee-3wuA' = 'Fee-QZlz',SourceD.FieldC,SourceE.FieldD))
        AS AddressyaZ0,
      CASE
        WHEN 'Fee-jOGE' LIKE DerivedTable3.FeeD0Po THEN
          IF(
            STARTS_WITH(SourceE.FieldF,DerivedTable3.FeeT7G9), 'Account-5i06',
            'Account-malj')
        WHEN 'Fee-Xu9a' <> DerivedTable3.FeeD0Po THEN
          IF(
            'Account-280Q' != 'Address-Nz7D', DerivedTable2.AddressHdnu,
            'Amt-ZVEo')
        WHEN SourceA.FieldB NOT LIKE DerivedTable4.CodeJM19 THEN
          IF(
            STARTS_WITH(SourceD.FieldE,'CCY-CvP7'), 'Customer-K70C',
            SourceB.FieldB)
        ELSE
          IF(
            STARTS_WITH(DerivedTable3.CountrygPvi,SourceC.FieldD),
            DerivedTable2.AmtmorU, 'Country-INOm')
      END
        AS CountryItp6,
      IF(
        'Customer-oa6z' <> 'Account-88cs',
        CASE
          WHEN ENDS_WITH('Customer-MbRu',DerivedTable3.CodeUeLq) THEN
            DerivedTable3.FeeqkjX
          WHEN 'Country-A4hu' LIKE 'Account-9gcC' THEN
            DerivedTable2.AmtmorU
          WHEN DerivedTable7.AccountKbFf LIKE 'Address-BBXC' THEN
            'Amt-aLL8'
          ELSE
            SourceD.FieldD
        END, CASE
               WHEN 'Address-iE9w' LIKE 'Country-29g1' THEN
                 SourceB.FieldI
               WHEN STARTS_WITH('Customer-XYxH',SourceB.FieldC) THEN
                 'Fee-3Z0w'
               WHEN ENDS_WITH('CCY-xBNf','Address-bamW') THEN
                 DerivedTable7.AddressuSTK
               ELSE
                 'Amt-ZclS'
             END)
        AS Code77fF,
      IF(
        'Code-zUhO' <> DerivedTable3.Account4sxv,
        IF(
          DerivedTable3.CCYO742 != 'Country-0Fse', 'Account-kcoB',
          DerivedTable7.CountryrPg8),
        IF(
          STARTS_WITH(SourceD.FieldA,SourceB.FieldH), SourceC.FieldD,
          'Code-E6hm'))
        AS AmtCXms
    FROM
      testdataset.DerivedTable2 AS DerivedTable2
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH(SourceA.FieldD,DerivedTable2.Address7oYI) AND
         'CCY-WT6G' NOT LIKE 'Customer-LDMM' OR
         STARTS_WITH(DerivedTable2.Amt885g,'Code-CCiZ') OR
         STARTS_WITH(DerivedTable2.Amt885g,DerivedTable2.Amt885g))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceB.FieldD,'Address-QEZi') AND
         STARTS_WITH(SourceB.FieldG,'Customer-thmp') OR
         ENDS_WITH(DerivedTable2.AmtNncS,'Fee-ML1H') OR
         SourceB.FieldF NOT LIKE SourceA.FieldG)
      INNER JOIN testdataset.SourceC AS SourceC ON
        (SourceB.FieldA <> SourceC.FieldH AND
         STARTS_WITH(SourceA.FieldG,DerivedTable2.Amtp4ee) OR
         'Fee-UMeP' <> SourceC.FieldB AND 'Amt-iGYI' LIKE SourceA.FieldH)
      INNER JOIN testdataset.SourceD AS SourceD ON
        (SourceB.FieldJ <> 'Amt-oNqC' OR
         STARTS_WITH(SourceC.FieldC,DerivedTable2.AccounttVM1) OR
         SourceB.FieldG LIKE 'Customer-qZoJ' OR
         SourceC.FieldD != SourceB.FieldD)
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (SourceC.FieldB NOT LIKE 'Country-NH6c' OR
         STARTS_WITH('CCY-mcr7','Customer-on0W') OR
         STARTS_WITH(SourceD.FieldA,'Fee-xcVP') OR
         'Customer-9gQN' = 'Account-FYxC')
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        ('Code-VPd5' != 'Address-MSmn' AND 'Account-U3tA' = 'Account-UHFG' AND
         'Fee-0Jkw' = 'Address-MoSH' OR STARTS_WITH('Amt-QMwH','Country-EVZU'))
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH('Code-6fa2',DerivedTable4.CodelvtV) AND
         STARTS_WITH(SourceE.FieldC,SourceD.FieldC) AND
         DerivedTable2.AccounttVM1 LIKE 'Account-Tbbw' OR
         'Account-5ooY' <> DerivedTable2.AddresscM17)
      RIGHT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        ('Account-sMKf' <> 'Account-ASho' OR
         ENDS_WITH('Fee-8CL1',SourceB.FieldC) AND
         STARTS_WITH('Fee-ESCO',DerivedTable3.CustomereBLI) AND
         'Amt-Upky' != DerivedTable2.CodeRziT)
      RIGHT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        ('Customer-frNH' != 'Address-3Hbh' OR
         'Address-kMhe' LIKE DerivedTable4.AddressB6KJ OR
         DerivedTable5.AddressV4AX = DerivedTable2.Account61kv AND
         SourceA.FieldC <> DerivedTable3.CodeVHWz)
    WHERE
      'Country-Mp3X' NOT LIKE 'Customer-eoEt' OR
      'Amt-UQvr' NOT LIKE 'Amt-VPus' OR
      SourceE.FieldF LIKE DerivedTable3.CountrygPvi OR
      ENDS_WITH(DerivedTable2.CodeRziT,'Customer-6ATY') AND
      STARTS_WITH(SourceE.FieldA,'Amt-SP82') OR
      'Customer-QXdO' NOT LIKE 'CCY-8ZSN' OR
      STARTS_WITH(DerivedTable7.Amt3faU,'Address-PATe') OR
      'Account-Enut' != 'Address-sg99'
  ),
  Table37 AS (
    SELECT
      IF(
        ENDS_WITH(SourceC.FieldG,DerivedTable2.AmtmorU),
        IF('Fee-We8C' NOT LIKE 'Amt-KNXO','Address-KurO','Fee-6nwg'),
        IF(
          STARTS_WITH(DerivedTable1.AccountfEJK,DerivedTable3.CustomerULyd),
          DerivedTable3.Account4sxv, 'Amt-BKst'))
        AS Amt891X,
      IF(
        'CCY-q71q' <> DerivedTable2.Address7oYI,
        IF(
          'Amt-clK1' NOT LIKE 'Country-2rii', 'Customer-s3oU',
          DerivedTable2.AccounttVM1),
        IF(
          DerivedTable3.CountrygMB4 NOT LIKE DerivedTable6.AmtCCH1, 'Code-vqaE',
          SourceB.FieldF))
        AS AccountgNpd,
      IF(
        DerivedTable2.Address7oYI NOT LIKE SourceC.FieldJ,
        IF(
          SourceD.FieldD = DerivedTable4.Country5f1u, DerivedTable4.Country8VtM,
          'Address-uKGJ'),
        IF(STARTS_WITH('Amt-zFyP','Country-dK5Q'),'Code-rCOR','Amt-pQnB'))
        AS CCY4GHR,
      IF(
        STARTS_WITH('Customer-xwWK',SourceB.FieldG),
        CASE
          WHEN ENDS_WITH(DerivedTable3.CountryoFFA,'Fee-O04s') THEN
            SourceD.FieldG
          WHEN STARTS_WITH(DerivedTable2.Address7oYI,DerivedTable6.FeeEU6a) THEN
            DerivedTable7.CountryrPg8
          ELSE
            'Customer-ntpO'
        END, IF(
               STARTS_WITH('Account-Pfql','Fee-xN8l'), 'Customer-jpZG',
               DerivedTable6.AmtW2w8))
        AS Code3cb6,
      IF(
        STARTS_WITH('Account-F2Wx',DerivedTable2.Address5RWP),
        IF(
          ENDS_WITH('Fee-cPTu',DerivedTable7.CountryrPg8),
          DerivedTable6.AddressD7PZ, 'Amt-Lpaj'),
        IF(
          STARTS_WITH(DerivedTable1.AccountqcVc,'CCY-FIsX'), 'Code-lfNR',
          'Code-O7KV'))
        AS Code4OwH,
      IF(
        STARTS_WITH(SourceB.FieldJ,DerivedTable4.AddressWvnP),
        CASE
          WHEN ENDS_WITH('Fee-BPAx','CCY-jhYn') THEN
            'Address-TcTc'
          WHEN STARTS_WITH('Fee-yqos','CCY-cVXN') THEN
            DerivedTable6.AddressYFBr
          WHEN DerivedTable3.FeeF6P4 != DerivedTable1.AmtR86V THEN
            'Address-PwaU'
          ELSE
            SourceD.FieldJ
        END, CASE
               WHEN STARTS_WITH('Account-cSBR',DerivedTable3.FeeF6P4) THEN
                 'Country-vgqm'
               WHEN ENDS_WITH(DerivedTable4.FeezLpC,'Amt-DKrl') THEN
                 DerivedTable3.FeepQGh
               ELSE
                 DerivedTable2.AccountctCy
             END)
        AS CCYPw5k,
      IF(
        'Fee-7Eb9' <> DerivedTable6.AddressWejH,
        IF(
          SourceB.FieldB != SourceC.FieldC, DerivedTable4.AccountMw3l,
          'CCY-7WMA'),
        IF(
          ENDS_WITH(DerivedTable4.AccountMw3l,DerivedTable1.AddressoeLk),
          'Code-ylFE', 'Code-yxNV'))
        AS Feesdmj,
      IF(
        ENDS_WITH(DerivedTable3.FeeD0Po,'Country-duo3'),
        IF(
          SourceB.FieldI != DerivedTable3.Code3Xip, 'Fee-REPd',
          DerivedTable7.CCYZD20),
        IF(
          'Country-8Si4' <> DerivedTable1.CustomerWpfX, 'Code-LBuV',
          SourceC.FieldI))
        AS CCYiCm5,
      IF(
        STARTS_WITH(DerivedTable2.AccountctCy,'Customer-xWr9'),
        CASE
          WHEN 'Amt-PVbT' <> DerivedTable6.AddressPpXC THEN
            DerivedTable6.CodeY4Yb
          WHEN 'Country-pYF1' = DerivedTable1.AmtzEYq THEN
            'Address-xgWv'
          ELSE
            SourceC.FieldC
        END,
        IF('Fee-fFZT' != 'Customer-AHEw','Code-MdYc',DerivedTable6.FeeN99q))
        AS Customerv7eo,
      IF(
        'Customer-q2y2' LIKE DerivedTable2.Amt885g,
        CASE
          WHEN DerivedTable6.CountryD7gw <> SourceD.FieldF THEN
            'Address-HnCJ'
          WHEN DerivedTable7.AddressuSTK = 'Amt-YLJE' THEN
            'Country-eJjS'
          WHEN ENDS_WITH('Country-Wd9y',SourceA.FieldJ) THEN
            DerivedTable6.AddressYFBr
          ELSE
            'Account-T10R'
        END, IF(
               STARTS_WITH('CCY-BVXG','Code-pYer'), 'Customer-msHe',
               DerivedTable3.FeeqkjX))
        AS Amtpi4k,
      CASE
        WHEN SourceA.FieldB LIKE SourceD.FieldJ THEN
          CASE
            WHEN DerivedTable2.Address5RWP != 'Account-hC8d' THEN
              SourceC.FieldF
            WHEN SourceC.FieldJ != 'CCY-Xv6M' THEN
              'Amt-xCP1'
            ELSE
              'Country-Fqxq'
          END
        WHEN 'Account-BTeB' NOT LIKE 'Address-wguh' THEN
          CASE
            WHEN ENDS_WITH(DerivedTable2.Address5RWP,'CCY-zxiB') THEN
              SourceC.FieldB
            WHEN 'Fee-Fo5f' <> 'Fee-4Q52' THEN
              DerivedTable4.FeezLpC
            WHEN 'Address-rcUp' <> 'Customer-9gcb' THEN
              'Code-gDB5'
            WHEN 'Amt-QdNt' <> DerivedTable4.Country8VtM THEN
              DerivedTable7.AddressuSTK
            WHEN 'Fee-w5YU' LIKE SourceD.FieldE THEN
              SourceD.FieldJ
            ELSE
              'CCY-LJF2'
          END
        WHEN STARTS_WITH('Country-gijH','CCY-ds67') THEN
          IF(SourceB.FieldC <> 'Fee-1K1R',SourceD.FieldI,'Fee-BjSj')
        ELSE
          CASE
            WHEN 'Customer-gDkU' = 'Customer-wopF' THEN
              'Account-qyZZ'
            WHEN ENDS_WITH(SourceA.FieldB,SourceA.FieldF) THEN
              SourceD.FieldC
            WHEN STARTS_WITH(SourceB.FieldE,'Fee-lABU') THEN
              'Customer-JqfS'
            WHEN DerivedTable2.Account61kv NOT LIKE 'Fee-6dV6' THEN
              'Address-0v8Y'
            ELSE
              DerivedTable7.CCYZD20
          END
      END
        AS CustomerSKc6,
      IF(
        ENDS_WITH(DerivedTable4.Account1SJF,'Amt-Ly7j'),
        IF(
          ENDS_WITH('Country-iNYB','Amt-0ZV4'), DerivedTable1.AmtR86V,
          DerivedTable6.AddressPIbO),
        CASE
          WHEN ENDS_WITH('Fee-50J2','Amt-7M0k') THEN
            'Fee-U9qF'
          WHEN STARTS_WITH(DerivedTable1.AmtR86V,DerivedTable4.CodelvtV) THEN
            DerivedTable6.FeeN99q
          WHEN 'Fee-Ld7M' != SourceD.FieldE THEN
            DerivedTable7.CountrypfA6
          WHEN ENDS_WITH('Account-xteA','Amt-PGyY') THEN
            DerivedTable1.CountryV1io
          WHEN 'Amt-ypTS' != DerivedTable6.CountryD7gw THEN
            DerivedTable2.AccountrJZf
          ELSE
            SourceA.FieldB
        END)
        AS Countryu2fW,
      IF(
        DerivedTable7.Address3l5L LIKE 'Code-Viue',
        IF(
          'CCY-fJoK' <> 'Code-nhCO', DerivedTable6.FeeEU6a,
          DerivedTable6.CustomerCZg3),
        IF(
          DerivedTable2.AccountrJZf LIKE 'CCY-6t1d', 'Account-LttG',
          'Country-3Tl5'))
        AS Customer7IDY,
      IF(
        'CCY-nNDm' NOT LIKE DerivedTable3.CountryoFFA,
        IF(
          DerivedTable2.AmtmorU NOT LIKE DerivedTable3.Feeg8kl,
          DerivedTable4.CodeXHEv, SourceD.FieldD),
        IF(DerivedTable1.FeerBIS LIKE 'CCY-NF8T','Fee-djIU','Customer-nMRW'))
        AS FeevHoM,
      CASE
        WHEN STARTS_WITH(DerivedTable6.FeeN99q,DerivedTable4.AccounteutQ) THEN
          IF(
            DerivedTable4.FeezLpC = 'Address-jvBy', 'Account-qMMR',
            DerivedTable4.CCYY6No)
        WHEN ENDS_WITH(SourceB.FieldE,DerivedTable3.CountryoFFA) THEN
          CASE
            WHEN
              DerivedTable6.CodeY4Yb NOT LIKE 'Address-5Bi9'
            THEN
              DerivedTable3.FeeT7G9
            WHEN
              STARTS_WITH(DerivedTable4.FeexvTE,DerivedTable4.AccountMw3l)
            THEN
              'Amt-xN45'
            ELSE
              'Country-cW5K'
          END
        WHEN ENDS_WITH(DerivedTable3.FeeT7G9,SourceC.FieldJ) THEN
          IF(
            STARTS_WITH(DerivedTable3.AccountViga,'Account-jKfY'),
            DerivedTable7.CountrypfA6, 'Fee-7AbA')
        WHEN ENDS_WITH('Account-iXLV',DerivedTable4.CustomerCDJF) THEN
          CASE
            WHEN 'Code-zyrB' != 'Country-REbe' THEN
              DerivedTable3.CustomerULyd
            WHEN STARTS_WITH(DerivedTable3.FeeD0Po,DerivedTable3.FeeqkjX) THEN
              DerivedTable7.CountryrPg8
            ELSE
              'Customer-gnjc'
          END
        WHEN STARTS_WITH('Amt-23j7','Account-VKos') THEN
          IF('Code-pV6P' = 'CCY-aDoI','Account-iYPh',DerivedTable7.FeeLG30)
        ELSE
          IF(
            ENDS_WITH('Country-AWwh','Country-1frB'), DerivedTable2.AmtmorU,
            'Account-TYF0')
      END
        AS CCYc87y,
      IF(
        STARTS_WITH(SourceC.FieldG,DerivedTable2.CountrywaWi),
        IF(
          ENDS_WITH(DerivedTable2.Amtp4ee,'Customer-NhWY'),
          DerivedTable7.CodeOp8k, 'Amt-a4zH'),
        IF(
          SourceA.FieldA NOT LIKE DerivedTable7.CodeOp8k,
          DerivedTable2.AccountrJZf, 'CCY-U8Pn'))
        AS Customer3tVN,
      IF(
        STARTS_WITH(SourceD.FieldF,DerivedTable3.Codeaqvh),
        IF(
          DerivedTable1.AddressoeLk <> DerivedTable4.CodelvtV, 'Fee-6AON',
          'CCY-3ltd'),
        IF(
          DerivedTable3.AccountViga LIKE DerivedTable2.Account61kv,
          DerivedTable1.CustomerKrwR, SourceC.FieldE))
        AS Customer3id9,
      IF(
        'Address-dtOR' LIKE 'Amt-rEie',
        IF(ENDS_WITH('CCY-iP4O',SourceD.FieldH),'Address-PbtW','Fee-ppJk'),
        IF(SourceA.FieldG != 'Amt-idLH','Account-eRFp','Code-Vd04'))
        AS Amte2OU,
      IF(
        'CCY-hTHM' != 'Account-BqUs',
        IF(
          DerivedTable6.CountryD7gw LIKE SourceC.FieldF,
          DerivedTable6.CustomerxLhn, SourceB.FieldI),
        IF('Code-EKb4' <> DerivedTable6.AddressPIbO,SourceB.FieldC,'Amt-CGyk'))
        AS CustomerdBAq
    FROM
      testdataset.DerivedTable6 AS DerivedTable6
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH(DerivedTable6.CodebnT9,SourceA.FieldI) OR
         ENDS_WITH('Fee-ds7p','Country-BPKy') AND
         STARTS_WITH('Amt-s4Pc',DerivedTable6.AmtW2w8) AND
         DerivedTable6.AddressWejH NOT LIKE DerivedTable6.CodeY4Yb)
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH('Amt-61tT','Customer-z3ri') AND
         ENDS_WITH('Country-iQiq',SourceB.FieldI) AND
         STARTS_WITH('Code-uvSZ',DerivedTable6.CodebnT9) OR
         ENDS_WITH('Address-LShX',SourceA.FieldJ))
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        ('Address-WWFt' LIKE SourceB.FieldD OR
         ENDS_WITH(DerivedTable6.FeeFMCK,SourceC.FieldE) OR
         STARTS_WITH('Code-kIQw','Account-UaxL') AND
         SourceC.FieldB LIKE 'Code-DzLz')
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(SourceD.FieldF,'Code-i79W') OR
         STARTS_WITH(SourceA.FieldG,SourceB.FieldE) OR
         SourceD.FieldG != 'Address-gJAH' OR
         ENDS_WITH('Customer-MztZ',DerivedTable6.AmtW2w8))
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH(DerivedTable6.FeeEU6a,DerivedTable6.FeeEU6a) OR
         'Amt-rY2S' = 'Code-Yn23' AND
         ENDS_WITH('Fee-psrJ',SourceC.FieldC) AND 'Fee-XG9d' != 'Code-deAZ')
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (STARTS_WITH(SourceC.FieldC,'Customer-amy1') OR
         'Address-KJ76' <> 'Account-HMC5' OR
         ENDS_WITH(DerivedTable1.Codeuhr9,SourceB.FieldD) AND
         STARTS_WITH('Account-brBh',DerivedTable2.Address7oYI))
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (ENDS_WITH(DerivedTable3.FeeCUde,DerivedTable3.CodeUeLq) OR
         ENDS_WITH(DerivedTable3.CountrygPvi,SourceD.FieldI) OR
         'Fee-LboN' != DerivedTable6.AddressD7PZ OR
         DerivedTable2.AccountrJZf LIKE DerivedTable1.CustomerKrwR)
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (ENDS_WITH('Fee-XiHo','Address-X4eq') AND
         STARTS_WITH('Fee-cb4c',DerivedTable2.AccounttVM1) OR
         'CCY-878J' = 'Address-QCPB' OR 'Code-hOLi' = 'Code-uHDJ')
      RIGHT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        ('Account-i5KD' <> 'Address-0q69' AND
         STARTS_WITH(DerivedTable6.AddressPIbO,DerivedTable2.AmtmorU) OR
         'Address-pDHY' <> 'Address-8qGg' AND
         DerivedTable2.AccountctCy = 'Customer-eKHR')
    WHERE
      'Country-4QeQ' != DerivedTable1.CountryV1io OR
      ENDS_WITH('Amt-Sxq3',DerivedTable4.Addressm87D) OR
      ENDS_WITH(SourceB.FieldI,'Account-UJDv') OR
      ENDS_WITH(DerivedTable3.Code3Xip,'Address-D2ms') OR
      ENDS_WITH(DerivedTable1.CCYXSkS,DerivedTable7.CountrypfA6) OR
      DerivedTable3.FeeF6P4 NOT LIKE SourceC.FieldF AND
      'Code-szY4' = DerivedTable1.AccountqcVc OR
      STARTS_WITH('Fee-tTOg',DerivedTable3.AccountViga)
  ),
  Table38 AS (
    SELECT
      IF(
        'Country-ivA5' NOT LIKE 'Account-fZ1G',
        IF(
          ENDS_WITH('Address-akwe',DerivedTable4.Country8VtM), Table37.CCYPw5k,
          SourceD.FieldE),
        IF(
          STARTS_WITH(DerivedTable5.FeeogF5,Table36.CustomerpfFm),
          'Address-H1sk', DerivedTable3.CustomerULyd))
        AS AmtQO8w,
      IF(
        ENDS_WITH(DerivedTable2.Account61kv,Table37.CCYPw5k),
        IF('Amt-xo8Z' <> 'Account-fZC6','Customer-ccip','Fee-OonT'),
        CASE
          WHEN STARTS_WITH('Address-q8kn',DerivedTable1.CountryV1io) THEN
            DerivedTable3.CountrygMB4
          WHEN 'Amt-b0Tk' NOT LIKE DerivedTable1.CCYXSkS THEN
            'Address-BlGA'
          WHEN ENDS_WITH(Table37.CCYPw5k,'Address-PjUo') THEN
            'CCY-AoNv'
          WHEN STARTS_WITH(SourceE.FieldF,Table36.CountryrZ1u) THEN
            'Customer-lRjF'
          ELSE
            'Code-CsNl'
        END)
        AS AccountMhfS,
      IF(
        'Code-8IXI' = DerivedTable3.CodeVHWz,
        IF('Amt-AOCQ' <> SourceB.FieldG,'Address-7bjx','Fee-zq38'),
        IF(
          STARTS_WITH('Account-P6Oj',SourceA.FieldH), 'Amt-PY7P',
          'Country-PYau'))
        AS AccountMRTX,
      IF(
        'Amt-4k0N' = 'Customer-ahux',
        IF(
          ENDS_WITH('Code-8k8j','Customer-g4Z5'), 'Customer-Zw9B',
          'Account-lCXK'),
        CASE
          WHEN STARTS_WITH(SourceD.FieldI,SourceB.FieldC) THEN
            'Customer-5PND'
          WHEN STARTS_WITH(DerivedTable3.Codeaqvh,'CCY-lpoQ') THEN
            'Address-MQzP'
          WHEN 'CCY-9S4f' LIKE DerivedTable5.Code7INp THEN
            'CCY-GFPX'
          ELSE
            DerivedTable1.FeerBIS
        END)
        AS AddressTTU2,
      IF(
        Table36.FeelElc <> DerivedTable2.AmtNncS,
        IF(
          'Customer-MYRY' <> DerivedTable1.AccountKwSP, DerivedTable3.Code3Xip,
          Table36.CCYgOyc), IF(
                              ENDS_WITH(DerivedTable3.FeeF6P4,'Amt-XLjK'),
                              Table37.CustomerSKc6, Table36.CountryItp6))
        AS FeeuRWt,
      IF(
        STARTS_WITH('Address-hJtZ','Code-J13O'),
        CASE
          WHEN ENDS_WITH(SourceC.FieldB,'Code-Nb7D') THEN
            DerivedTable3.FeeT7G9
          WHEN 'Country-iCrX' = 'Amt-44tx' THEN
            'Fee-pao3'
          WHEN ENDS_WITH('Amt-buUn','Address-bMWd') THEN
            DerivedTable1.AccountmNQo
          WHEN 'Code-RV36' LIKE DerivedTable3.FeeF6P4 THEN
            DerivedTable3.CountrygMB4
          WHEN STARTS_WITH('Customer-n9Lr',Table37.Customer3id9) THEN
            'Customer-yFHo'
          ELSE
            'Customer-YOSl'
        END, CASE
               WHEN ENDS_WITH(SourceC.FieldJ,'Country-TGPB') THEN
                 Table37.CustomerSKc6
               WHEN ENDS_WITH(SourceD.FieldD,SourceC.FieldC) THEN
                 'Customer-OE7d'
               ELSE
                 'Account-i6S4'
             END)
        AS AddressK86j,
      IF(
        'Amt-oyAS' = 'CCY-Z0F1',
        IF(
          STARTS_WITH('Fee-xKcS','Account-b9j1'), SourceB.FieldG,
          Table37.Code3cb6), IF(
                               DerivedTable3.FeeD0Po <> Table37.FeevHoM,
                               'Country-Z1WQ', DerivedTable1.AmtzEYq))
        AS CountryE5K9,
      CASE
        WHEN STARTS_WITH('Fee-Llge','Code-Jgy5') THEN
          IF(
            'Code-8AYn' <> 'Country-JpgY', 'Country-RNh3',
            DerivedTable4.CodeXHEv)
        WHEN ENDS_WITH('Customer-RR8M','Country-lVyr') THEN
          IF(
            DerivedTable1.AddressoeLk = DerivedTable3.CustomerULyd, 'Amt-LIP4',
            DerivedTable1.AddressoeLk)
        WHEN 'Fee-UtOU' = DerivedTable2.CodeRziT THEN
          IF(Table37.CCY4GHR LIKE SourceB.FieldJ,SourceC.FieldB,'Address-R3SX')
        WHEN STARTS_WITH('Account-goap','Fee-6z5j') THEN
          IF(
            STARTS_WITH(SourceB.FieldH,SourceB.FieldF), SourceA.FieldJ,
            'Address-8apl')
        ELSE
          IF(
            'Address-FrDK' = DerivedTable7.AddressgmSt, 'Amt-GEMz',
            DerivedTable1.AccountmNQo)
      END
        AS AmtfWfp,
      IF(
        DerivedTable2.Amt885g = 'Amt-uiDH',
        IF(
          'Code-cPuB' = Table37.AccountgNpd, 'Fee-hl9Z',
          DerivedTable4.Addressm87D),
        IF(
          STARTS_WITH(SourceD.FieldD,'Country-ZlUF'), 'Customer-lVpm',
          SourceC.FieldD))
        AS AccountjFa3,
      IF(
        'CCY-MyM4' != 'Address-GAzh',
        CASE
          WHEN SourceB.FieldI <> 'Country-PYwE' THEN
            DerivedTable3.CountrygMB4
          WHEN ENDS_WITH(SourceC.FieldC,DerivedTable4.AddressWvnP) THEN
            'Country-IiOw'
          WHEN STARTS_WITH(DerivedTable4.Addressm87D,DerivedTable3.FeeCUde) THEN
            'Customer-9bFH'
          WHEN 'Fee-7KqA' = SourceE.FieldI THEN
            Table36.CCYgOyc
          WHEN 'Fee-eJ7f' LIKE DerivedTable3.FeeD0Po THEN
            Table37.Code3cb6
          ELSE
            DerivedTable1.CustomerKrwR
        END,
        CASE
          WHEN ENDS_WITH('CCY-WZMt',SourceA.FieldG) THEN
            SourceA.FieldJ
          WHEN ENDS_WITH('Customer-6jCK',DerivedTable1.CustomerWpfX) THEN
            DerivedTable3.Code3Xip
          WHEN ENDS_WITH(DerivedTable1.Codeuhr9,DerivedTable5.CodePGRC) THEN
            'Customer-f2Vc'
          WHEN SourceA.FieldA != 'Code-b4Mo' THEN
            'Fee-b5kG'
          WHEN 'Account-zTbF' = DerivedTable1.CustomerWrlz THEN
            'Customer-3i8L'
          ELSE
            DerivedTable2.AddressHdnu
        END)
        AS CCY2KE3,
      CASE
        WHEN 'Fee-MjGa' NOT LIKE 'Country-QS3J' THEN
          IF(DerivedTable7.CCYZD20 NOT LIKE 'Code-lYa4','Code-gi9O','CCY-DESZ')
        WHEN 'Country-O3XZ' NOT LIKE DerivedTable4.Addressm87D THEN
          IF(
            STARTS_WITH(DerivedTable3.CountryoFFA,DerivedTable2.CodeRziT),
            'Country-2rB2', 'Address-DJzH')
        ELSE
          IF(
            ENDS_WITH(DerivedTable1.FeerBIS,Table36.FeelElc), SourceD.FieldB,
            DerivedTable4.FeezLpC)
      END
        AS AccountgvvI,
      IF(
        STARTS_WITH('Fee-9Kin','Account-cbBN'),
        IF(
          'Code-QIlD' != Table36.CustomerpfFm, DerivedTable4.Account1SJF,
          DerivedTable2.AddresscM17),
        IF(
          ENDS_WITH(DerivedTable4.CCYY6No,'CCY-hqMb'), SourceE.FieldD,
          SourceB.FieldD))
        AS CountryPRG9,
      IF(
        'Amt-ZNgw' LIKE 'Fee-EFho',
        IF(
          STARTS_WITH('Country-fJxA',DerivedTable3.FeeD0Po), SourceC.FieldB,
          Table36.CCYgOyc),
        IF(
          STARTS_WITH(DerivedTable2.CodeRziT,Table37.CCYiCm5), SourceC.FieldA,
          DerivedTable7.FeeBS3r))
        AS AddressT8dt,
      IF(
        Table37.CustomerSKc6 LIKE SourceD.FieldA,
        IF(
          DerivedTable7.AccountKbFf LIKE DerivedTable1.AccountfEJK,
          DerivedTable4.FeesrBh, 'Country-tIoV'),
        CASE
          WHEN STARTS_WITH('Country-vb3f',DerivedTable2.AddressHdnu) THEN
            'Country-EBwB'
          WHEN DerivedTable3.FeeD0Po = 'Amt-Bxr4' THEN
            'Code-dKZj'
          ELSE
            DerivedTable4.FeesrBh
        END)
        AS CustomerIacp,
      IF(
        STARTS_WITH('CCY-ryZN','Fee-NFng'),
        IF(
          'Amt-kpQf' != 'Customer-eRMv', 'Address-f96P',
          DerivedTable7.AddressuSTK),
        CASE
          WHEN DerivedTable2.Address7oYI = SourceE.FieldB THEN
            DerivedTable3.Code3Xip
          WHEN DerivedTable2.Address5RWP != DerivedTable2.Address5RWP THEN
            'CCY-qsdb'
          WHEN ENDS_WITH('Amt-KtTp','Fee-Jxj5') THEN
            DerivedTable3.FeeF6P4
          WHEN DerivedTable4.Country8VtM != DerivedTable5.CountrynpDj THEN
            DerivedTable5.Code7INp
          WHEN Table37.Customer3id9 <> DerivedTable7.CustomertAao THEN
            SourceE.FieldB
          ELSE
            DerivedTable2.Address5RWP
        END)
        AS CCYONyI,
      IF(
        ENDS_WITH(DerivedTable4.FeexvTE,DerivedTable7.CCYZD20),
        IF(
          'CCY-8BTW' LIKE DerivedTable3.CCYO742, SourceB.FieldB,
          DerivedTable2.Account61kv),
        IF(SourceE.FieldA LIKE 'Amt-wzAm','Amt-qoVA','Fee-c2e0'))
        AS Fee99vx,
      IF(
        Table37.CCYc87y = SourceC.FieldI,
        IF(
          SourceC.FieldF = 'Customer-B6aX', DerivedTable2.AccounttVM1,
          DerivedTable5.Code7INp),
        IF(
          STARTS_WITH(DerivedTable2.AddressHdnu,SourceE.FieldF), SourceD.FieldB,
          SourceE.FieldD))
        AS CustomerV2rH,
      IF(
        ENDS_WITH('Fee-j9YU',DerivedTable1.AddressoeLk),
        CASE
          WHEN DerivedTable5.Feepmor LIKE 'CCY-ui3K' THEN
            DerivedTable7.FeeBS3r
          WHEN ENDS_WITH(DerivedTable7.CodeOp8k,'Code-0rOk') THEN
            'Account-OlYs'
          WHEN ENDS_WITH('Customer-7pZ9',Table37.Amte2OU) THEN
            'CCY-YXRu'
          ELSE
            'Amt-dvSS'
        END, IF(
               STARTS_WITH(Table36.CCYgOyc,'Country-HA87'),
               DerivedTable3.CountrygMB4, Table36.CountryItp6))
        AS Address5B52,
      IF(
        ENDS_WITH(DerivedTable1.CountryV1io,'Customer-bx8n'),
        CASE
          WHEN STARTS_WITH(DerivedTable1.AccountqcVc,'Fee-MFHl') THEN
            'Account-Vq13'
          WHEN SourceB.FieldC = 'Country-zjF4' THEN
            'Country-TF6H'
          WHEN DerivedTable1.Codeuhr9 NOT LIKE 'Country-Jalr' THEN
            DerivedTable5.CountrynpDj
          ELSE
            SourceC.FieldH
        END, IF(
               'Account-Y4LX' NOT LIKE SourceE.FieldA, 'Fee-S1q3',
               DerivedTable1.CustomerKrwR))
        AS CCYyDfB,
      IF(
        SourceC.FieldE NOT LIKE 'Country-oQWO',
        IF(
          STARTS_WITH('Customer-Sexy','Account-tjFd'), 'Code-Oc8f',
          Table37.CCYiCm5),
        IF(
          STARTS_WITH(DerivedTable1.AmtzEYq,'Customer-aBzT'), 'Country-Wstc',
          SourceA.FieldE))
        AS CodeqXTY
    FROM
      testdataset.SourceA AS SourceA
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (SourceB.FieldI LIKE 'Fee-E8l3' OR SourceB.FieldE != SourceB.FieldC AND
         ENDS_WITH('CCY-TApB',SourceB.FieldD) AND 'Fee-0hEk' <> 'Customer-munc')
      INNER JOIN testdataset.SourceC AS SourceC ON
        (SourceC.FieldI LIKE SourceB.FieldD AND
         STARTS_WITH(SourceB.FieldB,'Address-wALK') OR
         'CCY-vWKm' = SourceA.FieldC OR
         ENDS_WITH(SourceC.FieldE,SourceB.FieldB))
      INNER JOIN testdataset.SourceD AS SourceD ON
        ('Code-ee0P' <> 'Account-VkC1' OR SourceB.FieldA <> SourceD.FieldA AND
         STARTS_WITH(SourceB.FieldA,'Country-wK6c') AND
         STARTS_WITH(SourceD.FieldH,SourceC.FieldJ))
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (ENDS_WITH(SourceD.FieldH,'Country-jKk6') AND
         STARTS_WITH('Fee-mEk2',SourceC.FieldF) AND
         SourceA.FieldH != SourceE.FieldJ OR
         ENDS_WITH('Customer-7Cdo',SourceC.FieldD))
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('CCY-Rfvj','Fee-VjxB') OR 'Address-lMfd' <> 'CCY-hHNj' AND
         ENDS_WITH('Account-ZTlJ','Account-rjrb') AND
         'Code-anwg' = DerivedTable1.FeerBIS)
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Account-oX9q' NOT LIKE DerivedTable1.AccountKwSP AND
         'Amt-Gu3Z' = DerivedTable2.AccounttVM1 AND
         ENDS_WITH(DerivedTable1.AmtR86V,DerivedTable2.AccountrJZf) AND
         ENDS_WITH('Account-sOVk','Address-CPvT'))
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        ('Account-zhUa' != 'Country-ddkS' AND
         STARTS_WITH('Amt-eh6g',DerivedTable3.Codeaqvh) OR
         DerivedTable1.FeerBIS != SourceE.FieldG OR
         STARTS_WITH('Amt-nP8v',DerivedTable1.CCYXSkS))
      LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH('Fee-1cWA',SourceE.FieldB) AND
         ENDS_WITH('Country-WhM8',DerivedTable3.CustomerLP0M) OR
         'Customer-xhHd' <> SourceD.FieldA AND
         ENDS_WITH(DerivedTable4.CodeJM19,'Amt-KmAg'))
      INNER JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (STARTS_WITH('CCY-UNKW',SourceD.FieldF) AND
         ENDS_WITH('Amt-gI0Y','Code-aJK7') OR
         'Fee-Fbk4' <> SourceD.FieldB OR SourceA.FieldH NOT LIKE SourceE.FieldC)
      RIGHT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        (SourceD.FieldI <> SourceB.FieldB AND
         ENDS_WITH('Account-DwMn','Amt-1V9G') AND
         STARTS_WITH('Account-zxxt',DerivedTable3.CCYO742) AND
         'Amt-3xX8' = 'Address-IhLA')
      INNER JOIN Table36 AS Table36 ON
        ('Customer-wheU' != 'CCY-gIvp' OR
         STARTS_WITH('Code-xmB6','Account-XTJD') OR
         STARTS_WITH(DerivedTable1.CCYXSkS,'Account-umpg') OR
         ENDS_WITH('Address-563A','Code-kj7T'))
      LEFT JOIN Table37 AS Table37 ON
        ('Account-KLcc' NOT LIKE 'CCY-g36l' OR
         SourceC.FieldG <> 'Account-yHzk' OR
         'Address-UrFA' = DerivedTable2.AmtNncS OR
         STARTS_WITH('Account-ObbR',DerivedTable4.FeezLpC))
    WHERE
      ENDS_WITH(SourceD.FieldJ,'Amt-udYO') OR
      ENDS_WITH(DerivedTable1.CustomerKrwR,'Fee-hjXQ') OR
      STARTS_WITH('Fee-Soow','Address-c2Dv') AND
      DerivedTable4.AccountMw3l NOT LIKE Table37.Customer3tVN OR
      SourceD.FieldF NOT LIKE 'Account-r3kY' AND
      STARTS_WITH('Address-GupR',Table37.CCY4GHR) OR
      ENDS_WITH(Table36.CodeuSJZ,SourceB.FieldF) AND
      DerivedTable3.Code3Xip = 'Fee-ZXMU'
  ),
  Table39 AS (
    SELECT
      CASE
        WHEN Table37.Amtpi4k != SourceC.FieldF THEN
          CASE
            WHEN DerivedTable7.FeeLG30 != Table37.AccountgNpd THEN
              'Customer-izGy'
            WHEN ENDS_WITH('Country-BGeh',SourceA.FieldI) THEN
              'Amt-HSFh'
            WHEN DerivedTable3.CodeUeLq NOT LIKE Table37.Feesdmj THEN
              'Fee-xih7'
            WHEN 'Country-q3nt' NOT LIKE Table38.CCY2KE3 THEN
              'Fee-QLzk'
            WHEN STARTS_WITH('Fee-XDiQ','Code-O1zc') THEN
              DerivedTable5.CCYVD4h
            ELSE
              DerivedTable1.Codeuhr9
          END
        WHEN STARTS_WITH(SourceA.FieldC,DerivedTable5.CountrynpDj) THEN
          CASE
            WHEN STARTS_WITH('Address-PmzX',DerivedTable7.FeeLG30) THEN
              SourceD.FieldD
            WHEN DerivedTable2.AddresscM17 <> SourceD.FieldD THEN
              'Customer-c2zM'
            WHEN 'Country-UrSe' != 'Account-Up2H' THEN
              'Country-RNQz'
            ELSE
              'Fee-N40C'
          END
        WHEN STARTS_WITH(Table37.Countryu2fW,'Address-jJ06') THEN
          IF(
            STARTS_WITH('Code-fb7K',DerivedTable3.CCYO742), 'Fee-10og',
            DerivedTable3.FeeF6P4)
        ELSE
          IF('CCY-N63r' LIKE 'Code-pj7l','Country-qINF',DerivedTable5.CodePGRC)
      END
        AS AmtUpSf,
      CASE
        WHEN STARTS_WITH('Fee-tj3Y',DerivedTable7.CountryXTJ7) THEN
          IF(
            STARTS_WITH(DerivedTable7.CustomertAao,'Account-VGcY'),
            'Customer-OyOg', SourceD.FieldJ)
        WHEN ENDS_WITH('Country-MFRa','Country-uXv8') THEN
          CASE
            WHEN DerivedTable7.FeeBS3r != 'Address-V40Y' THEN
              DerivedTable2.Amt885g
            WHEN 'Account-ATZy' <> DerivedTable3.CodeVHWz THEN
              'Customer-iqpV'
            WHEN 'Fee-mdmN' = 'Amt-620Z' THEN
              'Code-hdM2'
            WHEN Table38.CCYONyI = SourceD.FieldI THEN
              'CCY-XT0D'
            ELSE
              'Address-xO0a'
          END
        WHEN 'Amt-EKf3' = 'CCY-6SUz' THEN
          IF(DerivedTable7.Amt3faU != 'Code-hxmS','Country-1TpL','Country-ERJU')
        WHEN DerivedTable7.AddressuSTK LIKE Table38.CodeqXTY THEN
          IF(
            ENDS_WITH('Country-Ev70','Account-p2z9'), DerivedTable2.AddresscM17,
            'Customer-KiRw')
        WHEN STARTS_WITH(DerivedTable3.FeeD0Po,Table37.CCYiCm5) THEN
          IF(
            DerivedTable3.CustomerLP0M = 'Amt-sqqq', 'Country-Vp7K',
            Table37.CCY4GHR)
        ELSE
          CASE
            WHEN 'Country-FKoo' NOT LIKE DerivedTable7.AddressgmSt THEN
              SourceA.FieldG
            WHEN ENDS_WITH(DerivedTable5.CodePGRC,'Customer-Twyi') THEN
              'CCY-HHCg'
            WHEN 'Address-BXnr' != DerivedTable7.Codeuiec THEN
              SourceD.FieldF
            WHEN ENDS_WITH(SourceC.FieldF,'Address-lo00') THEN
              'Code-RDb4'
            WHEN STARTS_WITH('Code-GmTU',DerivedTable7.Amt3faU) THEN
              'Fee-ljrG'
            ELSE
              SourceC.FieldI
          END
      END
        AS AddressdXQM,
      IF(
        Table37.CustomerdBAq != SourceA.FieldF,
        IF(
          STARTS_WITH('CCY-ovmh',DerivedTable3.CountryoFFA), 'CCY-kYbY',
          'Account-WZHL'), IF(
                             ENDS_WITH(SourceD.FieldE,SourceD.FieldH),
                             DerivedTable5.CodePGRC, DerivedTable1.Addressrgjd))
        AS Address4ZzK,
      IF(
        DerivedTable7.Codeuiec NOT LIKE 'Customer-8RcN',
        IF(SourceD.FieldG LIKE 'Customer-MA10','Address-iAp5','Fee-xZAK'),
        IF('Customer-kFaZ' = Table38.AccountgvvI,'Address-Ft2A','Country-pT1x'))
        AS Feexyk2,
      IF(
        'CCY-hTUw' <> Table37.Amte2OU,
        IF(
          ENDS_WITH(SourceC.FieldC,'Account-8sTZ'), 'Address-xcQF',
          'Address-OSII'), IF(
                             'Amt-xqzu' = SourceC.FieldC,
                             DerivedTable1.AccountfEJK, SourceC.FieldA))
        AS CodeUbXe,
      IF(
        DerivedTable3.CodeVHWz LIKE DerivedTable2.AccounttVM1,
        IF(
          ENDS_WITH(DerivedTable2.AmtmorU,DerivedTable2.CodeDmva), 'CCY-wBvJ',
          DerivedTable7.AccountKbFf),
        IF(Table38.AccountgvvI = 'Country-gV1e',SourceA.FieldE,SourceD.FieldC))
        AS Account4SGC,
      IF(
        STARTS_WITH('Customer-U0bA',SourceD.FieldE),
        IF(
          DerivedTable2.AddressHdnu LIKE DerivedTable3.FeeD0Po, 'Account-3ZIm',
          'Amt-N5dy'),
        CASE
          WHEN DerivedTable5.CountrynpDj NOT LIKE Table37.CustomerdBAq THEN
            'Account-nSo6'
          WHEN STARTS_WITH(DerivedTable3.FeeT7G9,'Amt-aEkc') THEN
            'Customer-Fcln'
          WHEN STARTS_WITH(DerivedTable2.Address5RWP,'Account-v7LC') THEN
            DerivedTable1.CustomerWpfX
          ELSE
            DerivedTable7.Address3l5L
        END)
        AS Account3mNL,
      IF(
        DerivedTable3.CodeUeLq NOT LIKE Table37.Customer3tVN,
        IF(
          ENDS_WITH('Country-nbPH','Fee-KeM1'), DerivedTable7.Amt3faU,
          DerivedTable7.CodeOp8k),
        IF(STARTS_WITH('Fee-gDQJ','Customer-eds4'),'Address-2Aep','Code-j7YQ'))
        AS CustomerTlC1,
      IF(
        STARTS_WITH(DerivedTable7.AccountKbFf,Table38.AmtfWfp),
        IF(
          'Amt-bPvv' LIKE 'Code-6arG', DerivedTable2.AmtmorU,
          DerivedTable3.CodeVHWz),
        CASE
          WHEN 'Fee-E2lK' != DerivedTable3.FeeqkjX THEN
            DerivedTable5.CCYVD4h
          WHEN STARTS_WITH('Fee-wPnV','Amt-iT3V') THEN
            'Fee-wvRu'
          WHEN STARTS_WITH('Account-VPnL','Country-vbDG') THEN
            'Fee-DLIr'
          ELSE
            'Fee-GGgQ'
        END)
        AS AmtbpZz,
      IF(
        SourceC.FieldG LIKE SourceA.FieldB,
        IF(
          DerivedTable2.AddresscM17 != Table37.CCY4GHR, DerivedTable3.FeepQGh,
          SourceD.FieldC), IF(
                             Table38.Fee99vx != DerivedTable2.Amtp4ee,
                             'Amt-i8aq', Table38.AccountgvvI))
        AS AccountTotP,
      IF(
        STARTS_WITH(Table37.CustomerSKc6,SourceA.FieldG),
        CASE
          WHEN 'Address-8s1d' NOT LIKE DerivedTable3.FeeqkjX THEN
            DerivedTable2.AddresscM17
          WHEN ENDS_WITH(DerivedTable2.Address7oYI,Table37.CCYPw5k) THEN
            Table37.FeevHoM
          ELSE
            'Code-Amjc'
        END,
        IF(SourceA.FieldJ <> 'CCY-JbV5','Address-8uU6',Table37.Customer3id9))
        AS CCYaOxR,
      IF(
        ENDS_WITH(Table37.Code3cb6,'Customer-yQQ3'),
        CASE
          WHEN ENDS_WITH('Code-LMkQ','Customer-3gT8') THEN
            'Code-aCYX'
          WHEN 'Country-Q5UT' != DerivedTable1.AccountfEJK THEN
            SourceC.FieldA
          WHEN 'Customer-97Dr' != Table38.CCYyDfB THEN
            'Country-sgtx'
          ELSE
            DerivedTable2.AmtNncS
        END, IF(
               'Amt-rWnI' NOT LIKE SourceA.FieldA, 'Customer-RoXG',
               DerivedTable3.CountrygPvi))
        AS CodeFDZZ,
      IF(
        STARTS_WITH(DerivedTable5.AddressV4AX,Table37.AccountgNpd),
        IF(
          DerivedTable5.CodePGRC != Table38.Fee99vx, DerivedTable2.AccountrJZf,
          'Fee-cty4'), IF(
                         ENDS_WITH(Table37.Customer3id9,DerivedTable3.Codeaqvh),
                         'Customer-l2Zx', 'Fee-32gC'))
        AS CCYuMfJ,
      IF(
        'Account-9lEX' <> 'Code-RC0K',
        IF(
          DerivedTable7.FeeBS3r NOT LIKE SourceD.FieldB, 'CCY-D7fs',
          'Country-OD2g'), IF(
                             STARTS_WITH('Country-WXyg',SourceC.FieldH),
                             'Address-OFO4', SourceD.FieldJ))
        AS AmtP1fc
    FROM
      testdataset.DerivedTable7 AS DerivedTable7
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        ('Customer-guHs' = 'CCY-VJxt' AND
         DerivedTable7.CountryrPg8 NOT LIKE DerivedTable7.CountryrPg8 AND
         STARTS_WITH('Code-groY',SourceA.FieldE) OR
         'CCY-oo1r' LIKE DerivedTable7.CountryrPg8)
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH(SourceA.FieldJ,'Account-smVS') AND
         STARTS_WITH(DerivedTable7.CustomertAao,'Address-hwBX') OR
         STARTS_WITH('Customer-kUm9','Account-3PCP') OR
         SourceA.FieldF NOT LIKE DerivedTable7.Amt3faU)
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Country-aWdZ',SourceC.FieldH) OR
         STARTS_WITH(SourceA.FieldB,SourceD.FieldA) AND
         SourceA.FieldA NOT LIKE DerivedTable7.FeeBS3r AND
         ENDS_WITH(DerivedTable7.Amt3faU,DerivedTable7.CCYZD20))
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (DerivedTable7.AddressuSTK NOT LIKE DerivedTable1.CustomerKrwR AND
         'Amt-TWIU' != 'Code-g05u' OR
         SourceC.FieldB = 'Amt-Hd4g' AND STARTS_WITH('CCY-pY5B',SourceC.FieldD))
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Code-EMFm' = 'Code-RFFE' OR
         ENDS_WITH(DerivedTable7.Codeuiec,DerivedTable1.AddressoeLk) OR
         SourceA.FieldC != SourceD.FieldA AND
         'Amt-hBFD' = DerivedTable1.AmtR86V)
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (DerivedTable3.Feeg8kl = 'Amt-rcc1' OR
         SourceC.FieldB LIKE DerivedTable2.AmtmorU OR
         DerivedTable2.CodeRziT NOT LIKE 'Amt-9ClD' OR
         STARTS_WITH(DerivedTable3.FeeqkjX,'Account-rQuS'))
      LEFT JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
        (ENDS_WITH(DerivedTable3.CustomerLP0M,SourceD.FieldD) OR
         'Fee-P2ih' LIKE 'Customer-FuNj' OR
         ENDS_WITH(SourceA.FieldJ,SourceD.FieldG) AND
         ENDS_WITH(DerivedTable5.Code7INp,'CCY-Gxrt'))
      RIGHT JOIN Table37 AS Table37 ON
        (ENDS_WITH('Code-UezQ','Account-kWpH') AND
         Table37.Customer3tVN LIKE 'Customer-aACz' OR
         STARTS_WITH(Table37.Code3cb6,Table37.Countryu2fW) OR
         Table37.AccountgNpd LIKE DerivedTable5.Customera0qh)
      INNER JOIN Table38 AS Table38 ON
        (STARTS_WITH('Fee-gESM','Amt-uSdk') OR
         Table37.Amtpi4k <> 'Account-u1t3' AND
         STARTS_WITH(DerivedTable3.CodeVHWz,Table37.Customer3id9) AND
         'Customer-jQwt' != DerivedTable3.CCYO742)
    WHERE
      'Country-bcIS' = DerivedTable3.FeeqkjX OR
      DerivedTable7.CustomertAao = 'Customer-ypNi' OR
      'Country-kxuB' NOT LIKE DerivedTable1.CustomerWpfX AND
      ENDS_WITH('Fee-Bk6H',DerivedTable1.AmtR86V) OR
      'Address-Utja' LIKE SourceA.FieldJ OR
      STARTS_WITH(DerivedTable1.CCYXSkS,'Customer-TmwC') AND
      ENDS_WITH('Account-CnIy','Account-W3pF') OR 'Amt-DIAi' <> 'Account-NoDD'
  ),
  Table40 AS (
    SELECT
      IF(
        STARTS_WITH('Amt-aBZO',DerivedTable6.AddressD7PZ),
        IF(
          'Customer-3o0c' = DerivedTable7.AddressuSTK, 'Fee-O3J9',
          DerivedTable3.CountrygMB4),
        IF(
          ENDS_WITH('Address-MRzT','Code-ThkP'), 'Fee-CAp5',
          DerivedTable7.Amt9aae))
        AS CodebTku,
      IF(
        STARTS_WITH(Table37.Customer7IDY,Table39.CodeFDZZ),
        IF(
          DerivedTable1.CustomerWpfX <> Table38.AccountMRTX, 'Fee-VLNh',
          'CCY-COEe'), IF(
                         DerivedTable7.CountryrPg8 = 'Customer-3Oal',
                         DerivedTable1.CCYXSkS, 'CCY-P9BQ'))
        AS CodeZTAf,
      IF(
        Table38.CCYONyI NOT LIKE Table37.Feesdmj,
        IF(
          STARTS_WITH(Table38.CountryE5K9,Table39.Feexyk2), 'Account-5kwc',
          DerivedTable3.CountrygPvi),
        CASE
          WHEN SourceC.FieldD != DerivedTable4.AddressB6KJ THEN
            'Account-iwvW'
          WHEN 'Account-G5w4' != 'Code-aRu8' THEN
            'Amt-TDpg'
          WHEN STARTS_WITH('Fee-RqEq',DerivedTable6.AddressgQNB) THEN
            'Code-UrkK'
          WHEN Table39.Feexyk2 <> 'Country-IIaP' THEN
            'Amt-moUw'
          WHEN 'Account-OiFS' = 'Customer-cF24' THEN
            DerivedTable3.Codeaqvh
          ELSE
            'Address-M2Cv'
        END)
        AS CountryExAQ,
      IF(
        ENDS_WITH('Address-fPZa',Table37.Countryu2fW),
        CASE
          WHEN
            ENDS_WITH(DerivedTable1.AccountKwSP,DerivedTable4.Country8VtM)
          THEN
            Table36.Code77fF
          WHEN
            ENDS_WITH(Table38.AddressT8dt,DerivedTable3.Codeaqvh)
          THEN
            'Code-es4b'
          WHEN
            STARTS_WITH(SourceC.FieldI,'Address-MV3U')
          THEN
            'Country-Ikhu'
          ELSE
            DerivedTable3.Account4sxv
        END, IF(
               ENDS_WITH('Country-n2ub',Table37.Amte2OU), 'Code-zqfu',
               'Account-Tzyu'))
        AS AddressggAj,
      IF(
        STARTS_WITH(DerivedTable6.Fee8TJM,Table38.AddressK86j),
        CASE
          WHEN DerivedTable3.CustomerLP0M = Table36.CountryrZ1u THEN
            'Account-55so'
          WHEN ENDS_WITH('Country-EhZ8',DerivedTable6.FeeEU6a) THEN
            'Code-6svq'
          WHEN 'Amt-vAtC' = DerivedTable6.AmtW2w8 THEN
            Table39.CodeFDZZ
          WHEN STARTS_WITH('Address-D3kv','Customer-x4lD') THEN
            DerivedTable3.FeeqkjX
          ELSE
            SourceC.FieldD
        END, IF(SourceD.FieldA <> 'Fee-OFrK',DerivedTable3.FeeF6P4,'Fee-5Rsk'))
        AS AddressBCo4,
      IF(
        'Fee-X9Zb' NOT LIKE 'Account-PWJu',
        IF(SourceC.FieldF NOT LIKE Table37.CCYPw5k,'CCY-ZSHl','Fee-bmyU'),
        IF(
          DerivedTable7.CustomertAao != SourceA.FieldB, Table39.CodeFDZZ,
          DerivedTable6.CountryD7gw))
        AS Feewa14,
      IF(
        SourceD.FieldF NOT LIKE 'Amt-NBWM',
        IF(
          STARTS_WITH('Fee-W61x','Account-gTrg'), 'Account-gxnh',
          Table37.Code4OwH), IF(
                               'Address-1xOA' LIKE DerivedTable6.AddressgQNB,
                               Table39.AccountTotP, DerivedTable6.Address54VO))
        AS FeeyqT2,
      IF(
        STARTS_WITH(DerivedTable1.AmtzEYq,'Code-DgOv'),
        IF(
          ENDS_WITH('Amt-N4po',DerivedTable4.AddressWvnP), Table37.Code4OwH,
          Table39.CodeUbXe),
        IF(STARTS_WITH('CCY-xIAY','Customer-uBVW'),'Amt-FpYB',SourceC.FieldD))
        AS Amtvgu4,
      IF(
        'Address-HeFJ' = 'Country-skNm',
        IF(
          STARTS_WITH('Amt-3znJ','Fee-0hEd'), Table36.CountryItp6,
          SourceA.FieldG), IF(
                             DerivedTable4.AddressWvnP NOT LIKE 'Country-lqI1',
                             DerivedTable3.FeeCUde, Table38.AddressK86j))
        AS Accounts7RW,
      IF(
        ENDS_WITH(Table36.CodezIk3,'Fee-1tnf'),
        IF(
          ENDS_WITH(Table38.CustomerV2rH,Table38.CountryPRG9),
          DerivedTable6.Address54VO, 'Fee-rWM5'),
        IF(
          STARTS_WITH('Code-LOsp',Table37.Customer7IDY), 'Amt-rx5d',
          'Account-g04K'))
        AS Countryix6U,
      IF(
        'Address-xJl6' <> Table38.AmtfWfp,
        IF(
          ENDS_WITH('CCY-CRqM','Customer-b57v'), SourceC.FieldB,
          'Customer-yPQc'),
        IF(ENDS_WITH('Amt-a55D','Code-w0BS'),'Country-ljYH','Customer-lVnS'))
        AS FeekDFE,
      IF(
        STARTS_WITH('CCY-drIB',DerivedTable7.AddressuSTK),
        CASE
          WHEN STARTS_WITH('Code-Kkxl',DerivedTable3.FeeCUde) THEN
            Table39.Account3mNL
          WHEN ENDS_WITH(SourceA.FieldJ,DerivedTable6.Address54VO) THEN
            'Amt-KvSl'
          WHEN ENDS_WITH('Code-CRwo','Code-yQNF') THEN
            'Fee-rzvA'
          WHEN STARTS_WITH(Table39.CCYaOxR,DerivedTable1.AmtR86V) THEN
            'Amt-B2j4'
          WHEN STARTS_WITH(DerivedTable4.FeexvTE,'Amt-P2vO') THEN
            DerivedTable3.Codeaqvh
          ELSE
            'Code-AY1R'
        END, IF(
               ENDS_WITH(Table39.CCYuMfJ,DerivedTable3.FeeCUde), SourceB.FieldA,
               Table36.CCYgOyc))
        AS AddressnUFN,
      IF(
        SourceA.FieldB != 'Customer-AkNm',
        IF(
          ENDS_WITH(SourceC.FieldI,DerivedTable4.FeezLpC),
          DerivedTable1.Codeuhr9, DerivedTable7.AddressuSTK),
        IF(
          STARTS_WITH(Table38.AccountgvvI,'Customer-TiZG'), 'Country-iHsE',
          'Account-275x'))
        AS Addressheot,
      IF(
        DerivedTable6.CodebnT9 NOT LIKE DerivedTable1.AccountKwSP,
        IF(
          STARTS_WITH('Fee-PRNQ',Table38.Address5B52), Table36.AddressyaZ0,
          'Code-6y5d'), IF(
                          ENDS_WITH(Table37.CustomerdBAq,'CCY-FD6G'),
                          'Country-Bky6', 'Address-5fBs'))
        AS CCYgn5o,
      IF(
        STARTS_WITH(DerivedTable6.Fee8TJM,'Country-OGcg'),
        IF(
          ENDS_WITH('Customer-QVii','Code-zATU'), 'Fee-ffKf',
          DerivedTable6.AddressPIbO),
        IF(
          Table38.AccountMhfS LIKE DerivedTable1.AccountKwSP, 'Fee-TJZ8',
          DerivedTable4.CodeJM19))
        AS AmtI7Yv,
      IF(
        STARTS_WITH(Table37.CustomerSKc6,'Address-gXuD'),
        IF(
          Table38.Fee99vx <> DerivedTable1.Addressrgjd,
          DerivedTable6.CustomerCZg3, DerivedTable4.CodeXHEv),
        IF(ENDS_WITH('Fee-NJzi',Table37.Code4OwH),'Fee-8btd','CCY-eOOS'))
        AS AmtH1hT,
      CASE
        WHEN 'Country-8D6M' <> Table37.Countryu2fW THEN
          IF(
            STARTS_WITH('Account-sqoZ','Amt-9hXJ'), SourceB.FieldE,
            DerivedTable6.CCYp8wn)
        WHEN STARTS_WITH('Address-VlfV','CCY-pNU6') THEN
          IF(
            'Address-Y5Lv' <> DerivedTable7.CountrypfA6, SourceA.FieldI,
            'Customer-WN26')
        WHEN STARTS_WITH(SourceA.FieldE,SourceC.FieldJ) THEN
          IF(
            'Fee-vW8q' <> Table38.AddressK86j, DerivedTable6.AddressWejH,
            SourceC.FieldC)
        ELSE
          IF(
            STARTS_WITH(Table39.CCYuMfJ,'Country-Ksts'), 'Code-8rr0',
            'Country-EWcc')
      END
        AS AccountNSdg,
      CASE
        WHEN Table38.Address5B52 = Table36.CCYgOyc THEN
          IF(
            ENDS_WITH('Address-3R5Z','Customer-cMOH'), SourceB.FieldH,
            'Fee-WdeN')
        WHEN ENDS_WITH('Code-PAZT','Code-DAb9') THEN
          IF(
            STARTS_WITH('Country-ayhM',DerivedTable4.AddressWvnP),
            'Account-q8kH', 'Amt-5Eey')
        WHEN Table38.CountryE5K9 != DerivedTable1.AddressoeLk THEN
          CASE
            WHEN 'Country-5k9T' LIKE SourceA.FieldE THEN
              Table39.Account3mNL
            WHEN Table37.Customer7IDY <> DerivedTable3.FeepQGh THEN
              'Code-GUQ7'
            WHEN STARTS_WITH('Fee-W4lS','Amt-KpCF') THEN
              'CCY-aXkh'
            WHEN 'Amt-Va2i' != SourceC.FieldD THEN
              'Address-Wbak'
            WHEN DerivedTable3.Feeg8kl LIKE 'Code-U1zM' THEN
              DerivedTable4.CustomerCDJF
            ELSE
              'Account-LeIB'
          END
        WHEN Table36.CodeuSJZ <> 'Code-4yAP' THEN
          IF('Amt-HNHa' <> Table38.CCY2KE3,'Fee-Gobl','Account-dXzY')
        ELSE
          IF(
            DerivedTable4.CodeJM19 NOT LIKE Table38.CCY2KE3,
            Table39.Account4SGC, 'Fee-Qf4o')
      END
        AS CodeBIaV,
      IF(
        STARTS_WITH('Customer-kZD6',Table38.CCYONyI),
        IF(
          STARTS_WITH('Amt-Xn8f',DerivedTable3.Codeaqvh),
          DerivedTable3.CodeVHWz, DerivedTable3.CustomerLP0M),
        IF(
          DerivedTable6.CodeY4Yb LIKE SourceA.FieldB, 'Customer-Zw90',
          'Amt-EA8u'))
        AS FeeofZv
    FROM
      testdataset.SourceD AS SourceD
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (SourceA.FieldD != 'Customer-XOxn' OR
         SourceA.FieldF <> SourceD.FieldD OR
         'Address-OeZ2' = SourceA.FieldC AND
         ENDS_WITH(SourceD.FieldJ,SourceA.FieldF))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (SourceD.FieldD <> 'Country-KKKw' AND
         STARTS_WITH(SourceA.FieldG,'Account-liDH') AND
         ENDS_WITH(SourceA.FieldI,'Fee-eAji') OR
         'Account-YKZc' LIKE SourceD.FieldA)
      LEFT JOIN testdataset.SourceC AS SourceC ON
        ('Address-Pvsv' != 'Country-dNN1' OR 'Country-HV4i' <> SourceA.FieldJ OR
         STARTS_WITH(SourceB.FieldH,SourceC.FieldG) AND
         STARTS_WITH(SourceD.FieldE,'Fee-88Cj'))
      INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (SourceB.FieldF NOT LIKE 'Customer-Ic7T' AND
         SourceC.FieldB <> 'Code-7t8H' OR
         STARTS_WITH('Account-2oJA',SourceB.FieldD) AND
         SourceA.FieldD <> 'Code-uuJW')
      INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        ('Country-SCje' <> 'Country-3EDX' OR
         STARTS_WITH(DerivedTable1.FeerBIS,DerivedTable3.CountryoFFA) AND
         DerivedTable1.CountryV1io = 'Code-4n5g' AND
         STARTS_WITH(SourceA.FieldF,SourceA.FieldD))
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (STARTS_WITH(SourceC.FieldD,'Code-ck0w') AND
         SourceA.FieldJ = DerivedTable1.AmtR86V AND
         STARTS_WITH('Account-8D0x',SourceA.FieldA) OR
         'Country-ePCl' != DerivedTable4.AccounteutQ)
      LEFT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
        (STARTS_WITH(DerivedTable3.FeeCUde,DerivedTable3.CodeVHWz) AND
         'CCY-FsfD' NOT LIKE DerivedTable1.AccountmNQo AND
         'Customer-GeYw' != 'Customer-p7LU' OR
         'Country-NR7J' = DerivedTable3.CCYO742)
      LEFT JOIN testdataset.DerivedTable7 AS DerivedTable7 ON
        ('Code-DQAT' LIKE 'Code-1UcS' OR SourceA.FieldC != 'Customer-eyXC' AND
         ENDS_WITH('Account-s5CI','CCY-bwQ3') OR
         STARTS_WITH(DerivedTable7.CountryXTJ7,'Country-RNRD'))
      RIGHT JOIN Table36 AS Table36 ON
        (STARTS_WITH(DerivedTable3.AccountViga,'Account-EyS7') AND
         DerivedTable7.Codeuiec LIKE 'Customer-uLl8' AND
         'Address-hmR8' = 'Fee-db4P' OR STARTS_WITH('Amt-fiXK',SourceC.FieldC))
      LEFT JOIN Table37 AS Table37 ON
        ('Country-1Uyv' = 'Fee-FGmD' AND SourceB.FieldD = 'Amt-61vs' AND
         STARTS_WITH('Address-JQNB','Amt-cs6A') OR 'Amt-mGt4' = SourceC.FieldF)
      LEFT JOIN Table38 AS Table38 ON
        (STARTS_WITH(DerivedTable6.Address54VO,DerivedTable3.CodeUeLq) AND
         STARTS_WITH('CCY-q80P',DerivedTable3.CountrygPvi) AND
         Table37.CCY4GHR LIKE 'Fee-wET5' OR
         STARTS_WITH('Fee-dzsd','Country-qYcG'))
      RIGHT JOIN Table39 AS Table39 ON
        (STARTS_WITH('CCY-wb3g','Country-AwUA') AND
         ENDS_WITH('Account-BLCg','Customer-CDVE') AND
         SourceD.FieldB = SourceB.FieldC OR 'Customer-KlGf' LIKE 'Amt-Dnoy')
    WHERE
      'Amt-AnHK' NOT LIKE 'Customer-cxEP' OR
      STARTS_WITH('Address-RtHL',DerivedTable6.Addressp4Y0) AND
      'Country-AMmP' = 'Customer-YM9D' OR
      ENDS_WITH(Table38.CCY2KE3,DerivedTable3.Account4sxv) AND
      DerivedTable1.CustomerWpfX LIKE Table37.CCYPw5k OR
      Table36.CountryItp6 NOT LIKE DerivedTable6.AddressYFBr OR
      DerivedTable4.CodeJM19 LIKE DerivedTable3.Account4sxv OR
      ENDS_WITH(Table36.FeeKF3x,DerivedTable1.FeerBIS)
  )
SELECT
  IF(
    ENDS_WITH('CCY-9ghy','CCY-DUjM'),
    IF(
      STARTS_WITH(SourceE.FieldI,'Amt-VRyB'), DerivedTable2.AmtmorU,
      'Account-5l3d'), IF(
                         SourceC.FieldI LIKE DerivedTable5.CountrynpDj,
                         DerivedTable1.CCYXSkS, DerivedTable3.FeeCUde))
    AS Customerrbtf,
  IF(
    Table37.Customer3id9 <> 'Country-Lh2i',
    CASE
      WHEN ENDS_WITH('Account-leGc',DerivedTable4.AddressB6KJ) THEN
        'Fee-DB3b'
      WHEN STARTS_WITH('CCY-vb7V',SourceE.FieldF) THEN
        SourceE.FieldG
      WHEN ENDS_WITH(Table38.AmtQO8w,'Address-i92J') THEN
        Table37.Customerv7eo
      ELSE
        DerivedTable4.Country8VtM
    END,
    IF(DerivedTable2.AddresscM17 = 'Address-DqhJ',Table40.Amtvgu4,'Fee-rebQ'))
    AS CustomerN13F,
  IF(
    'Code-okxt' = Table37.CustomerdBAq,
    IF(SourceD.FieldA = 'Customer-ojfw',DerivedTable6.AddressPIbO,'Amt-vQVr'),
    IF(STARTS_WITH(Table39.CCYaOxR,'CCY-ADou'),'Amt-ve7c',Table36.CodeuSJZ))
    AS FeePN5q,
  CASE
    WHEN ENDS_WITH(Table36.CodeuSJZ,'Customer-nXZk') THEN
      IF(Table39.Account3mNL NOT LIKE 'Fee-lNfS','Fee-3Je6','Country-Dwfm')
    WHEN 'Country-4Ade' LIKE 'Country-8N65' THEN
      CASE
        WHEN ENDS_WITH(Table36.CountryrZ1u,SourceD.FieldC) THEN
          'Fee-uSvK'
        WHEN Table38.AccountjFa3 <> 'Fee-sjI9' THEN
          SourceC.FieldH
        ELSE
          Table40.FeeofZv
      END
    ELSE
      IF(Table37.Customerv7eo != Table36.AddressyaZ0,'Fee-rizh',Table37.CCY4GHR)
  END
    AS Account3V3K,
  IF(
    STARTS_WITH(DerivedTable4.Account1SJF,SourceE.FieldI),
    IF('Account-unpq' <> Table40.CountryExAQ,DerivedTable3.FeeT7G9,'Code-0uvz'),
    CASE
      WHEN DerivedTable6.FeeFMCK != DerivedTable2.AmtNncS THEN
        'Country-k6B0'
      WHEN Table39.AmtbpZz != Table40.FeeyqT2 THEN
        DerivedTable2.Amtp4ee
      WHEN STARTS_WITH(DerivedTable5.Customera0qh,Table37.Code4OwH) THEN
        'CCY-BFgp'
      WHEN ENDS_WITH(Table37.Amte2OU,'Country-VtY0') THEN
        'CCY-z06X'
      ELSE
        'Customer-LNuW'
    END)
    AS CCYeu0i,
  IF(
    STARTS_WITH(DerivedTable2.AddresscM17,'Code-7FRV'),
    IF(Table40.Addressheot NOT LIKE 'Country-xIoD','Amt-hGAR',SourceD.FieldB),
    IF(
      ENDS_WITH(Table38.AccountMRTX,'CCY-cnKc'), 'Account-LEcy',
      DerivedTable3.Feeg8kl))
    AS CustomerCtTz,
  IF(
    ENDS_WITH(DerivedTable1.AccountfEJK,Table38.AccountgvvI),
    CASE
      WHEN ENDS_WITH(DerivedTable2.CodeDmva,'Amt-YsPb') THEN
        Table38.AddressK86j
      WHEN ENDS_WITH(SourceE.FieldB,'CCY-JUH1') THEN
        'CCY-qkpz'
      WHEN STARTS_WITH(DerivedTable3.CustomerLP0M,'Amt-v1UD') THEN
        'Address-NjA8'
      WHEN DerivedTable3.FeeD0Po = 'Amt-33M0' THEN
        Table40.CodeBIaV
      ELSE
        'Fee-BQvR'
    END,
    IF(Table38.CCYONyI <> DerivedTable6.Addressp4Y0,SourceE.FieldH,'Code-hBiM'))
    AS Country2xzz,
  CASE
    WHEN ENDS_WITH(DerivedTable4.Addressm87D,'Fee-E7uy') THEN
      IF(
        DerivedTable2.AddresscM17 NOT LIKE DerivedTable4.Account1SJF,
        'Account-v4FI', 'Fee-S12d')
    WHEN 'Account-wEmn' <> Table40.Accounts7RW THEN
      CASE
        WHEN ENDS_WITH(Table39.Account3mNL,DerivedTable3.CustomerLP0M) THEN
          SourceC.FieldJ
        WHEN ENDS_WITH('Amt-i4Zs','Account-X5Ix') THEN
          Table38.CodeqXTY
        WHEN Table36.CountryrZ1u LIKE 'Country-DCem' THEN
          SourceD.FieldH
        ELSE
          Table39.CCYuMfJ
      END
    ELSE
      IF(
        STARTS_WITH(DerivedTable4.AddressB6KJ,'Amt-JhzU'), Table38.FeeuRWt,
        'Fee-Wh4L')
  END
    AS Code6dqw,
  IF(
    DerivedTable3.FeeT7G9 NOT LIKE 'Amt-FVDh',
    IF(
      ENDS_WITH(Table39.Account4SGC,'Customer-rsRH'), 'Customer-xr03',
      'Customer-oPnv'), IF(
                          ENDS_WITH('Fee-vlSr',DerivedTable3.Codeaqvh),
                          DerivedTable4.AccounteutQ, 'Code-H6zo'))
    AS AmtSXKP,
  IF(
    ENDS_WITH('Country-TvuO','Customer-OWV7'),
    IF(
      SourceC.FieldC NOT LIKE 'Address-Soz4', DerivedTable4.Addressm87D,
      'Country-JNqK'),
    IF(
      ENDS_WITH(DerivedTable4.FeezLpC,DerivedTable1.AccountqcVc),
      'Customer-achB', DerivedTable5.AddressV4AX))
    AS CustomerZmFX,
  IF(
    STARTS_WITH(DerivedTable2.AmtNncS,'Customer-eCzQ'),
    IF(
      'Account-HRfC' <> 'Account-sHUk', 'Account-DHyc',
      DerivedTable1.AccountfEJK),
    IF(
      DerivedTable1.AccountfEJK LIKE DerivedTable3.CodeUeLq, SourceC.FieldA,
      'Code-WS71'))
    AS CountryKrJm,
  IF(
    STARTS_WITH(Table37.CCY4GHR,'Fee-AAIY'),
    IF(
      STARTS_WITH('Code-F1TM',DerivedTable6.AddressPpXC), 'Customer-w2Oz',
      'Customer-V8IH'),
    CASE
      WHEN Table36.FeelElc <> Table36.CountryrZ1u THEN
        DerivedTable4.FeexvTE
      WHEN ENDS_WITH(DerivedTable1.AccountmNQo,'Account-jF20') THEN
        Table36.CodeuSJZ
      WHEN 'Account-FHyb' <> DerivedTable6.AddressD7PZ THEN
        'CCY-65Y8'
      ELSE
        'Fee-T6Bx'
    END)
    AS Amti3Vx,
  IF(
    STARTS_WITH(DerivedTable4.CCYY6No,'Country-7Mcq'),
    IF(
      ENDS_WITH(DerivedTable2.AccountctCy,DerivedTable3.FeeD0Po), 'Code-Y8p8',
      DerivedTable2.AmtmorU),
    IF(SourceC.FieldI <> 'Amt-J80l',Table36.FeeKF3x,'Country-VuVk'))
    AS CustomerAce4,
  IF(
    ENDS_WITH('Account-Bcwi','Fee-watk'),
    IF(
      DerivedTable6.AmtCCH1 <> DerivedTable3.FeepQGh, Table40.CodebTku,
      'Account-mrin'), IF(
                         'Amt-Idnu' <> Table40.AddressnUFN, Table37.Amt891X,
                         DerivedTable6.AddressgQNB))
    AS CodeNPzY
FROM
  Table36 AS Table36
  INNER JOIN testdataset.SourceC AS SourceC ON
    (STARTS_WITH(SourceC.FieldE,'Code-yfXy') AND
     STARTS_WITH(Table36.CustomerpfFm,'Amt-ULNz') OR
     STARTS_WITH('Address-DWHS',SourceC.FieldA) OR
     'Account-gPnY' <> 'Code-3bpw')
  RIGHT JOIN testdataset.SourceD AS SourceD ON
    (SourceD.FieldI NOT LIKE SourceC.FieldF AND
     Table36.CustomerpfFm = 'CCY-rFym' OR
     SourceD.FieldI <> Table36.CustomerpfFm OR 'CCY-WQH5' != SourceC.FieldF)
  RIGHT JOIN testdataset.SourceE AS SourceE ON
    (STARTS_WITH('Account-fgUT','Customer-I1bu') OR
     ENDS_WITH(Table36.FeelElc,'Customer-SC95') OR
     Table36.CustomerpfFm NOT LIKE Table36.FeelElc AND
     ENDS_WITH(Table36.AmtCXms,'Customer-DBWc'))
  INNER JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
    ('Account-7h5S' != 'Account-2a4Y' OR ENDS_WITH('CCY-r94v','Amt-Jmji') AND
     STARTS_WITH(Table36.CustomerpfFm,'Code-ld82') OR
     STARTS_WITH('Customer-CLBB',SourceC.FieldH))
  INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
    (SourceD.FieldC <> 'Customer-dlOa' OR 'Account-i58d' <> SourceE.FieldH OR
     STARTS_WITH('Country-GgNU','Fee-JMW1') AND 'Amt-FesY' = 'Fee-wh6d')
  LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
    (STARTS_WITH('CCY-jCh1','Amt-kk2R') AND
     ENDS_WITH('Country-4scH',DerivedTable2.AccountrJZf) AND
     ENDS_WITH(SourceE.FieldJ,Table36.FeelElc) AND
     STARTS_WITH(DerivedTable2.Address7oYI,'Customer-mKeZ'))
  LEFT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
    ('CCY-effs' NOT LIKE 'Customer-LmdY' AND
     ENDS_WITH(DerivedTable2.Address5RWP,'CCY-NUHl') AND
     'Customer-RfJM' <> 'Code-6hHq' OR DerivedTable4.CCYY6No = 'Amt-kBMM')
  INNER JOIN testdataset.DerivedTable5 AS DerivedTable5 ON
    (STARTS_WITH('Amt-r0RY','CCY-9Sf3') AND
     STARTS_WITH('Fee-pwye','CCY-FbbW') AND
     STARTS_WITH('Address-x8Yz','Amt-qXgl') AND
     'CCY-MAlP' NOT LIKE DerivedTable5.CCYQK2Q)
  RIGHT JOIN testdataset.DerivedTable6 AS DerivedTable6 ON
    (STARTS_WITH('CCY-L99q','Amt-kebS') OR
     DerivedTable6.CountryD7gw LIKE 'Country-dMcf' OR
     STARTS_WITH(DerivedTable2.Account61kv,'Account-V4oh') AND
     STARTS_WITH(DerivedTable1.AccountmNQo,'Customer-Z6XN'))
  RIGHT JOIN Table37 AS Table37 ON
    ('Customer-f3GX' NOT LIKE SourceC.FieldC AND
     DerivedTable3.CodeVHWz LIKE 'Customer-FvP5' AND
     STARTS_WITH('Code-HasJ','Amt-Nnpq') AND
     DerivedTable6.AddressWejH NOT LIKE DerivedTable2.AccountctCy)
  INNER JOIN Table38 AS Table38 ON
    ('Customer-HqIu' LIKE 'Country-1UN9' OR SourceD.FieldG <> 'CCY-bUGk' OR
     SourceE.FieldA = DerivedTable1.AccountfEJK OR
     STARTS_WITH('Address-EoLO',Table36.CodezIk3))
  INNER JOIN Table39 AS Table39 ON
    ('Fee-H5oo' = DerivedTable2.AccounttVM1 AND
     Table39.Account4SGC LIKE 'Amt-XDst' OR
     DerivedTable5.CCYVD4h <> 'Country-BlCb' AND 'Customer-Bnja' = 'Amt-T60G')
  INNER JOIN Table40 AS Table40 ON
    (STARTS_WITH(Table40.CodeZTAf,Table39.Feexyk2) OR
     'Country-Sg5s' LIKE SourceD.FieldG AND
     ENDS_WITH(DerivedTable4.FeezLpC,DerivedTable3.CustomereBLI) OR
     'Fee-dGqx' <> SourceE.FieldJ)
WHERE
  STARTS_WITH('Amt-xXgW',DerivedTable2.AccountctCy) AND
  Table38.CCYyDfB <> 'Account-ckb5' OR
  STARTS_WITH(Table40.Accounts7RW,DerivedTable4.AccounteutQ) AND
  STARTS_WITH('Code-LzlY','Fee-1x5L') AND
  DerivedTable5.FeeogF5 <> SourceD.FieldA OR
  STARTS_WITH(Table37.CustomerdBAq,'Country-yYQP') OR
  STARTS_WITH(Table37.FeevHoM,Table37.Code4OwH) AND
  SourceC.FieldC NOT LIKE Table37.Countryu2fW