CREATE OR REPLACE TABLE testdataset.DerivedTable5 AS
WITH
  Table21 AS (
    SELECT
      IF(
        STARTS_WITH(SourceB.FieldA,DerivedTable4.CodeXHEv),
        IF(
          ENDS_WITH(SourceB.FieldI,DerivedTable2.Account61kv),
          DerivedTable2.Amt885g, SourceD.FieldG),
        IF(
          ENDS_WITH('Address-zQfz','Account-auCW'), DerivedTable4.AddressB6KJ,
          SourceA.FieldB))
        AS Code3qis,
      CASE
        WHEN STARTS_WITH(DerivedTable4.AddressWvnP,'Fee-ri0y') THEN
          IF(
            STARTS_WITH('Amt-7tHb',DerivedTable4.Country8VtM), 'Amt-owHy',
            SourceE.FieldA)
        WHEN SourceD.FieldG <> 'CCY-yfTy' THEN
          IF(ENDS_WITH('Code-kwpm',SourceD.FieldI),'CCY-Nb76',SourceA.FieldE)
        WHEN ENDS_WITH('Country-7mvf','CCY-267l') THEN
          IF(STARTS_WITH('Amt-Rml1','Amt-Vn5S'),'Country-Wl86','Account-zNIH')
        WHEN ENDS_WITH('Code-vt8G','Fee-tY3s') THEN
          IF(
            ENDS_WITH(DerivedTable1.CCYXSkS,'CCY-r1Ad'), SourceA.FieldB,
            SourceB.FieldA)
        ELSE
          IF('Account-zN5k' = SourceA.FieldJ,'Account-7Yqs',SourceC.FieldC)
      END
        AS Codetnm3,
      CASE
        WHEN STARTS_WITH(DerivedTable2.Amtp4ee,DerivedTable2.Address7oYI) THEN
          IF(
            STARTS_WITH(SourceB.FieldI,'Address-2Nn3'),
            DerivedTable2.Address5RWP, 'Amt-jhIh')
        WHEN STARTS_WITH('Country-cfv8',DerivedTable1.AccountfEJK) THEN
          IF(
            ENDS_WITH(SourceA.FieldG,DerivedTable2.CodeRziT), SourceE.FieldD,
            'Fee-9Jon')
        ELSE
          IF(
            ENDS_WITH('Address-8fev',DerivedTable1.CustomerWrlz),
            SourceC.FieldC, 'Fee-1eUX')
      END
        AS AddressEm08,
      IF(
        DerivedTable2.Address7oYI <> 'Fee-Rdb5',
        IF(SourceD.FieldI LIKE 'Country-m2jz',SourceD.FieldJ,'Code-VrPH'),
        IF(
          SourceE.FieldF NOT LIKE 'Fee-LXti', DerivedTable1.AccountqcVc,
          'Customer-3rr2'))
        AS FeeafPs,
      CASE
        WHEN 'Amt-wWiQ' <> DerivedTable4.CodeJM19 THEN
          IF(
            DerivedTable2.AmtmorU NOT LIKE 'Account-S1nP', 'Country-XDPr',
            DerivedTable4.AddressB6KJ)
        WHEN ENDS_WITH('Country-sbms','Account-7Bq8') THEN
          IF(
            ENDS_WITH(DerivedTable1.CCYXSkS,DerivedTable1.AccountKwSP),
            'Code-Bg9c', 'Country-caBm')
        ELSE
          IF(
            'Account-9IGq' != DerivedTable2.AddressHdnu, 'CCY-q5W6',
            'Address-ZGCS')
      END
        AS CCYANyf,
      IF(
        ENDS_WITH(SourceA.FieldA,'Customer-KtUf'),
        CASE
          WHEN 'Fee-mpwu' NOT LIKE SourceA.FieldC THEN
            'Address-mwqe'
          WHEN DerivedTable1.CustomerWpfX NOT LIKE 'Address-HB5m' THEN
            'Country-vWri'
          WHEN ENDS_WITH(DerivedTable4.AccounteutQ,'CCY-HA9k') THEN
            'Customer-KuEP'
          ELSE
            'Customer-vO7L'
        END, IF(
               STARTS_WITH(SourceE.FieldI,'Amt-tRcS'),
               DerivedTable4.AccountMw3l, 'Fee-dysp'))
        AS FeeMDz0,
      IF(
        'Address-v4cS' <> 'Customer-HgC5',
        CASE
          WHEN ENDS_WITH(DerivedTable2.AccountctCy,SourceC.FieldE) THEN
            'Country-rBhh'
          WHEN DerivedTable4.CodeJM19 NOT LIKE 'CCY-3G1e' THEN
            'CCY-SnFM'
          WHEN 'Code-jDCs' = SourceC.FieldF THEN
            SourceE.FieldA
          WHEN 'Customer-eGkK' = SourceD.FieldG THEN
            SourceD.FieldE
          ELSE
            DerivedTable2.CodeRziT
        END,
        IF('CCY-YeeS' NOT LIKE 'Amt-ruRf','Code-IrXB',DerivedTable4.CodelvtV))
        AS CCY8AOe,
      IF(
        'Account-Usbr' <> 'Account-6Toy',
        CASE
          WHEN 'Customer-K8Xk' LIKE SourceD.FieldD THEN
            SourceC.FieldH
          WHEN STARTS_WITH(SourceA.FieldA,SourceA.FieldE) THEN
            DerivedTable4.FeezLpC
          WHEN ENDS_WITH(DerivedTable4.AccounteutQ,SourceD.FieldF) THEN
            'Account-H0ic'
          WHEN ENDS_WITH(SourceD.FieldA,SourceA.FieldE) THEN
            DerivedTable1.AccountmNQo
          WHEN STARTS_WITH('Amt-NHFf',SourceD.FieldG) THEN
            SourceD.FieldA
          ELSE
            SourceB.FieldB
        END, IF(
               STARTS_WITH('CCY-hFfp',SourceD.FieldH), 'Address-7RXe',
               'Country-XVV5'))
        AS FeewjsN,
      IF(
        'Account-YQRg' != SourceB.FieldD,
        IF(
          SourceD.FieldG LIKE 'Customer-jCeV', 'Account-QGFR',
          DerivedTable1.Codeuhr9),
        IF(
          ENDS_WITH(SourceD.FieldF,'Customer-V9gc'), 'Account-4kWx',
          DerivedTable2.Address7oYI))
        AS Amt7FNh,
      IF(
        STARTS_WITH('Amt-KbJx',SourceD.FieldD),
        CASE
          WHEN DerivedTable2.AccounttVM1 != DerivedTable1.AccountfEJK THEN
            DerivedTable1.AccountqcVc
          WHEN ENDS_WITH('Customer-hfFP',SourceB.FieldA) THEN
            'Country-sCet'
          WHEN SourceB.FieldG LIKE 'Fee-HNGY' THEN
            SourceE.FieldC
          WHEN STARTS_WITH(DerivedTable4.FeesrBh,'Account-4tXJ') THEN
            SourceD.FieldG
          ELSE
            'Fee-7niT'
        END, IF(
               STARTS_WITH('Country-NcDf','Customer-fHfW'),
               DerivedTable1.AddressoeLk, 'Account-biHW'))
        AS FeegATY,
      IF(
        STARTS_WITH(SourceD.FieldF,'Address-knfU'),
        IF('Amt-gBZa' LIKE 'Amt-oJLT','CCY-f88H',DerivedTable2.Account61kv),
        IF(
          STARTS_WITH(DerivedTable4.CodeJM19,'Country-KfFG'), 'Account-Eqn1',
          SourceA.FieldI))
        AS CountrysAfr,
      CASE
        WHEN 'Account-Au9O' <> SourceC.FieldJ THEN
          IF(
            'Country-cfbL' = DerivedTable4.CodeXHEv, DerivedTable4.FeezLpC,
            SourceA.FieldG)
        WHEN ENDS_WITH('Fee-UViq','Account-9IFH') THEN
          CASE
            WHEN 'CCY-lfNF' LIKE DerivedTable4.Addressm87D THEN
              SourceB.FieldG
            WHEN DerivedTable2.AddresscM17 = 'Fee-Bjbg' THEN
              DerivedTable4.Country5f1u
            ELSE
              'Code-a1Cm'
          END
        ELSE
          IF(
            DerivedTable2.Amt885g = SourceE.FieldE, 'Customer-wczO',
            SourceC.FieldC)
      END
        AS CCYOxTW,
      IF(
        SourceA.FieldC = 'Code-gNgy',
        IF('Amt-jbe2' LIKE 'Customer-UDjE','CCY-bnCW','Address-fihk'),
        CASE
          WHEN 'Amt-oscE' NOT LIKE 'Country-DuKB' THEN
            'Account-nuBl'
          WHEN ENDS_WITH('Country-3EEc',DerivedTable4.CodelvtV) THEN
            'CCY-yjdD'
          WHEN SourceD.FieldG = DerivedTable4.Country5f1u THEN
            DerivedTable2.Address7oYI
          WHEN DerivedTable2.Address7oYI LIKE DerivedTable1.AddressoeLk THEN
            'Fee-O94m'
          ELSE
            DerivedTable2.CodeRziT
        END)
        AS AccountYh1Z,
      IF(
        ENDS_WITH('Fee-tOeO',SourceB.FieldA),
        CASE
          WHEN STARTS_WITH(DerivedTable1.AmtR86V,SourceD.FieldB) THEN
            SourceE.FieldF
          WHEN STARTS_WITH(SourceE.FieldI,SourceB.FieldD) THEN
            'Account-gsWb'
          WHEN ENDS_WITH(DerivedTable1.AccountqcVc,SourceA.FieldF) THEN
            'Code-QnGV'
          WHEN STARTS_WITH('Amt-uKmu',DerivedTable1.AccountKwSP) THEN
            SourceE.FieldE
          WHEN DerivedTable1.Addressrgjd NOT LIKE SourceE.FieldG THEN
            SourceB.FieldJ
          ELSE
            DerivedTable2.CodeDmva
        END, IF(
               'Fee-SCZg' NOT LIKE SourceB.FieldF, DerivedTable2.AmtmorU,
               DerivedTable2.Amtp4ee))
        AS AddressTZ4C,
      IF(
        STARTS_WITH(DerivedTable2.Address7oYI,SourceB.FieldB),
        IF(
          STARTS_WITH('CCY-xEsD',DerivedTable4.AddressB6KJ), 'Fee-Kszl',
          'Customer-bPaJ'),
        CASE
          WHEN DerivedTable1.AddressoeLk LIKE SourceD.FieldI THEN
            SourceE.FieldF
          WHEN 'Account-CkW2' LIKE SourceB.FieldI THEN
            'Customer-SdYP'
          WHEN DerivedTable2.CodeRziT <> 'Code-2R1D' THEN
            'Account-yKL8'
          ELSE
            SourceA.FieldB
        END)
        AS CountryMugA,
      IF(
        DerivedTable2.Account61kv NOT LIKE 'Amt-PjIH',
        CASE
          WHEN SourceC.FieldB <> 'CCY-ZJ00' THEN
            'Address-TXsI'
          WHEN STARTS_WITH('Code-hePp',SourceD.FieldI) THEN
            DerivedTable4.FeezLpC
          WHEN 'Address-WOlO' = DerivedTable2.Address5RWP THEN
            DerivedTable2.CountrywaWi
          ELSE
            SourceE.FieldB
        END, IF(
               ENDS_WITH('CCY-MWJz',DerivedTable1.CustomerWpfX), 'Country-XTQ2',
               SourceA.FieldA))
        AS AddressdSes,
      IF(
        ENDS_WITH('Amt-I8p8',DerivedTable2.AccountrJZf),
        IF(
          DerivedTable2.Amt885g != 'Fee-sKgx', 'Code-K7Ib',
          DerivedTable2.Amt885g),
        CASE
          WHEN ENDS_WITH(DerivedTable1.AccountqcVc,SourceC.FieldG) THEN
            'Address-8imn'
          WHEN 'Amt-XSIP' = DerivedTable1.AddressoeLk THEN
            'Code-8DUe'
          WHEN ENDS_WITH(SourceE.FieldB,SourceB.FieldH) THEN
            SourceE.FieldA
          ELSE
            'Amt-jFCZ'
        END)
        AS CodeDuQT,
      IF(
        'Amt-xGeL' <> 'CCY-u2pd',
        IF(
          ENDS_WITH(DerivedTable2.CountrywaWi,DerivedTable1.CustomerWrlz),
          'Code-fG2B', SourceA.FieldG),
        IF(
          'Fee-Gs2g' LIKE SourceE.FieldI, 'Account-MGC2',
          DerivedTable2.AccountctCy))
        AS Countryl6wQ,
      CASE
        WHEN STARTS_WITH(SourceE.FieldE,'Code-roRk') THEN
          IF(ENDS_WITH('Address-6ADc','CCY-84Jt'),'CCY-il4j','Account-oH3N')
        WHEN STARTS_WITH('Customer-tMiH',SourceA.FieldC) THEN
          IF(
            ENDS_WITH('Code-rhqw',DerivedTable4.AddressB6KJ),
            DerivedTable1.Addressrgjd, DerivedTable4.Country8VtM)
        WHEN STARTS_WITH(DerivedTable1.AccountfEJK,'Address-FgiR') THEN
          CASE
            WHEN 'Address-rs1j' <> DerivedTable1.AccountmNQo THEN
              'Code-28mE'
            WHEN 'Customer-GEna' = SourceE.FieldF THEN
              DerivedTable2.AccountctCy
            WHEN STARTS_WITH('Fee-jVCj','Code-Orzw') THEN
              'CCY-3rY9'
            ELSE
              'Fee-8y5I'
          END
        ELSE
          IF('Code-PvuP' NOT LIKE SourceD.FieldC,'Customer-eJ77','Address-tGPR')
      END
        AS AmtUKmU,
      IF(
        ENDS_WITH('Country-76un','Fee-GOez'),
        IF(DerivedTable1.AmtzEYq <> 'Address-z0QX','CCY-Wk7Z',SourceC.FieldG),
        IF(
          'Account-uzlb' = SourceD.FieldC, SourceE.FieldJ,
          DerivedTable2.CountrywaWi))
        AS AmtPgyZ
    FROM
      testdataset.SourceB AS SourceB
      INNER JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH('Account-V9CD',SourceB.FieldD) OR
         'Customer-FLwg' NOT LIKE 'Address-DNee' AND
         SourceA.FieldA <> SourceB.FieldE OR
         STARTS_WITH('Address-DAav',SourceA.FieldB))
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH('Amt-AjD4','Code-ZEM0') OR
         STARTS_WITH(SourceA.FieldD,SourceC.FieldJ) AND
         'Fee-E0lA' = SourceC.FieldC OR ENDS_WITH('Amt-TK0s',SourceA.FieldA))
      LEFT JOIN testdataset.SourceD AS SourceD ON
        (STARTS_WITH('Account-D5TQ','Country-oJvH') AND
         ENDS_WITH(SourceA.FieldC,SourceA.FieldE) OR
         ENDS_WITH('Address-XtQC',SourceB.FieldA) AND
         STARTS_WITH('Code-ykER','Fee-tQ9O'))
      LEFT JOIN testdataset.SourceE AS SourceE ON
        (SourceA.FieldG NOT LIKE 'Account-O2I0' AND
         STARTS_WITH(SourceB.FieldG,'Amt-3Wqr') OR
         ENDS_WITH('Customer-qgWV','Amt-mx31') OR
         SourceC.FieldB <> 'Address-5mjK')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (ENDS_WITH('CCY-1DWz','Country-UaGS') OR
         STARTS_WITH(SourceB.FieldB,'Address-1UrN') OR
         'Fee-Zwmw' <> SourceC.FieldB AND 'Address-DZqD' <> SourceC.FieldD)
      INNER JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Code-m65N' != 'Address-x970' OR SourceA.FieldH <> 'Customer-XdKT' AND
         SourceD.FieldC != 'Address-njYd' AND 'Country-3aek' = SourceB.FieldH)
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (ENDS_WITH('Fee-xsWg',DerivedTable4.Account1SJF) AND
         'Amt-Xh6a' <> SourceB.FieldI OR
         STARTS_WITH(DerivedTable2.AccounttVM1,'CCY-VjFh') AND
         ENDS_WITH(SourceB.FieldD,SourceB.FieldC))
    WHERE
      ENDS_WITH(DerivedTable4.Country8VtM,'Fee-GMB6') OR
      'Fee-wy0P' NOT LIKE 'Country-MO0S' OR
      'Address-yZcC' LIKE 'Customer-3ii9' AND 'Amt-MdNJ' = 'Address-eHZV' AND
      'Amt-3yiR' <> 'Amt-n2py' OR
      ENDS_WITH(DerivedTable1.CCYXSkS,'Code-CNMA') OR
      STARTS_WITH('Amt-oOPo','CCY-EWxe') AND
      STARTS_WITH(SourceC.FieldF,SourceA.FieldD)
  ),
  Table22 AS (
    SELECT
      IF(
        'CCY-OJpF' = DerivedTable3.FeeT7G9,
        IF(
          ENDS_WITH('Customer-P1nb','Code-OdkM'), DerivedTable2.AmtNncS,
          'Fee-CAT8'),
        CASE
          WHEN STARTS_WITH('Country-OhtW','Amt-fL3J') THEN
            DerivedTable1.Addressrgjd
          WHEN ENDS_WITH(SourceE.FieldE,SourceD.FieldB) THEN
            DerivedTable2.AmtmorU
          WHEN 'Amt-nBSl' = DerivedTable1.CustomerWpfX THEN
            'Fee-HcO1'
          WHEN 'Code-3R12' = DerivedTable2.AmtmorU THEN
            'Amt-AfXF'
          WHEN 'CCY-CYOy' LIKE 'Account-u1Yg' THEN
            DerivedTable2.AccounttVM1
          ELSE
            DerivedTable1.CustomerKrwR
        END)
        AS AddressRgfZ,
      IF(
        DerivedTable1.AmtzEYq != 'CCY-ukjz',
        IF(
          STARTS_WITH(DerivedTable3.CountrygPvi,'Code-kpjV'), 'Code-0wWO',
          'Account-254x'),
        IF('Address-rLCw' <> SourceE.FieldH,'Customer-TSES','Country-XsCq'))
        AS AmtaxC0,
      IF(
        SourceD.FieldF != 'Code-YYCZ',
        IF(
          DerivedTable1.AccountmNQo NOT LIKE DerivedTable2.AmtmorU,
          DerivedTable2.AmtNncS, 'Amt-UVuD'),
        IF(
          ENDS_WITH('Code-72pC','Country-C7D0'), DerivedTable3.Code3Xip,
          SourceE.FieldI))
        AS CountryQJTJ,
      IF(
        'Address-cXpH' NOT LIKE DerivedTable3.Code3Xip,
        IF(
          'Address-soXs' LIKE SourceD.FieldE, DerivedTable1.CCYXSkS,
          DerivedTable1.Codeuhr9),
        IF('Amt-XBG5' NOT LIKE 'Amt-4QHa',SourceD.FieldH,SourceD.FieldE))
        AS AddressOyCN,
      IF(
        ENDS_WITH('Account-b3QS','Customer-cIsI'),
        IF(
          ENDS_WITH(DerivedTable3.FeeD0Po,DerivedTable2.AmtNncS),
          'Country-HLcJ', 'Account-TU2K'),
        IF(
          DerivedTable3.CustomerLP0M <> 'Address-mMJk', 'CCY-tpxy',
          DerivedTable1.AccountmNQo))
        AS CountryKVrI,
      IF(
        ENDS_WITH('Amt-XvBW',SourceB.FieldB),
        IF(
          ENDS_WITH(DerivedTable2.Address7oYI,SourceB.FieldE),
          DerivedTable1.CustomerWrlz, DerivedTable2.AccountctCy),
        IF(
          STARTS_WITH('Address-B0RY','Code-hol5'), DerivedTable3.FeeF6P4,
          DerivedTable1.CCYXSkS))
        AS AccountaDlm,
      IF(
        STARTS_WITH(SourceB.FieldJ,'Country-idLc'),
        IF(
          DerivedTable2.Account61kv != SourceD.FieldC, 'Address-PJAS',
          'Amt-JTcC'),
        IF(STARTS_WITH('Amt-2Q3D','CCY-jfEr'),'Country-nk3u','Amt-mSoN'))
        AS Codehp2O,
      IF(
        SourceB.FieldI NOT LIKE 'Fee-8KlJ',
        IF(
          STARTS_WITH(DerivedTable1.Codeuhr9,'Fee-CwR6'), SourceB.FieldE,
          'Fee-moXs'), IF(
                         'Customer-i9Al' LIKE SourceB.FieldI,
                         DerivedTable2.AccountrJZf, SourceB.FieldD))
        AS FeeGWbe,
      IF(
        ENDS_WITH('Address-z9Oo',SourceD.FieldD),
        CASE
          WHEN 'Country-Bjuq' = 'Fee-Zqx9' THEN
            'Amt-uQWe'
          WHEN STARTS_WITH(SourceB.FieldG,DerivedTable1.CustomerKrwR) THEN
            SourceB.FieldD
          ELSE
            'Fee-nFiW'
        END,
        CASE
          WHEN ENDS_WITH(DerivedTable3.FeeF6P4,DerivedTable3.CodeVHWz) THEN
            DerivedTable3.FeeqkjX
          WHEN DerivedTable3.Codeaqvh <> 'Fee-rW3v' THEN
            DerivedTable2.AccounttVM1
          WHEN SourceD.FieldH <> 'Country-fvH3' THEN
            'Customer-s7bO'
          WHEN DerivedTable3.FeeCUde LIKE 'Fee-54UZ' THEN
            DerivedTable3.CustomerULyd
          WHEN DerivedTable2.Address5RWP NOT LIKE SourceD.FieldC THEN
            'Customer-yHx0'
          ELSE
            DerivedTable1.Addressrgjd
        END)
        AS CountryCpDv,
      CASE
        WHEN 'CCY-XyJX' <> 'Customer-z5k3' THEN
          IF(
            STARTS_WITH('Code-J53R','Code-r5dL'), DerivedTable2.Amtp4ee,
            SourceE.FieldC)
        WHEN SourceE.FieldF = DerivedTable3.Account4sxv THEN
          IF(
            DerivedTable2.CountrywaWi != SourceE.FieldD,
            DerivedTable2.Address7oYI, 'Fee-eLfL')
        WHEN ENDS_WITH(DerivedTable1.AccountqcVc,'Address-kVsu') THEN
          IF(
            ENDS_WITH(SourceE.FieldH,'Customer-4UqA'), SourceE.FieldC,
            'Code-09gZ')
        WHEN DerivedTable3.Codeaqvh LIKE DerivedTable2.Amt885g THEN
          IF(
            DerivedTable3.AccountViga <> SourceD.FieldB, 'Fee-yxID',
            DerivedTable1.CountryV1io)
        ELSE
          IF('Account-ZvNZ' <> 'Fee-lPSI',SourceD.FieldJ,'Customer-G7sH')
      END
        AS Customer6SmR,
      IF(
        STARTS_WITH('Amt-Yc0a','Code-YUYC'),
        IF(
          ENDS_WITH(SourceD.FieldE,DerivedTable1.CustomerWrlz),
          DerivedTable3.Code3Xip, 'Code-LwlQ'),
        IF(
          ENDS_WITH('Amt-mCvo','Address-YRmg'), 'Fee-Ytol',
          DerivedTable2.CountrywaWi))
        AS CCYSfd1
    FROM
      testdataset.SourceD AS SourceD
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(SourceB.FieldH,SourceB.FieldJ) AND
         'Account-eKsr' != 'CCY-VYmV' AND
         SourceD.FieldI NOT LIKE SourceB.FieldF AND
         'Amt-OTyM' LIKE 'Account-leDR')
      INNER JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH(SourceE.FieldJ,SourceD.FieldB) AND
         'Code-58bq' NOT LIKE 'Account-n3nb' AND
         ENDS_WITH(SourceB.FieldH,SourceB.FieldI) AND
         SourceB.FieldI <> 'Country-s67h')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (DerivedTable1.AmtR86V LIKE SourceB.FieldA AND
         ENDS_WITH('Customer-RyFA',SourceD.FieldB) AND
         ENDS_WITH(SourceD.FieldG,'Fee-FzSY') OR
         STARTS_WITH(DerivedTable1.CustomerWpfX,'Customer-aK96'))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (SourceB.FieldJ NOT LIKE SourceB.FieldC AND
         ENDS_WITH(SourceE.FieldF,DerivedTable1.CCYXSkS) AND
         ENDS_WITH(SourceD.FieldC,'Customer-zTrO') OR
         STARTS_WITH(SourceB.FieldJ,'Fee-DNdV'))
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (SourceD.FieldD <> 'Address-IMDH' AND
         STARTS_WITH('CCY-3OEi','Address-r3RS') AND
         STARTS_WITH(DerivedTable3.CodeUeLq,DerivedTable3.CodeVHWz) OR
         DerivedTable1.AmtR86V NOT LIKE 'Fee-5iHR')
    WHERE
      'Amt-yay3' = DerivedTable3.FeeCUde AND
      STARTS_WITH(SourceE.FieldG,'Fee-wOtq') AND
      STARTS_WITH('Country-d5rN',DerivedTable3.FeeCUde) OR
      DerivedTable3.CustomereBLI != 'Customer-nELP' OR
      STARTS_WITH(SourceE.FieldJ,'Code-h0yL') OR
      ENDS_WITH(DerivedTable3.FeeT7G9,DerivedTable2.CodeRziT) AND
      STARTS_WITH('Address-QZDW',DerivedTable1.AmtR86V) AND
      ENDS_WITH(SourceB.FieldA,DerivedTable3.Codeaqvh)
  ),
  Table23 AS (
    SELECT
      CASE
        WHEN 'Account-wEUa' <> DerivedTable4.CodeJM19 THEN
          IF(
            ENDS_WITH('Country-3BVp',DerivedTable1.AccountqcVc),
            Table21.CCYOxTW, 'Address-frlH')
        WHEN 'Account-b3Rj' != SourceE.FieldJ THEN
          IF(
            ENDS_WITH(Table22.AddressOyCN,'Customer-hPA7'),
            DerivedTable2.AccounttVM1, 'Account-IcH4')
        WHEN STARTS_WITH('Account-NrpS','Amt-iegn') THEN
          IF(
            Table21.CountryMugA NOT LIKE 'Customer-H6Ys', 'Customer-2WA4',
            Table22.AccountaDlm)
        ELSE
          CASE
            WHEN
              'Account-KoYA' NOT LIKE DerivedTable4.FeezLpC
            THEN
              DerivedTable4.FeezLpC
            WHEN
              ENDS_WITH(DerivedTable4.AccountMw3l,DerivedTable2.Address5RWP)
            THEN
              'CCY-l81l'
            WHEN
              'Customer-6FuG' NOT LIKE Table22.Customer6SmR
            THEN
              SourceA.FieldC
            ELSE
              DerivedTable4.CodeXHEv
          END
      END
        AS AccountlSjE,
      IF(
        ENDS_WITH(SourceA.FieldJ,'CCY-wW7M'),
        IF(ENDS_WITH(SourceD.FieldD,'Customer-wP7K'),'Amt-1ixP','Account-eL2n'),
        IF(
          STARTS_WITH('Address-ffOB',SourceE.FieldE), 'Account-7rSm',
          'Country-u9Zz'))
        AS FeeARyF,
      IF(
        STARTS_WITH('CCY-NCIa',DerivedTable1.AccountfEJK),
        IF(
          ENDS_WITH(DerivedTable1.CustomerWrlz,SourceE.FieldD), SourceC.FieldG,
          Table21.Code3qis), IF(
                               ENDS_WITH('Fee-4SeY',Table21.AddressEm08),
                               'Code-XVTM', SourceC.FieldH))
        AS AddressAuhg,
      IF(
        ENDS_WITH('CCY-zO8j','Country-2csr'),
        IF(
          DerivedTable1.AmtzEYq NOT LIKE 'Address-L5YL', 'Country-vUnD',
          'Amt-sOgV'), IF(
                         ENDS_WITH(SourceB.FieldE,DerivedTable2.CodeDmva),
                         'Address-y6DO', Table21.Code3qis))
        AS AmtD7IV,
      CASE
        WHEN 'Customer-PnT3' = DerivedTable1.AccountfEJK THEN
          IF(SourceC.FieldC <> 'Code-vdac',SourceE.FieldD,DerivedTable2.AmtNncS)
        WHEN DerivedTable1.AccountKwSP LIKE 'Fee-s9js' THEN
          IF(
            DerivedTable2.AccounttVM1 LIKE Table22.CountryKVrI, 'Amt-IuqE',
            DerivedTable2.Amtp4ee)
        ELSE
          CASE
            WHEN 'Address-Lapc' != 'Fee-8aMa' THEN
              'Address-Y2RC'
            WHEN ENDS_WITH(DerivedTable4.AddressB6KJ,'Account-Urqw') THEN
              'Code-nYPv'
            WHEN STARTS_WITH('Country-qZ8H',SourceD.FieldC) THEN
              'Address-yEw2'
            ELSE
              'CCY-HEvT'
          END
      END
        AS Customer1B0G,
      IF(
        'CCY-G145' <> DerivedTable1.AccountmNQo,
        IF(
          ENDS_WITH('Country-6BZP',SourceA.FieldE), DerivedTable4.CustomerCDJF,
          'CCY-ADGS'),
        IF('CCY-qUQd' NOT LIKE SourceD.FieldC,'Country-gcYa','Amt-Efjw'))
        AS Addressbd0Z,
      IF(
        STARTS_WITH(Table21.CCYOxTW,DerivedTable4.Addressm87D),
        CASE
          WHEN ENDS_WITH('Customer-6AsK',Table22.Codehp2O) THEN
            'Address-vvLK'
          WHEN DerivedTable1.FeerBIS <> 'Customer-wjpq' THEN
            'Country-9jZA'
          WHEN 'Fee-9OxO' <> 'Code-CZOZ' THEN
            SourceC.FieldF
          ELSE
            DerivedTable2.Address5RWP
        END,
        IF(ENDS_WITH(Table21.FeeafPs,'Customer-ggKs'),'CCY-WWX3','CCY-i4C6'))
        AS Countryhibo,
      IF(
        STARTS_WITH(DerivedTable4.AccounteutQ,'Country-5QRt'),
        IF(
          'Address-LBDh' LIKE SourceD.FieldE, SourceA.FieldB,
          DerivedTable4.CodeXHEv),
        IF(
          STARTS_WITH(DerivedTable4.FeexvTE,DerivedTable2.AccounttVM1),
          'Amt-QbOq', 'Address-aOiC'))
        AS AccountqzUJ,
      IF(
        'CCY-Nt6y' LIKE Table22.CCYSfd1,
        IF('Customer-hIjb' LIKE Table21.CodeDuQT,'Customer-MZQM','CCY-vdKT'),
        CASE
          WHEN STARTS_WITH(DerivedTable1.CustomerWrlz,'Code-xyRC') THEN
            'Address-hIfa'
          WHEN ENDS_WITH(SourceB.FieldH,'Account-ljoK') THEN
            'Amt-o3EH'
          WHEN 'Customer-Fkuc' = 'CCY-7UmF' THEN
            DerivedTable1.AccountqcVc
          WHEN Table21.AccountYh1Z != 'CCY-2aI6' THEN
            'CCY-Gq01'
          WHEN 'Address-2bCa' NOT LIKE DerivedTable1.FeerBIS THEN
            'Country-qjDp'
          ELSE
            SourceB.FieldG
        END)
        AS CodeSKCS,
      IF(
        STARTS_WITH('Amt-zxYY','Customer-QTYj'),
        IF(
          DerivedTable1.Codeuhr9 <> 'Country-jiq4', DerivedTable4.FeesrBh,
          Table22.FeeGWbe), IF(
                              DerivedTable4.CustomerCDJF = 'Fee-eLrc',
                              'CCY-RcIB', DerivedTable1.AddressoeLk))
        AS Fee5219,
      IF(
        ENDS_WITH(SourceB.FieldD,'Country-HizV'),
        CASE
          WHEN ENDS_WITH('Fee-WOL0','Fee-TEnt') THEN
            Table21.CCY8AOe
          WHEN ENDS_WITH(DerivedTable4.FeezLpC,SourceC.FieldB) THEN
            SourceC.FieldI
          ELSE
            'Amt-ygvF'
        END, IF('Code-bTv1' LIKE DerivedTable4.CodelvtV,'Amt-tcWK','Fee-Ptnc'))
        AS CountrysDX6,
      IF(
        ENDS_WITH(DerivedTable4.AddressB6KJ,SourceE.FieldC),
        IF(ENDS_WITH('Fee-Amlc','Country-LnXU'),'Fee-Xy2V',SourceD.FieldB),
        IF(
          ENDS_WITH('Address-7qzy','Amt-eazU'), DerivedTable4.CodeJM19,
          SourceA.FieldB))
        AS CountryXN0g,
      IF(
        STARTS_WITH(SourceD.FieldH,'Code-Ria5'),
        IF(
          DerivedTable4.CodelvtV LIKE 'Code-1YqF', SourceA.FieldJ,
          DerivedTable4.FeexvTE),
        IF(
          'Account-IPWf' <> Table22.AccountaDlm, DerivedTable4.Addressm87D,
          Table21.FeewjsN))
        AS CCYt432
    FROM
      testdataset.SourceC AS SourceC
      RIGHT JOIN testdataset.SourceA AS SourceA ON
        (ENDS_WITH('CCY-4Q2u',SourceA.FieldH) OR
         ENDS_WITH('Customer-Z9Um','Amt-2mj4') AND
         'Fee-SVwp' = 'CCY-77iF' AND STARTS_WITH(SourceC.FieldD,SourceA.FieldB))
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (STARTS_WITH(SourceC.FieldH,SourceC.FieldE) OR
         STARTS_WITH(SourceC.FieldE,SourceC.FieldG) AND
         ENDS_WITH(SourceC.FieldG,'Fee-iYAD') OR
         'Customer-EHCd' = SourceA.FieldI)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH(SourceB.FieldB,'Code-ILNW') OR
         SourceC.FieldH LIKE SourceC.FieldG AND
         'Code-wugB' != 'Amt-4p1G' AND SourceB.FieldC = 'Address-ewcq')
      INNER JOIN testdataset.SourceE AS SourceE ON
        (STARTS_WITH(SourceC.FieldG,SourceB.FieldF) OR
         STARTS_WITH('Account-pRvN','Fee-beFc') OR
         STARTS_WITH('Country-tdUF',SourceB.FieldD) OR
         SourceD.FieldE LIKE 'Address-Wdsn')
      LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (SourceB.FieldC != DerivedTable1.AccountKwSP AND
         ENDS_WITH(SourceE.FieldA,SourceD.FieldF) AND
         'Code-8gta' = SourceE.FieldH OR SourceB.FieldA <> 'Country-v5cU')
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (ENDS_WITH(DerivedTable2.AddressHdnu,SourceB.FieldH) AND
         ENDS_WITH('CCY-gZWR',SourceB.FieldF) OR
         STARTS_WITH(SourceC.FieldI,'Address-BTNj') AND
         STARTS_WITH('Address-DxUs',SourceD.FieldD))
      RIGHT JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (ENDS_WITH(SourceD.FieldH,'Address-NL8B') AND
         STARTS_WITH(SourceB.FieldE,'Code-y9XF') AND
         DerivedTable4.FeezLpC LIKE DerivedTable4.AccountMw3l AND
         ENDS_WITH('Code-8LUe','Fee-ad0K'))
      INNER JOIN Table21 AS Table21 ON
        (DerivedTable2.Account61kv != Table21.AmtPgyZ OR
         ENDS_WITH('CCY-UuqF',Table21.FeewjsN) AND
         ENDS_WITH('CCY-x93i','Customer-m1IJ') OR
         DerivedTable4.AccounteutQ != 'Address-8mxs')
      INNER JOIN Table22 AS Table22 ON
        ('Account-rAVW' NOT LIKE 'Account-bRtF' OR
         STARTS_WITH('Fee-1nIn','Amt-CEdI') AND
         SourceD.FieldC = 'Code-TZFx' OR
         STARTS_WITH(SourceC.FieldE,'Code-NG7Y'))
    WHERE
      'Account-VI1G' = 'Code-lZXs' OR
      STARTS_WITH(SourceE.FieldF,SourceD.FieldA) OR
      STARTS_WITH('Customer-Xfmy',SourceC.FieldB) OR
      'Account-9OtH' LIKE SourceA.FieldC OR
      ENDS_WITH('Country-YTMh','Country-ksuZ') AND
      STARTS_WITH(DerivedTable1.CustomerWpfX,DerivedTable2.AmtmorU) OR
      Table21.AddressdSes = DerivedTable1.AccountKwSP OR
      ENDS_WITH('Customer-ruou',SourceC.FieldI)
  ),
  Table24 AS (
    SELECT
      IF(
        STARTS_WITH(SourceB.FieldF,'Country-h3Wi'),
        IF(Table22.AmtaxC0 <> 'Code-i7Zo',DerivedTable1.AccountKwSP,'Fee-EwXR'),
        IF(
          'Address-R7hZ' NOT LIKE 'Fee-2QHC', Table22.AddressRgfZ,
          DerivedTable4.CCYY6No))
        AS CustomerqfAx,
      IF(
        SourceB.FieldC LIKE 'Code-zbUj',
        IF(
          ENDS_WITH('Amt-kIXZ','Account-9NoR'), Table21.Code3qis,
          DerivedTable3.CustomerLP0M),
        IF(
          STARTS_WITH(Table21.Codetnm3,'Account-9jyn'), 'Account-LYhP',
          'Country-UjNx'))
        AS CodejijE,
      IF(
        DerivedTable3.Account4sxv NOT LIKE 'Customer-NSzL',
        IF(
          'Code-ABZ5' NOT LIKE 'Customer-hrez', DerivedTable1.AccountfEJK,
          'Account-LE8X'), IF(
                             DerivedTable3.FeeD0Po = 'Address-Cp2T',
                             DerivedTable1.AmtzEYq, 'Account-rXRO'))
        AS AddresscniD,
      CASE
        WHEN STARTS_WITH(DerivedTable4.AccountMw3l,SourceC.FieldC) THEN
          IF(
            ENDS_WITH('Code-OTvQ','Country-dwUP'), 'Customer-KoJB',
            'Address-XOz9')
        WHEN SourceC.FieldJ <> 'Address-nsTr' THEN
          IF(
            DerivedTable4.FeesrBh != SourceE.FieldC, Table21.Countryl6wQ,
            'Account-WVBG')
        WHEN STARTS_WITH(DerivedTable3.CustomereBLI,'Amt-e8Gg') THEN
          IF(
            'Account-bH9t' LIKE 'Amt-1ih6', DerivedTable3.CustomerULyd,
            DerivedTable2.AddressHdnu)
        ELSE
          IF('Amt-d7cV' <> 'Customer-Dylp','Country-4UWG','CCY-trtn')
      END
        AS Feecmli,
      CASE
        WHEN STARTS_WITH(SourceB.FieldJ,'CCY-aG8f') THEN
          IF(
            SourceC.FieldI LIKE Table22.AddressRgfZ, DerivedTable3.FeeD0Po,
            'Address-tNpD')
        WHEN 'Account-1XOj' LIKE 'Customer-DlhP' THEN
          IF(
            'Address-v0RF' NOT LIKE DerivedTable1.Addressrgjd,
            DerivedTable1.AddressoeLk, Table22.CCYSfd1)
        WHEN 'Address-uUOG' <> 'CCY-zDU4' THEN
          CASE
            WHEN 'Address-EoMp' LIKE 'Account-LMXa' THEN
              DerivedTable1.FeerBIS
            WHEN 'Address-N5MO' != DerivedTable3.CodeUeLq THEN
              'Code-pbJX'
            WHEN 'Fee-HiQK' LIKE DerivedTable1.CustomerWrlz THEN
              'Code-ezcC'
            WHEN DerivedTable1.Addressrgjd NOT LIKE 'Amt-Tz5Y' THEN
              'Amt-pJ1Q'
            WHEN 'CCY-8Eyl' <> DerivedTable3.CodeUeLq THEN
              'Amt-0bWz'
            ELSE
              SourceB.FieldD
          END
        ELSE
          IF(SourceE.FieldD NOT LIKE SourceA.FieldE,'Amt-Dr2f','CCY-wasm')
      END
        AS Coder5SD,
      IF(
        'Country-kdge' LIKE Table21.CCYOxTW,
        IF(
          DerivedTable3.CountrygMB4 LIKE 'Address-gVzF', 'Code-Ooxo',
          SourceA.FieldA),
        CASE
          WHEN ENDS_WITH('Account-AHQa','Customer-YVGg') THEN
            'CCY-q8H6'
          WHEN ENDS_WITH('CCY-5vLu','Country-RJ8D') THEN
            Table22.Customer6SmR
          WHEN 'Amt-DxTV' != Table22.AddressRgfZ THEN
            Table23.Countryhibo
          WHEN ENDS_WITH(DerivedTable3.CCYO742,SourceE.FieldI) THEN
            'CCY-5IoA'
          ELSE
            Table22.AddressOyCN
        END)
        AS AddressPa6a,
      IF(
        'Amt-7ZDF' = 'Amt-mrnK',
        IF(ENDS_WITH('Account-imZo',SourceC.FieldA),SourceB.FieldF,'CCY-ouu1'),
        IF(
          ENDS_WITH(SourceD.FieldE,'Address-87gx'), DerivedTable4.AccountMw3l,
          'Code-zeKz'))
        AS Codesj0X,
      IF(
        STARTS_WITH('Code-aCaP',DerivedTable1.AmtzEYq),
        CASE
          WHEN ENDS_WITH(SourceC.FieldG,'Code-nGAT') THEN
            DerivedTable4.CCYY6No
          WHEN ENDS_WITH(Table23.CodeSKCS,'CCY-zzRr') THEN
            DerivedTable1.CustomerKrwR
          WHEN STARTS_WITH(Table21.FeeMDz0,SourceC.FieldI) THEN
            'Country-3R6f'
          WHEN ENDS_WITH(SourceC.FieldG,'Amt-8mTZ') THEN
            SourceD.FieldE
          ELSE
            Table21.AccountYh1Z
        END, IF(
               ENDS_WITH('Address-EQHc','Account-urHK'), DerivedTable4.FeezLpC,
               SourceD.FieldB))
        AS CCYsiFc,
      CASE
        WHEN 'Country-j1gs' LIKE SourceD.FieldG THEN
          IF(
            'Customer-QaKN' <> 'Amt-0lji', DerivedTable3.CountrygMB4,
            'Code-mPvU')
        WHEN 'Customer-UvqJ' NOT LIKE 'Fee-FIPX' THEN
          IF('Customer-3lir' <> 'Fee-fQiF','Address-aCiX',Table21.AddressEm08)
        WHEN 'Account-cTWg' NOT LIKE 'CCY-Dpgz' THEN
          CASE
            WHEN DerivedTable3.FeeF6P4 LIKE 'Customer-cSCt' THEN
              'Address-62Hb'
            WHEN SourceC.FieldJ <> DerivedTable1.AccountKwSP THEN
              DerivedTable4.FeesrBh
            WHEN 'Amt-aUGG' <> DerivedTable2.AmtNncS THEN
              'Code-cQFW'
            WHEN STARTS_WITH('Fee-t8HW',DerivedTable1.AmtzEYq) THEN
              'Amt-iRXQ'
            WHEN ENDS_WITH(DerivedTable2.Address7oYI,SourceA.FieldE) THEN
              Table21.AddressdSes
            ELSE
              Table21.CCYANyf
          END
        WHEN DerivedTable1.AmtzEYq = DerivedTable1.AddressoeLk THEN
          CASE
            WHEN STARTS_WITH(DerivedTable2.AccountctCy,'Amt-9FYU') THEN
              SourceD.FieldA
            WHEN STARTS_WITH(DerivedTable4.Country8VtM,Table21.FeeMDz0) THEN
              Table23.CountryXN0g
            WHEN 'Amt-MBOq' != SourceD.FieldG THEN
              Table21.Code3qis
            ELSE
              'CCY-0Hxz'
          END
        WHEN 'Country-ZtZg' != 'Address-dtk8' THEN
          IF(
            'CCY-0kY0' NOT LIKE SourceD.FieldD, DerivedTable3.CodeVHWz,
            SourceC.FieldD)
        ELSE
          IF(
            Table23.Countryhibo NOT LIKE 'Country-r8zy', Table21.AmtUKmU,
            'Country-fN2b')
      END
        AS Account8Q9w,
      IF(
        STARTS_WITH(DerivedTable3.Feeg8kl,'CCY-BIi6'),
        IF(
          ENDS_WITH('Customer-PVKu','Customer-wUFR'), DerivedTable2.AccountctCy,
          SourceD.FieldA),
        IF('Code-2cll' LIKE DerivedTable3.Codeaqvh,'Account-gkIo','Code-hTUW'))
        AS AmtVc9J,
      IF(
        'Fee-C4Mw' LIKE 'Address-dclr',
        IF(
          DerivedTable3.CodeUeLq != Table23.Fee5219, 'Code-uowm',
          'Country-V4wc'), IF(
                             ENDS_WITH(Table21.Countryl6wQ,'Customer-xgNo'),
                             SourceC.FieldB, DerivedTable3.FeeqkjX))
        AS CodeLw2M,
      CASE
        WHEN 'Account-TYNs' <> DerivedTable3.CodeVHWz THEN
          IF(
            'Fee-NL8p' <> SourceD.FieldJ, 'Account-uqEC',
            DerivedTable1.AccountfEJK)
        WHEN DerivedTable3.FeeCUde = SourceD.FieldJ THEN
          IF(
            SourceA.FieldE NOT LIKE DerivedTable1.AccountKwSP, 'Fee-Ic1u',
            Table23.AccountlSjE)
        WHEN SourceA.FieldG <> 'Account-hZra' THEN
          IF(
            ENDS_WITH('Country-d0Ky',DerivedTable2.AccountrJZf), 'Fee-7oWJ',
            'Country-cnbV')
        WHEN ENDS_WITH(DerivedTable1.AccountmNQo,'Customer-fuKq') THEN
          IF(
            DerivedTable1.AccountKwSP NOT LIKE DerivedTable4.Country5f1u,
            'Fee-NHOR', 'Address-lPk8')
        ELSE
          IF(
            'Customer-Bpjj' != Table23.CountryXN0g, SourceA.FieldC,
            'Account-RYuR')
      END
        AS CountryPmkh,
      CASE
        WHEN ENDS_WITH('Fee-0Zh6',DerivedTable1.AccountKwSP) THEN
          IF('Amt-UGwx' != 'Address-mun8','Code-YX0s',DerivedTable3.FeeF6P4)
        WHEN SourceB.FieldG <> SourceA.FieldH THEN
          IF(Table23.AmtD7IV LIKE 'Address-MWOe','CCY-4vaE',Table21.AccountYh1Z)
        WHEN DerivedTable3.FeeqkjX <> 'Amt-oJjg' THEN
          IF(
            DerivedTable1.CustomerKrwR != 'Code-zNLn',
            DerivedTable3.CountryoFFA, DerivedTable3.AccountViga)
        WHEN STARTS_WITH('Amt-uHc8','Address-EQvh') THEN
          IF(SourceC.FieldF <> 'Address-v7x7',SourceD.FieldB,'Country-CwUE')
        WHEN SourceD.FieldC = Table21.CCYOxTW THEN
          IF(
            ENDS_WITH(DerivedTable2.CodeRziT,'Amt-FP1E'), Table23.CCYt432,
            'Address-5xeS')
        ELSE
          IF('Fee-oH3H' LIKE 'Amt-4EOC','CCY-lHJl',Table22.AmtaxC0)
      END
        AS FeeMjtG,
      IF(
        STARTS_WITH(DerivedTable2.AddressHdnu,SourceA.FieldJ),
        IF(
          STARTS_WITH(SourceD.FieldG,Table21.CountryMugA), Table23.CodeSKCS,
          DerivedTable2.Address7oYI),
        IF('Country-foRS' <> 'Address-GzSr','Country-jLhg','CCY-IwsV'))
        AS Address41BC,
      IF(
        ENDS_WITH(SourceA.FieldE,'Fee-yPsC'),
        IF('Address-cUfq' LIKE SourceA.FieldA,'Address-iwvM','Country-9fpM'),
        IF(SourceE.FieldJ NOT LIKE 'Fee-kdUh','Fee-xrZG','Account-uxEE'))
        AS Amta2KH,
      IF(
        Table21.FeeafPs != 'Amt-swK6',
        IF(
          ENDS_WITH(SourceD.FieldH,DerivedTable1.FeerBIS), SourceD.FieldD,
          'Amt-5Zjv'), IF(
                         DerivedTable4.CustomerCDJF != DerivedTable1.AmtR86V,
                         Table21.AddressdSes, SourceC.FieldF))
        AS CustomeretHd,
      IF(
        STARTS_WITH('Account-GW7B','Fee-0HF0'),
        CASE
          WHEN SourceD.FieldH <> 'Amt-tWgp' THEN
            'Code-ASXo'
          WHEN ENDS_WITH('Fee-NGhr','Account-tSIX') THEN
            DerivedTable3.FeeT7G9
          WHEN 'Address-behN' != 'CCY-8exG' THEN
            DerivedTable3.CodeUeLq
          WHEN ENDS_WITH('Fee-t4qk',DerivedTable3.CountryoFFA) THEN
            'Account-g3DE'
          WHEN ENDS_WITH(DerivedTable4.AddressB6KJ,'Address-nXlb') THEN
            DerivedTable2.AccountctCy
          ELSE
            SourceB.FieldJ
        END, IF(
               ENDS_WITH(SourceD.FieldC,SourceC.FieldB), Table21.AmtPgyZ,
               'Account-HTrR'))
        AS Addressavrf,
      IF(
        ENDS_WITH(DerivedTable3.CountryoFFA,'Account-qnyR'),
        CASE
          WHEN DerivedTable4.FeezLpC = 'Amt-04VU' THEN
            'Code-pm5n'
          WHEN SourceE.FieldH = DerivedTable3.Feeg8kl THEN
            'Country-rPoV'
          ELSE
            Table21.Countryl6wQ
        END,
        CASE
          WHEN ENDS_WITH('Country-qVQN','Account-D1oN') THEN
            'Amt-LtPD'
          WHEN ENDS_WITH(Table21.Amt7FNh,'Code-UiVg') THEN
            'Country-Khuu'
          WHEN ENDS_WITH(DerivedTable3.FeeqkjX,'Customer-pSfV') THEN
            'Country-1cgj'
          WHEN ENDS_WITH('Code-G4cI','Account-Kiid') THEN
            SourceC.FieldG
          WHEN DerivedTable4.FeexvTE <> 'Address-BSVF' THEN
            DerivedTable4.CodeJM19
          ELSE
            DerivedTable2.CodeDmva
        END)
        AS CountryGRsB,
      IF(
        'CCY-GQYI' <> Table21.Countryl6wQ,
        IF('Fee-2Nqv' LIKE 'Country-TRUJ','Account-3Qds','Code-MEnm'),
        IF(
          ENDS_WITH(Table22.CCYSfd1,DerivedTable4.AddressB6KJ), SourceA.FieldG,
          'Customer-tNnX'))
        AS CustomerxJTO
    FROM
      Table22 AS Table22
      LEFT JOIN testdataset.SourceA AS SourceA ON
        (STARTS_WITH(Table22.CountryCpDv,Table22.Codehp2O) OR
         Table22.AmtaxC0 NOT LIKE 'Country-WMq9' AND
         'Country-RCpl' NOT LIKE SourceA.FieldI OR
         STARTS_WITH(SourceA.FieldI,'Code-7CLJ'))
      RIGHT JOIN testdataset.SourceB AS SourceB ON
        ('Amt-hA3K' != 'Country-indL' OR
         ENDS_WITH('Amt-fEXw','Country-Vomb') AND
         SourceA.FieldF != 'Code-gwLd' OR 'Amt-AofJ' NOT LIKE 'Code-mYlT')
      INNER JOIN testdataset.SourceC AS SourceC ON
        (STARTS_WITH(SourceB.FieldA,Table22.Codehp2O) AND
         STARTS_WITH('Country-7prY',SourceB.FieldJ) OR
         'Customer-oyVe' LIKE SourceA.FieldC OR
         SourceA.FieldC = Table22.Codehp2O)
      INNER JOIN testdataset.SourceD AS SourceD ON
        (ENDS_WITH('Country-rcQO','Account-XT0J') AND
         ENDS_WITH(SourceA.FieldA,'Fee-zXpA') AND
         'CCY-CBHW' = Table22.Customer6SmR OR
         ENDS_WITH(SourceC.FieldB,SourceC.FieldJ))
      RIGHT JOIN testdataset.SourceE AS SourceE ON
        (SourceC.FieldE = Table22.CountryKVrI AND
         ENDS_WITH(SourceE.FieldJ,SourceD.FieldG) AND
         SourceA.FieldD <> 'CCY-qW7J' AND SourceB.FieldI != SourceC.FieldB)
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        (SourceB.FieldA = SourceB.FieldG OR
         ENDS_WITH('Code-k0Fo',DerivedTable1.CustomerKrwR) AND
         SourceE.FieldI != DerivedTable1.AccountKwSP AND
         STARTS_WITH('Code-GwIH','Customer-LB5p'))
      LEFT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        ('Customer-n0Y3' LIKE SourceC.FieldH OR
         STARTS_WITH(SourceA.FieldD,Table22.Customer6SmR) AND
         'Code-Jha3' != 'Country-PHeh' OR
         ENDS_WITH(SourceE.FieldE,Table22.AmtaxC0))
      LEFT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (SourceC.FieldH = 'Customer-n1dY' AND
         STARTS_WITH(SourceB.FieldG,'Customer-aY4g') OR
         'Fee-xJoY' NOT LIKE 'Code-EcyS' AND 'Customer-TAF0' = 'Code-OQBW')
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        (DerivedTable4.Account1SJF NOT LIKE DerivedTable3.FeeCUde OR
         ENDS_WITH('Account-LNZS','Amt-ZQfq') AND
         SourceE.FieldC <> 'Account-RJBB' OR
         ENDS_WITH(SourceB.FieldJ,'Amt-rPpN'))
      LEFT JOIN Table21 AS Table21 ON
        (STARTS_WITH('Account-5xXk','Account-jvCB') AND
         DerivedTable3.Codeaqvh = DerivedTable1.AccountqcVc AND
         ENDS_WITH('Code-2T0w','Customer-KZnM') OR
         DerivedTable4.CodeXHEv NOT LIKE 'CCY-B4DD')
      RIGHT JOIN Table23 AS Table23 ON
        (ENDS_WITH('Fee-LfuK',SourceC.FieldE) OR
         ENDS_WITH('CCY-v27E',DerivedTable3.CodeUeLq) AND
         ENDS_WITH(DerivedTable4.FeesrBh,Table21.AmtPgyZ) AND
         SourceA.FieldA LIKE Table21.Code3qis)
    WHERE
      ENDS_WITH('Address-Vv9n',SourceA.FieldF) AND
      STARTS_WITH(SourceA.FieldI,SourceE.FieldF) OR
      STARTS_WITH('Country-NTFJ','Customer-nI0H') AND
      'Fee-TiDS' != 'Country-15k8' OR
      STARTS_WITH('Customer-6Zas',SourceD.FieldG) AND
      'Fee-PsSn' NOT LIKE 'Code-2dnP' AND
      ENDS_WITH(DerivedTable1.CustomerWrlz,'Address-DuvW') OR
      ENDS_WITH(DerivedTable1.CCYXSkS,'Customer-e45Z')
  ),
  Table25 AS (
    SELECT
      IF(
        DerivedTable3.CodeUeLq != Table22.AmtaxC0,
        IF(
          ENDS_WITH(SourceC.FieldE,DerivedTable3.FeeqkjX), 'Amt-jnSu',
          DerivedTable3.CountryoFFA),
        IF(
          'Country-QyWg' LIKE 'Address-pH4y', SourceD.FieldI,
          DerivedTable1.AccountfEJK))
        AS Amt9Ka3,
      IF(
        ENDS_WITH('Code-U5Wa',DerivedTable3.CodeVHWz),
        IF(
          Table22.CountryQJTJ != 'Country-NpSp', Table23.Addressbd0Z,
          'Customer-iWI3'),
        IF(SourceD.FieldI <> 'Account-RQ0T','Code-YEPM','Amt-rilo'))
        AS CountryUSxc,
      IF(
        ENDS_WITH(SourceD.FieldG,'Customer-rDVG'),
        IF(
          DerivedTable4.CustomerCDJF <> DerivedTable4.FeezLpC,
          Table23.Addressbd0Z, DerivedTable1.AddressoeLk),
        IF(
          'Account-V5Wc' NOT LIKE DerivedTable3.Codeaqvh, 'CCY-hm0g',
          'CCY-InYx'))
        AS Feer2Bt,
      IF(
        STARTS_WITH(DerivedTable3.FeeCUde,'Code-UbbV'),
        CASE
          WHEN ENDS_WITH('Amt-BJL7','Amt-tTrc') THEN
            'CCY-fE5l'
          WHEN DerivedTable4.FeexvTE LIKE 'Customer-zwfj' THEN
            DerivedTable3.CountrygPvi
          ELSE
            'Amt-mXxV'
        END, CASE
               WHEN 'Code-oHMB' = 'Customer-HWk4' THEN
                 'CCY-ml40'
               WHEN ENDS_WITH('Amt-RiZH','Customer-ZoL6') THEN
                 DerivedTable2.AccounttVM1
               ELSE
                 'Account-j1zi'
             END)
        AS CodeWfI9,
      IF(
        SourceC.FieldC = 'Amt-Gmfm',
        IF(
          STARTS_WITH(SourceC.FieldH,'Code-FEfj'), 'Address-aldw',
          DerivedTable4.CCYY6No),
        IF(
          SourceE.FieldF NOT LIKE 'Address-0dnC', 'Address-rKfR',
          SourceC.FieldI))
        AS Customerp0cg,
      IF(
        DerivedTable4.FeezLpC NOT LIKE 'Address-7rnN',
        IF(ENDS_WITH('Code-5fFR','CCY-pFfK'),DerivedTable2.AmtNncS,'Code-Zjrd'),
        IF(
          SourceD.FieldB <> Table22.AmtaxC0, SourceE.FieldH,
          DerivedTable4.AccounteutQ))
        AS Customer55OD,
      IF(
        SourceA.FieldB = 'CCY-QqBw',
        IF(
          ENDS_WITH('Amt-oCo6',DerivedTable4.Country5f1u),
          DerivedTable3.CustomereBLI, Table21.FeeMDz0),
        IF('Fee-9W2l' <> DerivedTable3.CustomerULyd,'CCY-6UTZ',SourceA.FieldD))
        AS FeeA4pU,
      IF(
        'Country-RJad' = 'Country-yZyO',
        IF(
          ENDS_WITH('Fee-GA5m',DerivedTable4.Country5f1u), Table22.CountryKVrI,
          'Code-YTHN'),
        IF('Customer-fRYI' <> SourceA.FieldG,'CCY-zRK3','Fee-JV8z'))
        AS Codeq40v,
      IF(
        DerivedTable3.FeeqkjX NOT LIKE 'Address-ot8d',
        IF(
          STARTS_WITH('Country-D4js',DerivedTable3.CodeVHWz), 'Country-ahE4',
          'CCY-fIBA'), IF(
                         STARTS_WITH(SourceE.FieldA,Table22.AddressOyCN),
                         SourceD.FieldJ, 'Customer-uI0p'))
        AS CCYqEfR,
      IF(
        STARTS_WITH(DerivedTable4.Country8VtM,Table21.FeewjsN),
        IF(
          ENDS_WITH('CCY-uWQK',DerivedTable4.FeezLpC), 'Country-lV11',
          Table23.CCYt432),
        CASE
          WHEN STARTS_WITH(Table23.AddressAuhg,SourceC.FieldG) THEN
            'Account-2mKi'
          WHEN ENDS_WITH(SourceE.FieldF,'CCY-45AO') THEN
            'CCY-BtVg'
          WHEN 'Country-snZ7' LIKE 'CCY-e74h' THEN
            DerivedTable1.CountryV1io
          ELSE
            'Customer-F89K'
        END)
        AS Fee7dB5,
      IF(
        SourceB.FieldH != 'Fee-JFJP',
        IF('CCY-90Ls' <> 'Account-k2mV','Code-eLr0','Address-aqJK'),
        IF(Table21.FeewjsN <> SourceD.FieldJ,'Country-HWor','Amt-Czle'))
        AS FeeAjrz,
      CASE
        WHEN ENDS_WITH(Table23.CountryXN0g,Table23.Customer1B0G) THEN
          IF(
            STARTS_WITH(SourceE.FieldG,Table23.AccountqzUJ), 'Customer-iDgw',
            SourceB.FieldC)
        WHEN DerivedTable2.AddressHdnu NOT LIKE SourceC.FieldI THEN
          IF(
            STARTS_WITH(SourceC.FieldE,DerivedTable1.AccountmNQo),
            DerivedTable3.CodeUeLq, 'Address-mQYm')
        WHEN DerivedTable3.CustomerULyd != 'Fee-ac2p' THEN
          IF(
            Table21.CCYANyf <> DerivedTable2.CodeRziT, DerivedTable2.Amtp4ee,
            'Country-7cQu')
        ELSE
          IF(
            STARTS_WITH(DerivedTable2.Amtp4ee,DerivedTable4.AccounteutQ),
            Table23.CountryXN0g, 'Amt-9c6R')
      END
        AS Customer04IU,
      IF(
        Table23.Customer1B0G != Table23.AddressAuhg,
        IF(
          STARTS_WITH(DerivedTable1.AccountqcVc,Table21.Codetnm3), 'Code-xWPq',
          Table21.AddressTZ4C),
        IF(
          ENDS_WITH(DerivedTable4.CustomerCDJF,DerivedTable4.CodelvtV),
          'Amt-Wg2t', DerivedTable2.AccountctCy))
        AS FeeB9tM,
      IF(
        'Country-A71f' NOT LIKE 'Customer-baoj',
        IF(
          ENDS_WITH('Customer-QKYF','Customer-PE1o'), Table23.AccountqzUJ,
          DerivedTable3.FeepQGh),
        IF(
          ENDS_WITH('Fee-U4iF','Account-uQS5'), Table21.AddressEm08,
          'Customer-AJ92'))
        AS CCY8FX9
    FROM
      Table23 AS Table23
      LEFT JOIN testdataset.SourceA AS SourceA ON
        ('Fee-anSC' != 'CCY-Svuw' AND 'Account-YJ3S' LIKE 'Account-rjmq' OR
         ENDS_WITH('Country-3GJA',SourceA.FieldA) OR
         'Code-AxkB' != 'Customer-zz3U')
      LEFT JOIN testdataset.SourceB AS SourceB ON
        (ENDS_WITH(Table23.CountrysDX6,Table23.Fee5219) AND
         ENDS_WITH(Table23.AmtD7IV,SourceB.FieldE) AND
         'Fee-xRRu' <> SourceB.FieldE AND 'Address-cqLD' = SourceA.FieldB)
      RIGHT JOIN testdataset.SourceC AS SourceC ON
        (Table23.Countryhibo <> 'Amt-YkGc' AND
         Table23.AmtD7IV NOT LIKE 'Customer-DsyC' OR
         STARTS_WITH('Country-lirt',SourceB.FieldC) AND
         'Country-yTND' <> SourceB.FieldJ)
      RIGHT JOIN testdataset.SourceD AS SourceD ON
        (SourceC.FieldA NOT LIKE SourceD.FieldC OR
         ENDS_WITH(SourceD.FieldD,SourceC.FieldH) OR
         STARTS_WITH('Customer-wqN8',SourceC.FieldJ) AND
         Table23.AccountlSjE LIKE SourceC.FieldF)
      INNER JOIN testdataset.SourceE AS SourceE ON
        (SourceA.FieldA NOT LIKE SourceD.FieldJ AND
         SourceD.FieldE = SourceD.FieldI OR
         SourceC.FieldD <> 'CCY-JvXb' AND
         ENDS_WITH(SourceE.FieldG,SourceA.FieldB))
      RIGHT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
        ('CCY-dNbj' <> DerivedTable1.AddressoeLk AND
         STARTS_WITH(SourceC.FieldI,'Country-8CK0') AND
         ENDS_WITH(SourceE.FieldG,DerivedTable1.AccountfEJK) AND
         'CCY-3Lpz' NOT LIKE DerivedTable1.CustomerKrwR)
      RIGHT JOIN testdataset.DerivedTable2 AS DerivedTable2 ON
        (STARTS_WITH('Fee-q7Lv',DerivedTable2.Amt885g) OR
         ENDS_WITH(DerivedTable1.Codeuhr9,SourceD.FieldC) AND
         SourceB.FieldG <> 'Country-pmGp' OR SourceD.FieldF != SourceE.FieldD)
      RIGHT JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
        (ENDS_WITH(DerivedTable1.CustomerWrlz,'Country-7uV0') OR
         'Amt-UlDs' = DerivedTable3.CustomereBLI AND
         STARTS_WITH(DerivedTable1.AccountqcVc,SourceE.FieldF) OR
         STARTS_WITH(DerivedTable2.AccountrJZf,'Amt-qB9N'))
      INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
        ('Amt-L5yD' != DerivedTable3.Account4sxv AND
         'Fee-DYP1' <> 'Customer-rPOG' AND
         ENDS_WITH(DerivedTable1.CustomerWrlz,'CCY-2nvr') OR
         SourceC.FieldF != SourceA.FieldI)
      INNER JOIN Table21 AS Table21 ON
        (STARTS_WITH(Table21.CountryMugA,Table21.AddressEm08) AND
         ENDS_WITH(Table21.CCYANyf,'Country-9d91') AND
         'Customer-ZBtU' != DerivedTable1.FeerBIS OR
         DerivedTable1.AccountmNQo = 'Customer-a90l')
      LEFT JOIN Table22 AS Table22 ON
        ('CCY-Gg4l' <> SourceA.FieldA AND
         ENDS_WITH('Amt-30EZ',DerivedTable3.FeeqkjX) AND
         STARTS_WITH('Country-vb6e',DerivedTable4.CodeXHEv) AND
         STARTS_WITH('Code-KHCp',Table22.CCYSfd1))
    WHERE
      'Account-BfRc' = SourceB.FieldG OR
      ENDS_WITH(SourceB.FieldA,DerivedTable1.Addressrgjd) OR
      DerivedTable1.CustomerWrlz = DerivedTable4.FeezLpC OR
      STARTS_WITH('Account-qZau','Address-mAgS') OR
      'Fee-LFUg' NOT LIKE DerivedTable2.AddresscM17 AND
      'CCY-8rqL' != DerivedTable4.FeezLpC AND
      STARTS_WITH(SourceD.FieldH,DerivedTable3.CustomerLP0M) AND
      Table21.CountryMugA <> DerivedTable2.CodeRziT
  )
SELECT
  CASE
    WHEN STARTS_WITH(SourceE.FieldB,DerivedTable4.AccounteutQ) THEN
      IF(
        DerivedTable2.AmtNncS NOT LIKE DerivedTable4.AccountMw3l, 'Amt-t5zA',
        'Customer-soO6')
    WHEN ENDS_WITH('Fee-oLzo',SourceD.FieldJ) THEN
      IF(STARTS_WITH('Customer-bCFK','CCY-CT5e'),'Customer-JURk','Account-HJQA')
    WHEN SourceB.FieldC = Table23.CountryXN0g THEN
      IF(
        'Code-MOjE' NOT LIKE 'Address-0eSe', 'Customer-lVDD',
        DerivedTable2.AmtmorU)
    WHEN STARTS_WITH(DerivedTable1.AccountqcVc,'Country-QUpu') THEN
      IF('Customer-ctSs' = 'Amt-2TPj',Table25.FeeA4pU,'Fee-5FhL')
    ELSE
      IF(
        ENDS_WITH('Account-JLZZ','CCY-ISnv'), 'Country-mVMn',
        DerivedTable3.Codeaqvh)
  END
    AS Feepmor,
  CASE
    WHEN DerivedTable3.Feeg8kl NOT LIKE 'Country-KqHL' THEN
      IF(ENDS_WITH('Account-hZLA','CCY-DV7D'),'Customer-3ZtR',SourceC.FieldG)
    WHEN DerivedTable1.AccountqcVc NOT LIKE 'Code-wzCA' THEN
      CASE
        WHEN 'Fee-Jong' != 'CCY-rYjl' THEN
          'Fee-C7Mc'
        WHEN STARTS_WITH(SourceB.FieldC,Table22.Customer6SmR) THEN
          'Country-nnZY'
        WHEN Table23.Addressbd0Z = 'CCY-A8op' THEN
          DerivedTable2.AmtNncS
        ELSE
          'Amt-4Vae'
      END
    WHEN 'Amt-o9s7' <> 'Code-oZ1B' THEN
      IF(
        STARTS_WITH('Code-BgCd',SourceA.FieldD), 'Customer-G76a',
        DerivedTable3.CodeVHWz)
    WHEN ENDS_WITH(SourceA.FieldA,Table22.AddressOyCN) THEN
      IF(Table21.CountrysAfr = Table21.AmtUKmU,'Country-dEwj',Table21.AmtUKmU)
    ELSE
      CASE
        WHEN STARTS_WITH(Table21.CCY8AOe,'Amt-eTbW') THEN
          'Fee-ETJL'
        WHEN Table21.AddressEm08 <> 'Code-HBHG' THEN
          'CCY-NG2j'
        ELSE
          SourceC.FieldA
      END
  END
    AS CCY76AX,
  IF(
    Table25.CodeWfI9 != SourceA.FieldC,
    IF(
      STARTS_WITH(DerivedTable3.Code3Xip,'Fee-BG4N'), Table21.CountryMugA,
      SourceA.FieldA),
    IF(Table25.Amt9Ka3 <> 'Amt-palk',SourceE.FieldI,'Address-6g8M'))
    AS AddressV4AX,
  CASE
    WHEN 'Fee-wZY2' != 'Country-gTwj' THEN
      CASE
        WHEN 'Code-qklb' = Table24.CustomerqfAx THEN
          'Address-Q3h0'
        WHEN STARTS_WITH(DerivedTable2.AmtmorU,'Code-1D8a') THEN
          'Account-4Oib'
        WHEN STARTS_WITH('Country-FnAa','Fee-Ccrg') THEN
          'Customer-8PjQ'
        WHEN 'Amt-nzXU' NOT LIKE SourceE.FieldG THEN
          SourceC.FieldA
        ELSE
          'Account-mCFw'
      END
    WHEN STARTS_WITH(Table21.AddressdSes,'Address-J0cm') THEN
      IF('Address-lVKC' = Table23.CCYt432,'Account-I8BJ','Code-oIXt')
    WHEN STARTS_WITH('Fee-ng10',Table25.CodeWfI9) THEN
      IF(Table22.CountryKVrI LIKE 'Fee-V2Eb','Amt-C2jD','Amt-j0Pi')
    WHEN STARTS_WITH('CCY-Zc1U','Fee-TgFA') THEN
      IF(
        STARTS_WITH('Customer-3h6G',DerivedTable3.CustomerULyd),
        'Customer-XIhF', 'Amt-ZwHt')
    WHEN DerivedTable4.Addressm87D <> 'Account-aqFe' THEN
      IF(
        STARTS_WITH('Amt-R7Bd','CCY-Vvkk'), DerivedTable2.CountrywaWi,
        'Country-f1GR')
    ELSE
      IF(
        DerivedTable4.Account1SJF = Table24.CountryPmkh, 'Fee-MpgD',
        'Address-OIXj')
  END
    AS CCYQK2Q,
  IF(
    ENDS_WITH('Amt-bRBO','Address-RdhL'),
    IF(
      'CCY-xhDz' NOT LIKE 'Account-tH7o', DerivedTable1.AmtzEYq,
      Table21.FeewjsN),
    CASE
      WHEN SourceD.FieldB = 'Country-W3m1' THEN
        'CCY-uNEY'
      WHEN STARTS_WITH('Country-MvF9','Customer-RX7E') THEN
        Table21.FeeMDz0
      ELSE
        'Fee-ArRR'
    END)
    AS CountrynpDj,
  IF(
    Table24.CodejijE != DerivedTable3.AccountViga,
    IF(Table25.Fee7dB5 <> 'Fee-STIy','Account-tuy1',SourceC.FieldI),
    CASE
      WHEN ENDS_WITH(Table23.AddressAuhg,DerivedTable1.CustomerWrlz) THEN
        SourceA.FieldI
      WHEN ENDS_WITH('Customer-7VSi',DerivedTable1.AmtR86V) THEN
        'CCY-w8GN'
      ELSE
        Table23.CountrysDX6
    END)
    AS CCYVD4h,
  CASE
    WHEN ENDS_WITH('Amt-ZvpL',DerivedTable3.CodeUeLq) THEN
      IF(
        STARTS_WITH('CCY-B7oO',DerivedTable2.AccountrJZf), 'Country-RkBq',
        DerivedTable3.FeeD0Po)
    WHEN ENDS_WITH(DerivedTable4.CCYY6No,Table24.FeeMjtG) THEN
      CASE
        WHEN DerivedTable2.CodeDmva != 'Address-yPBU' THEN
          Table23.CCYt432
        WHEN ENDS_WITH(DerivedTable1.AccountKwSP,DerivedTable4.Account1SJF) THEN
          'Amt-UmSS'
        WHEN SourceD.FieldE <> 'Code-yCk7' THEN
          'Country-ONqH'
        WHEN Table25.CCYqEfR != DerivedTable3.CodeVHWz THEN
          'Amt-Fz30'
        ELSE
          'CCY-HeKm'
      END
    WHEN SourceE.FieldD <> 'Country-hLFS' THEN
      IF(SourceA.FieldA != 'Account-y48N',SourceD.FieldA,SourceC.FieldH)
    WHEN SourceD.FieldE LIKE DerivedTable4.FeexvTE THEN
      IF('Customer-bwbL' = 'Address-6woX',Table21.CCYANyf,Table21.AddressTZ4C)
    WHEN ENDS_WITH('Fee-YM9h','CCY-NT9g') THEN
      IF(
        STARTS_WITH(DerivedTable1.AccountfEJK,DerivedTable3.CustomereBLI),
        DerivedTable3.Account4sxv, 'Country-0qlN')
    ELSE
      IF(
        ENDS_WITH('Customer-VpUp',SourceA.FieldE), DerivedTable4.CodelvtV,
        Table23.FeeARyF)
  END
    AS CodePGRC,
  IF(
    Table22.AddressRgfZ != Table21.AccountYh1Z,
    IF(SourceA.FieldD NOT LIKE Table21.AmtPgyZ,'Country-cL2a','Address-fCqT'),
    IF('Code-Ngux' LIKE 'Amt-qIEp',Table21.CCYOxTW,DerivedTable4.CodeXHEv))
    AS Code7INp,
  CASE
    WHEN 'Country-2PHM' != 'Fee-Vjfn' THEN
      IF(
        STARTS_WITH('Country-tqhM',DerivedTable3.FeeqkjX), SourceC.FieldF,
        SourceB.FieldB)
    WHEN 'CCY-MvFE' NOT LIKE DerivedTable4.CCYY6No THEN
      IF(
        DerivedTable1.AmtR86V != Table25.Customer55OD, 'Fee-O7kv',
        Table22.CountryKVrI)
    WHEN 'Fee-dZZ4' NOT LIKE 'Address-6RLH' THEN
      IF(SourceD.FieldH <> SourceC.FieldC,Table23.AddressAuhg,'Account-snMR')
    ELSE
      IF(STARTS_WITH(Table23.AccountlSjE,'Fee-hT7C'),SourceC.FieldB,'Amt-fDr3')
  END
    AS FeeogF5,
  IF(
    'Code-MbYt' NOT LIKE 'Customer-pnAr',
    IF(
      STARTS_WITH('Fee-LU6m','CCY-NsSg'), DerivedTable3.CountryoFFA,
      'Code-B4bq'), IF(
                      ENDS_WITH('Account-oT1G','Address-3Q3J'),
                      DerivedTable1.Addressrgjd, Table25.Customer55OD))
    AS Customera0qh
FROM
  testdataset.DerivedTable2 AS DerivedTable2
  RIGHT JOIN testdataset.SourceA AS SourceA ON
    ('Code-xfi5' != DerivedTable2.Account61kv OR
     STARTS_WITH('Code-deSq','CCY-A7Q5') AND
     ENDS_WITH('Country-40Lz',DerivedTable2.AmtmorU) OR
     'Amt-wBNq' <> 'Amt-3oHK')
  LEFT JOIN testdataset.SourceB AS SourceB ON
    (SourceA.FieldE <> SourceA.FieldD OR 'Code-CBzn' LIKE 'Code-UewD' AND
     DerivedTable2.Amtp4ee NOT LIKE DerivedTable2.Address5RWP AND
     STARTS_WITH('Amt-Nhct',SourceB.FieldA))
  INNER JOIN testdataset.SourceC AS SourceC ON
    (STARTS_WITH(DerivedTable2.Account61kv,'Country-j4f6') OR
     SourceC.FieldD = SourceB.FieldI AND
     SourceA.FieldH LIKE 'CCY-c6d6' OR ENDS_WITH(SourceC.FieldI,'Amt-RWwc'))
  LEFT JOIN testdataset.SourceD AS SourceD ON
    ('Amt-RXSq' != SourceA.FieldF AND
     DerivedTable2.Address7oYI NOT LIKE DerivedTable2.Address7oYI OR
     ENDS_WITH('Account-RQ7j',DerivedTable2.Account61kv) AND
     STARTS_WITH('Fee-xPaH','CCY-1X8A'))
  LEFT JOIN testdataset.SourceE AS SourceE ON
    ('CCY-QGQJ' NOT LIKE SourceB.FieldF OR
     ENDS_WITH(SourceA.FieldH,'Code-awSM') OR
     SourceC.FieldA NOT LIKE 'Country-l3xE' OR
     ENDS_WITH(DerivedTable2.Account61kv,'CCY-a60x'))
  LEFT JOIN testdataset.DerivedTable1 AS DerivedTable1 ON
    (ENDS_WITH(DerivedTable2.AddresscM17,SourceA.FieldH) AND
     ENDS_WITH(DerivedTable2.Amtp4ee,'Address-g6ze') AND
     STARTS_WITH(DerivedTable2.CodeDmva,SourceC.FieldD) AND
     'Account-smt6' LIKE 'Country-4h0B')
  INNER JOIN testdataset.DerivedTable3 AS DerivedTable3 ON
    ('Country-Ym4W' <> 'Account-fLsL' AND
     ENDS_WITH(SourceE.FieldI,SourceC.FieldC) AND
     'Amt-DxPk' NOT LIKE SourceD.FieldG OR
     DerivedTable1.AccountmNQo <> SourceE.FieldF)
  INNER JOIN testdataset.DerivedTable4 AS DerivedTable4 ON
    (DerivedTable2.AccounttVM1 LIKE DerivedTable3.CustomereBLI AND
     STARTS_WITH('Country-NCSk',SourceA.FieldJ) AND
     'Customer-Ealc' <> DerivedTable1.CustomerWpfX AND
     SourceD.FieldD <> 'Fee-nFX7')
  LEFT JOIN Table21 AS Table21 ON
    (ENDS_WITH(SourceB.FieldH,'Country-eM0P') AND
     ENDS_WITH(SourceD.FieldB,DerivedTable3.CountrygPvi) AND
     ENDS_WITH('Fee-gVaU','CCY-SLrm') OR
     SourceE.FieldA NOT LIKE 'Customer-NaCn')
  LEFT JOIN Table22 AS Table22 ON
    (Table22.CountryKVrI LIKE 'Fee-1Z3Y' AND
     'Code-alhy' LIKE Table21.AmtPgyZ AND
     Table21.Amt7FNh != Table21.CCYOxTW OR
     STARTS_WITH('Country-Y9Pc',SourceE.FieldJ))
  RIGHT JOIN Table23 AS Table23 ON
    ('Amt-7SPa' = Table21.CCY8AOe AND 'Customer-Fi78' NOT LIKE 'Code-PW2U' OR
     SourceD.FieldA NOT LIKE Table22.AddressOyCN OR 'Fee-ZJIi' != 'CCY-L0Tj')
  INNER JOIN Table24 AS Table24 ON
    (ENDS_WITH(DerivedTable1.AmtzEYq,DerivedTable4.FeesrBh) OR
     ENDS_WITH(Table23.CCYt432,DerivedTable4.CodeXHEv) OR
     DerivedTable3.Codeaqvh <> Table21.CodeDuQT AND
     DerivedTable1.AccountKwSP NOT LIKE DerivedTable4.AddressB6KJ)
  INNER JOIN Table25 AS Table25 ON
    (STARTS_WITH('Customer-b9ih','Amt-4FKP') OR
     ENDS_WITH('Code-BnLQ',DerivedTable2.AccountrJZf) AND
     ENDS_WITH(SourceD.FieldG,DerivedTable4.AccounteutQ) AND
     'Address-hU5x' NOT LIKE Table25.Customerp0cg)
WHERE
  ENDS_WITH('CCY-ktcO','Customer-uxMr') AND
  STARTS_WITH('Amt-T5yl',Table21.FeeMDz0) AND
  ENDS_WITH('Country-Xrbz',DerivedTable1.AccountmNQo) AND
  ENDS_WITH('Customer-zuvF',Table25.Codeq40v) AND
  DerivedTable1.CustomerWrlz LIKE SourceB.FieldG OR
  DerivedTable2.CountrywaWi NOT LIKE 'Address-PFX1' AND
  ENDS_WITH(Table25.Fee7dB5,SourceE.FieldE) AND
  STARTS_WITH('Code-w5By','Customer-QolR')