MERGE
  `sbox-ujjal-ci-cd-capabilities.Target_test.IND_currency_rates` tgt
USING
  `sbox-ujjal-ci-cd-capabilities.Target_test.stg_IND_currency_rates` src
ON
  src.date = tgt.date
  AND (tgt.us_inr <> src.us_inr
       OR tgt.can_inr <> src.can_inr
       OR tgt.chi_inr <> src.chi_inr
       OR tgt.qatr_inr <> src.qatr_inr
       OR tgt.ind_inr <> src.ind_inr)
WHEN MATCHED THEN
  UPDATE SET 
    tgt.us_inr = src.us_inr,
    tgt.can_inr = src.can_inr,
    tgt.chi_inr = src.chi_inr,
    tgt.qatr_inr = src.qatr_inr,
    tgt.ind_inr = src.ind_inr,
    tgt.row_status_code = src.row_status_code,
    tgt.notes_txt = src.notes_txt,
    tgt.create_tmstmp = src.create_tmstmp
WHEN NOT MATCHED THEN
  INSERT (
    us_inr,
    can_inr,
    chi_inr,
    qatr_inr,
    ind_inr,
    row_status_code,
    notes_txt,
    create_tmstmp
  )
  VALUES (
    src.us_inr,
    src.can_inr,
    src.chi_inr,
    src.qatr_inr,
    src.ind_inr, 
    src.row_status_code,
    src.notes_txt,
    src.create_tmstmp
  );
