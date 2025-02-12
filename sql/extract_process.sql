DECLARE
  curr_date DATE DEFAULT CURRENT_DATE();
DECLARE
  curr_timestmp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
CREATE OR REPLACE TABLE
  `sbox-ujjal-ci-cd-capabilities.Target_test.stg_IND_currency_rates` AS (
  SELECT
    DATE,
    cr.Australian_Dollar *cr.indian_rupee AS US_INR,
    cr.canadian_dollar*cr.indian_rupee AS CAN_INR,
    cr.chinese_yuan*cr.indian_rupee AS CHI_INR,
    cr.qatar_riyal*cr.indian_rupee AS QATR_INR,
    cr.indian_rupee as IND_INR,
    CASE
      WHEN
      cr.indian_rupee IS NULL 
      THEN 'DQIssue'
      ELSE 'OK'
  END AS row_status_code,
    CONCAT ( (
        CASE
          WHEN cr.indian_rupee IS NULL THEN '[Indian_rupee is null in currency_rates_table]'
          ELSE ''
      END
        ) ) AS notes_txt,
    curr_timestmp AS create_tmstmp,
  FROM 
  (
    SELECT
    date,
    IFNULL(Australian_Dollar,0) AS Australian_Dollar,
    IFNULL(Canadian_Dollar,0) AS canadian_dollar,
    IFNULL(Chinese_Yuan,0) AS chinese_yuan,
    IFNULL(Qatar_Riyal,0) AS qatar_riyal,
    Indian_Rupee
    FROM
      `sbox-ujjal-ci-cd-capabilities.source_test.currency_rates` 
  ) cr);