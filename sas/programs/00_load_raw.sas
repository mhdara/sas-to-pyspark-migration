/*----------------------------------------------------------------------------------------------
   00_load_raw.sas - load the generated CSV files into SAS tables (.sas7bdat).

   What it does: reads each CSV in &root/raw with a DATA step (INFILE + INPUT) and saves it as a
     permanent SAS table. Every column gets an explicit type and length (LENGTH), and dates are read
     with the yymmdd10. informat. No PROC IMPORT: it would guess types and lengths.
   Reads:  &root/raw/*.csv (7 files, UTF-8)
   Writes: ctrl.client_segments, ctrl.reporting_periods, ctrl.macro_parameters,
           stg.customers, stg.loans, stg.loan_payments, stg.card_transactions
   Checks: prints the longest customer name in bytes (expect 30) and the missing incomes (expect 15).
   Trap:   SAS lengths count BYTES and SAS cuts longer text silently. The customer_name length is
           set in one place (&name_len): v1 = $12 cuts names, v2 = $60 keeps them all.
           python/sas_load.py runs both versions; &root is set by python/sas_session.py.
----------------------------------------------------------------------------------------------*/
libname stg  "&root/stg";
libname ctrl "&root/ctrl";

%let name_len = $60;   /* v1 = $12 (too short, cuts names) | v2 = $60 (correct) */

filename f_seg "&root/raw/ctrl_client_segments.csv" encoding="utf-8";
data ctrl.client_segments;
  infile f_seg dsd firstobs=2 truncover;
  length segment_code $10 segment_name $60 risk_band $10 min_income 8 max_income 8;
  input segment_code $ segment_name $ risk_band $ min_income max_income;
run;

filename f_per "&root/raw/ctrl_reporting_periods.csv" encoding="utf-8";
data ctrl.reporting_periods;
  infile f_per dsd firstobs=2 truncover;
  length period_id $6 period_start 8 period_label $20 active_flag 8;
  informat period_start yymmdd10.;
  format period_start yymmdd10.;
  input period_id $ period_start period_label $ active_flag;
run;

filename f_par "&root/raw/ctrl_macro_parameters.csv" encoding="utf-8";
data ctrl.macro_parameters;
  infile f_par dsd firstobs=2 truncover;
  length param_name $32 param_value $32;
  input param_name $ param_value $;
run;

filename f_cus "&root/raw/customers.csv" encoding="utf-8";
data stg.customers(label="customers loaded by 00_load_raw, customer_name length &name_len");
  infile f_cus dsd firstobs=2 truncover;
  length customer_id 8 customer_name &name_len city $40 province $2
         segment_code $10 annual_income 8 customer_since 8;
  informat customer_since yymmdd10.;
  format customer_since yymmdd10.;
  input customer_id customer_name $ city $ province $ segment_code $
        annual_income customer_since;
run;

filename f_loa "&root/raw/loans.csv" encoding="utf-8";
data stg.loans;
  infile f_loa dsd firstobs=2 truncover;
  length loan_id 8 customer_id 8 loan_type $12 origination_date 8 principal 8
         interest_rate 8 term_months 8 status $10 monthly_payment 8;
  informat origination_date yymmdd10.;
  format origination_date yymmdd10.;
  input loan_id customer_id loan_type $ origination_date principal
        interest_rate term_months status $ monthly_payment;
run;

filename f_pay "&root/raw/loan_payments.csv" encoding="utf-8";
data stg.loan_payments;
  infile f_pay dsd firstobs=2 truncover;
  length loan_id 8 payment_date 8 payment_amount 8 principal_paid 8
         interest_paid 8 days_past_due 8;
  informat payment_date yymmdd10.;
  format payment_date yymmdd10.;
  input loan_id payment_date payment_amount principal_paid interest_paid days_past_due;
run;

filename f_crd "&root/raw/card_transactions.csv" encoding="utf-8";
data stg.card_transactions;
  infile f_crd dsd firstobs=2 truncover;
  length transaction_id 8 customer_id 8 transaction_date 8 merchant_category $20
         amount 8 fraud_flag 8;
  informat transaction_date yymmdd10.;
  format transaction_date yymmdd10.;
  input transaction_id customer_id transaction_date merchant_category $ amount fraud_flag;
run;

/* quick check: longest customer name in bytes (CSV value: 30) and missing incomes (CSV: 15) */
proc sql;
  select max(length(customer_name)) as max_name_bytes,
         nmiss(annual_income)       as missing_income
  from stg.customers;
quit;
