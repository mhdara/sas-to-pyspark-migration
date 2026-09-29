/*----------------------------------------------------------------------------------------------
   04_payment_delinquency.sas - flag late payments and summarise each loan's repayment history.

   What it does:
     1. reads the late-payment threshold (DPD_THRESHOLD = "30", stored as text) from the settings
        table into the macro variable &dpd_threshold;
     2. sorts the payments by loan and date and removes the duplicate (NODUPKEY keeps the first);
     3. walks through each loan's payments in date order (BY loan_id): running total paid (sum
        statement), highest days past due so far (RETAIN), late flag (days_past_due > threshold),
        and the payment month as YYYYMM;
     4. writes every payment row, and one summary row per loan (its last row).
   Reads:  stg.loan_payments, ctrl.macro_parameters
   Writes: out.delinquency (one row per loan and month, duplicate removed),
           out.loan_status (one row per loan: cum_paid, max_dpd)
   Trap:   the threshold comes from a table, not the code; NODUPKEY keeps the FIRST copy while
           Spark's dropDuplicates keeps any copy; FIRST./LAST. and RETAIN depend on row order.
----------------------------------------------------------------------------------------------*/
libname stg  "&root/stg";
libname ctrl "&root/ctrl";
libname out  "&root/out";

proc sql noprint;
  select param_value into :dpd_threshold trimmed
  from ctrl.macro_parameters
  where param_name = 'DPD_THRESHOLD';
quit;

proc sort data=stg.loan_payments out=work.pay_sorted nodupkey;
  by loan_id payment_date;
run;

data out.delinquency out.loan_status(keep=loan_id cum_paid max_dpd);
  set work.pay_sorted;
  by loan_id;
  length period_id $6;
  retain max_dpd;
  if first.loan_id then do;
    cum_paid = 0;
    max_dpd  = 0;
  end;
  cum_paid + payment_amount;                 /* sum statement: retains, treats missing as 0 */
  max_dpd = max(max_dpd, days_past_due);
  delinquent_flag = (days_past_due > &dpd_threshold);
  period_id = put(payment_date, yymmn6.);
  output out.delinquency;
  if last.loan_id then output out.loan_status;
run;
