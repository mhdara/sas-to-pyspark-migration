/*----------------------------------------------------------------------------------------------
   11_executive_report.sas - one-page summary per segment, plus the repayment forecast.

   What it does: reads the late-payment threshold (DPD_THRESHOLD) from the settings table, as in
     program 04; per segment, counts the loans and the loans whose worst delay is above the
     threshold, and totals the principal; separately keeps only the future months of the forecast.
   Reads:  out.loan_enriched (02), out.loan_status (04), out.fc and out.monthly_repay (10),
           ctrl.macro_parameters
   Writes: out.exec_report (one row per segment), out.exec_forecast (12 future months)
   Trap:   it depends on three earlier programs, so an error upstream shows up here; the threshold
           is read from the table (the guide hard-coded 30).
----------------------------------------------------------------------------------------------*/
libname ctrl "&root/ctrl";
libname out  "&root/out";

proc sql noprint;
  select param_value into :dpd_threshold trimmed
  from ctrl.macro_parameters
  where param_name = 'DPD_THRESHOLD';
quit;

proc sql;
  create table out.exec_report as
  select e.segment_name,
         count(distinct e.loan_id)            as n_loans,
         sum(s.max_dpd > &dpd_threshold)      as n_delinquent_loans,
         sum(e.principal)                     as total_principal
  from out.loan_enriched as e
       left join out.loan_status as s on e.loan_id = s.loan_id
  group by e.segment_name;

  create table out.exec_forecast as
  select month, total_payment as forecast_payment
  from out.fc
  where _type_ = 'FORECAST'
    and month > (select max(month) from out.monthly_repay);
quit;
