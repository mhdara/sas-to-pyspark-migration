/*----------------------------------------------------------------------------------------------
   10_repayment_forecast.sas - total repayments per month, and a 12-month forecast.

   What it does: PROC SQL sums the payments per calendar month (48 months, 2022-2025); PROC FORECAST
     then forecasts 12 more months with double exponential smoothing (METHOD=EXPO TREND=2), with
     every option written out (WEIGHT=0.2, NSTART=12) so Python can reproduce it exactly.
   Reads:  stg.loan_payments
   Writes: out.monthly_repay (48 rows), out.fc (actuals, fitted values and forecasts, OUTFULL),
           out.fc_est (the method's final internal values)
   Note:   reads stg.loan_payments ON PURPOSE, including the planted duplicate row: one month is
           slightly high, and Python must reproduce that too (see docs/notes.md).
   Trap:   Python has no PROC FORECAST: the method is rebuilt by hand (Phase 11). SAS logs a
           warning that the procedure is obsolete.
----------------------------------------------------------------------------------------------*/
libname stg "&root/stg";
libname out "&root/out";

proc sql;
  create table out.monthly_repay as
  select intnx('month', payment_date, 0, 'b') as month format=yymmdd10.,
         sum(payment_amount) as total_payment
  from stg.loan_payments
  group by calculated month
  order by calculated month;
quit;

proc forecast data=out.monthly_repay interval=month
              method=expo trend=2 weight=0.2 nstart=12 lead=12
              out=out.fc outfull outest=out.fc_est;
  id month;
  var total_payment;
run;
