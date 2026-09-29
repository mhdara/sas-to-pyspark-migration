/*----------------------------------------------------------------------------------------------
   09_payment_model.sas - linear regression of the monthly payment.

   What it does: PROC REG fits monthly_payment = intercept + b1*principal + b2*interest_rate
     + b3*term_months on the enriched loans and saves the coefficients.
   Reads:  out.loan_enriched (program 02)
   Writes: out.reg_est (one row: intercept, the three coefficients, root mean squared error)
   Trap:   the converted code must fit the same model (ordinary least squares with an intercept) on
           the same 799 rows to give the same coefficients.
----------------------------------------------------------------------------------------------*/
libname out "&root/out";

proc reg data=out.loan_enriched outest=out.reg_est noprint;
  model monthly_payment = principal interest_rate term_months;
run;
quit;
