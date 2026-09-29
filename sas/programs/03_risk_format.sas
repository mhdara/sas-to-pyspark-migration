/*----------------------------------------------------------------------------------------------
   03_risk_format.sas - label each customer's income with a user-defined format.

   What it does: PROC FORMAT defines "incband", a lookup from income ranges to labels
     (LOW < 20,000, MID < 80,000, HIGH >= 80,000, anything else UNKNOWN); a DATA step applies it
     with PUT() to every customer.
   Reads:  out.customers_clean (program 01)
   Writes: out.customer_risk (500 rows)
   Trap:   Python and Spark have no PROC FORMAT: the ranges must be rebuilt as conditions, with the
           same open/closed bounds. OTHER also catches missing values, so here a missing income is
           UNKNOWN, while program 01 puts the same customers in LOW.
----------------------------------------------------------------------------------------------*/
libname out "&root/out";

proc format;
  value incband
    low -< 20000   = 'LOW'
    20000 -< 80000 = 'MID'
    80000 - high   = 'HIGH'
    other          = 'UNKNOWN';   /* OTHER also catches missing values */
run;

data out.customer_risk;
  set out.customers_clean;
  length income_band_fmt $7;
  income_band_fmt = put(annual_income, incband.);
run;
