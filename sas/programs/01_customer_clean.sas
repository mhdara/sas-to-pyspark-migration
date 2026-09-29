/*----------------------------------------------------------------------------------------------
   01_customer_clean.sas - clean the customer table and put each customer in an income band.

   What it does: copies stg.customers row by row; removes leading/trailing blanks from the name,
     upper-cases the province ("qc" -> "QC"), flags a missing income, and assigns an income band:
     LOW (< 20,000), MID (< 80,000) or HIGH.
   Reads:  stg.customers
   Writes: out.customers_clean (500 rows)
   Trap:   in SAS a missing number is smaller than any number, so "annual_income < 20000" is TRUE
           for the 15 customers without income: they get LOW. A literal translation to Python or
           Spark (where a comparison with null is false/null) puts them in HIGH instead.
----------------------------------------------------------------------------------------------*/
libname stg "&root/stg";
libname out "&root/out";

data out.customers_clean;
  set stg.customers;
  length income_band $4;
  customer_name = strip(customer_name);
  province      = upcase(province);
  income_missing = missing(annual_income);
  /* TRAP: in SAS a missing income is smaller than any number -> falls into LOW */
  if annual_income < 20000 then income_band = 'LOW';
  else if annual_income < 80000 then income_band = 'MID';
  else income_band = 'HIGH';
run;
