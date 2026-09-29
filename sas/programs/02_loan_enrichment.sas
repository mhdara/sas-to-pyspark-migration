/*----------------------------------------------------------------------------------------------
   02_loan_enrichment.sas - add customer and segment details to every loan.

   What it does: one PROC SQL query joins the loans to the cleaned customers (inner join), then to
     the segment lookup table (left join), adding the customer name, province, income band,
     segment name and risk level to each loan.
   Reads:  stg.loans, out.customers_clean (program 01), ctrl.client_segments
   Writes: out.loan_enriched (799 rows)
   Trap:   one loan belongs to customer 999, who does not exist. The inner join drops it silently:
           800 loans in, 799 out. The converted code must drop exactly the same row.
----------------------------------------------------------------------------------------------*/
libname stg  "&root/stg";
libname ctrl "&root/ctrl";
libname out  "&root/out";

proc sql;
  create table out.loan_enriched as
  select l.*, c.customer_name, c.province, c.income_band,
         s.segment_name, s.risk_band
  from stg.loans as l
       inner join out.customers_clean as c on l.customer_id = c.customer_id
       left join ctrl.client_segments as s on c.segment_code = s.segment_code;
quit;
