/*----------------------------------------------------------------------------------------------
   07_portfolio_report.sas - loan portfolio totals per customer segment, with a large-portfolio flag.

   What it does: includes the macro library (program 06); reads the HIGH_PRINCIPAL threshold
     ("5000000", text) from the settings table; calls %summarize to total the loan principal per
     segment, then %flag_high to flag segments whose total is above the threshold.
   Reads:  out.loan_enriched (program 02), ctrl.macro_parameters, program 06 (%include)
   Writes: out.portfolio_by_segment, out.portfolio_report (one row per segment)
   Trap:   the logic lives in another file (%include) and inside macros, and the threshold comes
           from a table: a converter must follow all three to reproduce the result.
----------------------------------------------------------------------------------------------*/
libname ctrl "&root/ctrl";
libname out  "&root/out";

%include "&root/programs/06_macro_library.sas";

proc sql noprint;
  select param_value into :high_principal trimmed
  from ctrl.macro_parameters
  where param_name = 'HIGH_PRINCIPAL';
quit;

%summarize(ds=out.loan_enriched, class=segment_name, var=principal, out=out.portfolio_by_segment);
%flag_high(ds=out.portfolio_by_segment, var=total, threshold=&high_principal, out=out.portfolio_report);
