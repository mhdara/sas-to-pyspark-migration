/*----------------------------------------------------------------------------------------------
   06_macro_library.sas - shared macros used by other programs; creates no data by itself.

   What it does: defines two macros.
     %summarize(ds=, class=, var=, out=)      count, total and average of &var per &class (PROC MEANS)
     %flag_high(ds=, var=, threshold=, out=)  adds high_flag = 1 when &var > &threshold
   Reads / writes: nothing on its own; programs 07 and 08 %include this file and call the macros.
   Trap:   the code runs only through the programs that include it, so it can only be tested
           through them; a converted version becomes a Python module imported by those programs.
----------------------------------------------------------------------------------------------*/

%macro summarize(ds=, class=, var=, out=);
  proc means data=&ds noprint nway;
    class &class;
    var &var;
    output out=&out(drop=_type_ _freq_) n=n sum=total mean=avg;
  run;
%mend summarize;

%macro flag_high(ds=, var=, threshold=, out=);
  data &out;
    set &ds;
    high_flag = (&var > &threshold);   /* missing values give 0 in SAS */
  run;
%mend flag_high;
