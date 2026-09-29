/*----------------------------------------------------------------------------------------------
   08_period_driver.sas - one payment summary per active reporting month, driven by a table.

   What it does:
     1. reads the months with active_flag = 1 from the reporting-periods table into one list
        (202509 202510 202511 202512);
     2. %period_driver loops over that list and calls %run_period for each month;
     3. %run_period keeps that month's payments and calls %summarize (program 06): total and average
        payment for on-time vs late payments, saved as out.period_summary_<month>.
     MPRINT is written to &root/logs/08_period_driver_mprint.sas: the code the macros generated,
     with the real table names.
   Reads:  out.delinquency (program 04), ctrl.reporting_periods, program 06 (%include)
   Writes: out.period_summary_202509 ... _202512 (one table per active month)
   Trap:   three levels of nested macros, a loop driven by data, and table names built at run time
           (out.period_summary_&pid): switching one more month to active in the table creates one
           more output table, without changing the code.
----------------------------------------------------------------------------------------------*/
libname ctrl "&root/ctrl";
libname out  "&root/out";

filename mprint "&root/logs/08_period_driver_mprint.sas";
options mprint mfile;

%include "&root/programs/06_macro_library.sas";

proc sql noprint;
  select period_id into :period_list separated by ' '
  from ctrl.reporting_periods
  where active_flag = 1
  order by period_id;
quit;

%macro run_period(pid);
  data work.pay_&pid;
    set out.delinquency;
    where period_id = "&pid";
  run;
  %summarize(ds=work.pay_&pid, class=delinquent_flag, var=payment_amount,
             out=out.period_summary_&pid);
%mend run_period;

%macro period_driver;
  %local i pid;
  %do i = 1 %to %sysfunc(countw(&period_list, %str( )));
    %let pid = %scan(&period_list, &i, %str( ));
    %run_period(&pid);
  %end;
%mend period_driver;

%period_driver;

options nomfile;
