/* check_environment.sas - one-time check of the SAS OnDemand workspace (plan Phase 2).        */
/* Not part of the migrated estate: it lives in sas/setup/, so the analyzer does not read it. */
/* Needs &root: set by python/sas_check.py (SASPy), or by the SAS Studio autoexec             */
/* (%let root=/home/<user id>/case;) when pasted into SAS Studio.                            */
/* Expected in the LOG: ROOT=..., ENCODING=UTF-8, ETS_LICENSED=1, CHECK OK lines, no ERROR.   */

/* 1. The folder shortcut from the autoexec */
%put &=root;

/* 2. Session encoding: must be UTF-8, or accented names are garbled on load */
proc options option=encoding; run;
%put ENCODING=%sysfunc(getoption(encoding));

/* 3. Every project folder exists */
%macro check_folder(name);
  %if %sysfunc(fileexist(&root/&name)) %then %put CHECK OK: folder &name exists;
  %else %put ERROR: folder &root/&name is missing - create it in Server Files and Folders;
%mend check_folder;
%check_folder(raw)
%check_folder(stg)
%check_folder(ctrl)
%check_folder(out)
%check_folder(logs)
%check_folder(programs)

/* 4. SAS can write a permanent table in the case folder, then clean up */
libname out "&root/out";
data out.hello; x = 1; run;
%put CHECK OK: out.hello written = %sysfunc(exist(out.hello));
proc datasets library=out nolist; delete hello; quit;

/* 5. PROC FORECAST belongs to SAS/ETS: check the licence, then run it exactly as program 10 will */
%put ETS_LICENSED=%sysprod(ets);

data work.t;
  do i = 1 to 24;
    month = intnx('month', '01jan2020'd, i - 1);
    y = 100 + 2 * i;
    output;
  end;
  format month yymmdd10.;
run;

proc forecast data=work.t interval=month method=expo trend=2 weight=0.2 nstart=12
              lead=3 out=work.f outfull outest=work.f_est;
  id month;
  var y;
run;

/* A straight line must be forecast as a straight line: expect 150, 152, 154 */
proc print data=work.f;
  where _type_ = 'FORECAST' and month > '01dec2021'd;
run;
