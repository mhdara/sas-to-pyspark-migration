/*----------------------------------------------------------------------------------------------
   05_card_fraud_summary.sas - card spending per merchant type, and fraud counts.

   What it does: PROC MEANS computes, per merchant category, the number of transactions, the total
     and the average amount (NWAY: category rows only, no grand total). PROC FREQ counts the
     transactions per merchant category and fraud flag, with percentages.
   Reads:  stg.card_transactions
   Writes: out.card_summary (one row per category), out.fraud_freq (category x fraud flag)
   Trap:   25 refunds (negative amounts) and 5 zero amounts are included: they lower the totals and
           the averages, and the converted code must count them the same way.
----------------------------------------------------------------------------------------------*/
libname stg "&root/stg";
libname out "&root/out";

proc means data=stg.card_transactions noprint nway;
  class merchant_category;
  var amount;
  output out=out.card_summary(drop=_type_ _freq_) n=n_txn sum=total_amount mean=avg_amount;
run;

proc freq data=stg.card_transactions noprint;
  tables merchant_category*fraud_flag / out=out.fraud_freq;
run;
