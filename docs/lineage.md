# Lineage: how the SAS programs, tables and files are connected

This page answers two questions about the SAS code in this project:

- **Where does a number come from?** For any result table, which programs produced it, which tables they
  read, and which source files those tables were loaded from.
- **What depends on what?** Which programs must be migrated before others, and which programs share code.

The diagrams are not drawn by hand. `python/analyzer.py` reads the SAS code and records every connection
it finds (for example "program 04 reads table `stg.loan_payments`") in `outputs/lineage_edges.csv`.
`python/lineage_diagram.py` then turns that file into this page. Because the page is regenerated from the
code, it stays correct when a program changes: rerunning `python python/run_all.py` redraws it.

## Two kinds of lineage

In SAS you need to follow two kinds of connections together:

| Kind | The question it answers | Connections recorded |
|---|---|---|
| **Data lineage** | Where did this table's data come from? | a source file is loaded into a table; a program reads a table; a program writes a table |
| **Code lineage** | Which code produced it, including code that lives in another program? | a program pulls in another program with `%include`; a program defines a macro; a program or a macro calls a macro |

Code lineage matters in SAS because two programs can depend on each other without any table between them.
Program 08 writes its summary tables by calling a macro (a reusable block of SAS code) called `%summarize`.
That macro is not written in program 08: it lives in program 06, and program 08 copies it in with
`%include`. Looking only at tables, you would never see that a change to program 06 changes program 08's
results.

In total the analyzer found 53 items (27 tables, 12 programs, 7 files, 4 macros, 3 libraries) and 97 connections between them.

**How sure is each connection?** Every connection has a confidence level, because not every table name
can be read directly from the code:

| Confidence | Meaning | Example |
|---|---|---|
| HIGH | The table name is written literally in the code. | `set stg.loan_payments;` |
| MEDIUM | The name was worked out indirectly: from the arguments of a macro call, or from SAS's MPRINT log, a file in which SAS records the code it actually ran after expanding the macros. | `%summarize(..., out=out.portfolio_by_segment)`; the four `out.period_summary_2025MM` tables in the MPRINT log of program 08 |
| LOW | The name contains a macro variable that is only known when the program runs. | `out.period_summary_&pid` |

## 1. Tracing a result back to its sources

### 1a. Automatically, for a whole table

The script `python/lineage_trace.py` follows the connections backwards from any table. Here is the result
for the executive report (`out.exec_report`), the last table in the chain. Read it from the top: the
report was written by program 11; program 11 read four result tables and one settings table; each of those was
written by an earlier program; and so on until the source CSV files.

Each line shows how the item is connected to the line above it, then its kind and the confidence level.
"(see above)" means the item was already traced higher up, so its sources are not repeated.

```text
$ python python/lineage_trace.py out.exec_report
out.exec_report [table]
└─ written by 11_executive_report [program, HIGH]
   ├─ reads ctrl.macro_parameters [table, HIGH]
   │  ├─ loaded from ctrl_macro_parameters.csv [file, HIGH]
   │  └─ written by 00_load_raw [program, HIGH]
   ├─ reads out.fc [table, HIGH]
   │  └─ written by 10_repayment_forecast [program, HIGH]
   │     └─ reads stg.loan_payments [table, HIGH]
   │        ├─ loaded from loan_payments.csv [file, HIGH]
   │        └─ written by 00_load_raw [program, HIGH] (see above)
   ├─ reads out.loan_enriched [table, HIGH]
   │  └─ written by 02_loan_enrichment [program, HIGH]
   │     ├─ reads ctrl.client_segments [table, HIGH]
   │     │  ├─ loaded from ctrl_client_segments.csv [file, HIGH]
   │     │  └─ written by 00_load_raw [program, HIGH] (see above)
   │     ├─ reads out.customers_clean [table, HIGH]
   │     │  └─ written by 01_customer_clean [program, HIGH]
   │     │     └─ reads stg.customers [table, HIGH]
   │     │        ├─ loaded from customers.csv [file, HIGH]
   │     │        └─ written by 00_load_raw [program, HIGH] (see above)
   │     └─ reads stg.loans [table, HIGH]
   │        ├─ loaded from loans.csv [file, HIGH]
   │        └─ written by 00_load_raw [program, HIGH] (see above)
   ├─ reads out.loan_status [table, HIGH]
   │  └─ written by 04_payment_delinquency [program, HIGH]
   │     ├─ reads ctrl.macro_parameters [table, HIGH] (see above)
   │     └─ reads stg.loan_payments [table, HIGH] (see above)
   └─ reads out.monthly_repay [table, HIGH]
      └─ written by 10_repayment_forecast [program, HIGH] (see above)
```

The same trace as a diagram. The arrows point in the direction the data flows, from the source files on
the left to the report on the right; to trace back, read from right to left.

```mermaid
flowchart LR
  n_00_load_raw(["00_load_raw"])
  n_01_customer_clean(["01_customer_clean"])
  n_02_loan_enrichment(["02_loan_enrichment"])
  n_04_payment_delinquency(["04_payment_delinquency"])
  n_10_repayment_forecast(["10_repayment_forecast"])
  n_11_executive_report(["11_executive_report"])
  n_ctrl_client_segments["ctrl.client_segments"]
  n_ctrl_macro_parameters["ctrl.macro_parameters"]
  n_ctrl_client_segments_csv[("ctrl_client_segments.csv")]
  n_ctrl_macro_parameters_csv[("ctrl_macro_parameters.csv")]
  n_customers_csv[("customers.csv")]
  n_loan_payments_csv[("loan_payments.csv")]
  n_loans_csv[("loans.csv")]
  n_out_customers_clean["out.customers_clean"]
  n_out_exec_report["out.exec_report"]
  n_out_fc["out.fc"]
  n_out_loan_enriched["out.loan_enriched"]
  n_out_loan_status["out.loan_status"]
  n_out_monthly_repay["out.monthly_repay"]
  n_stg_customers["stg.customers"]
  n_stg_loan_payments["stg.loan_payments"]
  n_stg_loans["stg.loans"]
  n_00_load_raw --> n_ctrl_client_segments
  n_00_load_raw --> n_ctrl_macro_parameters
  n_00_load_raw --> n_stg_customers
  n_00_load_raw --> n_stg_loan_payments
  n_00_load_raw --> n_stg_loans
  n_01_customer_clean --> n_out_customers_clean
  n_02_loan_enrichment --> n_out_loan_enriched
  n_04_payment_delinquency --> n_out_loan_status
  n_10_repayment_forecast --> n_out_fc
  n_10_repayment_forecast --> n_out_monthly_repay
  n_11_executive_report --> n_out_exec_report
  n_ctrl_client_segments --> n_02_loan_enrichment
  n_ctrl_macro_parameters --> n_04_payment_delinquency
  n_ctrl_macro_parameters --> n_11_executive_report
  n_ctrl_client_segments_csv -->|"loads"| n_ctrl_client_segments
  n_ctrl_macro_parameters_csv -->|"loads"| n_ctrl_macro_parameters
  n_customers_csv -->|"loads"| n_stg_customers
  n_loan_payments_csv -->|"loads"| n_stg_loan_payments
  n_loans_csv -->|"loads"| n_stg_loans
  n_out_customers_clean --> n_02_loan_enrichment
  n_out_fc --> n_11_executive_report
  n_out_loan_enriched --> n_11_executive_report
  n_out_loan_status --> n_11_executive_report
  n_out_monthly_repay --> n_11_executive_report
  n_stg_customers --> n_01_customer_clean
  n_stg_loan_payments --> n_04_payment_delinquency
  n_stg_loan_payments --> n_10_repayment_forecast
  n_stg_loans --> n_02_loan_enrichment
  classDef file fill:#fefce8,stroke:#a16207,color:#422006
  classDef program fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
  classDef macro fill:#f3e8ff,stroke:#7e22ce,color:#3b0764
  classDef stg fill:#fdf2f8,stroke:#be185d,color:#500724
  classDef ctrl fill:#ecfdf5,stroke:#047857,color:#022c22
  classDef out fill:#f3f4f6,stroke:#6b7280,color:#111827
  classDef dynamic fill:#fff7ed,stroke:#c2410c,stroke-dasharray:4 3,color:#7c2d12
  class n_ctrl_client_segments,n_ctrl_macro_parameters ctrl
  class n_ctrl_client_segments_csv,n_ctrl_macro_parameters_csv,n_customers_csv,n_loan_payments_csv,n_loans_csv file
  class n_out_customers_clean,n_out_exec_report,n_out_fc,n_out_loan_enriched,n_out_loan_status,n_out_monthly_repay out
  class n_00_load_raw,n_01_customer_clean,n_02_loan_enrichment,n_04_payment_delinquency,n_10_repayment_forecast,n_11_executive_report program
  class n_stg_customers,n_stg_loan_payments,n_stg_loans stg
```

The script also writes `outputs/lineage_paths.csv`, which lists, for every result table, everything
behind it and how many steps away it is. The Power BI dashboard uses that file: you pick a result table
and it lists all its sources.

### 1b. By hand, for a single number

The automatic trace works at table level. To follow one specific number, you also need to know which columns
each step used, which means reading the code at each step. Here is that trace for one value in the executive
report: **371**, the number of loans in the segment "Particulier Québec" that were ever more than 30 days late.
SAS computed 371, and the PySpark code converted by Claude also gives 371.

| Step | Value or column | Produced by (program and SAS code) | From |
|---|---|---|---|
| 1 | `out.exec_report.n_delinquent_loans` = 371 for "Particulier Québec" | `11_executive_report`: `sum(s.max_dpd > &dpd_threshold) as n_delinquent_loans ... group by e.segment_name` | `out.loan_status.max_dpd`, the macro variable `&dpd_threshold`, `out.loan_enriched.segment_name` |
| 2 | `&dpd_threshold` = `30` (text) | `11_executive_report`: `select param_value into :dpd_threshold from ctrl.macro_parameters where param_name = 'DPD_THRESHOLD'` | `ctrl.macro_parameters` |
| 3 | `ctrl.macro_parameters` | `00_load_raw`: `infile f_par` | `ctrl_macro_parameters.csv`, row `DPD_THRESHOLD,30` |
| 4 | `out.loan_status.max_dpd`: each loan's worst delay | `04_payment_delinquency`: `retain max_dpd; max_dpd = max(max_dpd, days_past_due); if last.loan_id then output out.loan_status` after `proc sort nodupkey` | `stg.loan_payments.days_past_due` |
| 5 | `stg.loan_payments.days_past_due` | `00_load_raw`: `infile f_pay` | `loan_payments.csv`, column `days_past_due` |
| 6 | `out.loan_enriched.segment_name` | `02_loan_enrichment`: `left join ctrl.client_segments as s on c.segment_code = s.segment_code` | `out.customers_clean.segment_code`, `ctrl.client_segments.segment_name` |
| 7 | `out.customers_clean.segment_code` | `01_customer_clean`: copied from `set stg.customers` | `stg.customers` ← `customers.csv` |

If 371 looked wrong, this table says where to look, in order: the `DPD_THRESHOLD` row in the settings table
(steps 2–3), the worst-delay logic and the duplicate removal in program 04 (step 4), the segment join in
program 02 (step 6), or the raw payment file (step 5). That is the practical use of lineage in a migration:
when a migrated number differs, it narrows down which piece of code to fix.

## 2. Data lineage: every file, table and program

The complete picture of how data flows, from the 7 source files on the left to the result tables on the
right. Colours show the kind of table: pink tables hold the loaded source data (`stg`), green tables hold
settings (`ctrl`), grey tables are results (`out`).

```mermaid
flowchart LR
  n_00_load_raw(["00_load_raw"])
  n_01_customer_clean(["01_customer_clean"])
  n_02_loan_enrichment(["02_loan_enrichment"])
  n_03_risk_format(["03_risk_format"])
  n_04_payment_delinquency(["04_payment_delinquency"])
  n_05_card_fraud_summary(["05_card_fraud_summary"])
  n_07_portfolio_report(["07_portfolio_report"])
  n_08_period_driver(["08_period_driver"])
  n_09_payment_model(["09_payment_model"])
  n_10_repayment_forecast(["10_repayment_forecast"])
  n_11_executive_report(["11_executive_report"])
  n_card_transactions_csv[("card_transactions.csv")]
  n_ctrl_client_segments["ctrl.client_segments"]
  n_ctrl_macro_parameters["ctrl.macro_parameters"]
  n_ctrl_reporting_periods["ctrl.reporting_periods"]
  n_ctrl_client_segments_csv[("ctrl_client_segments.csv")]
  n_ctrl_macro_parameters_csv[("ctrl_macro_parameters.csv")]
  n_ctrl_reporting_periods_csv[("ctrl_reporting_periods.csv")]
  n_customers_csv[("customers.csv")]
  n_loan_payments_csv[("loan_payments.csv")]
  n_loans_csv[("loans.csv")]
  n_out_card_summary["out.card_summary"]
  n_out_customer_risk["out.customer_risk"]
  n_out_customers_clean["out.customers_clean"]
  n_out_delinquency["out.delinquency"]
  n_out_exec_forecast["out.exec_forecast"]
  n_out_exec_report["out.exec_report"]
  n_out_fc["out.fc"]
  n_out_fc_est["out.fc_est"]
  n_out_fraud_freq["out.fraud_freq"]
  n_out_loan_enriched["out.loan_enriched"]
  n_out_loan_status["out.loan_status"]
  n_out_monthly_repay["out.monthly_repay"]
  n_out_period_summary__pid["out.period_summary_#amp;pid"]
  n_out_period_summary_202509["out.period_summary_202509"]
  n_out_period_summary_202510["out.period_summary_202510"]
  n_out_period_summary_202511["out.period_summary_202511"]
  n_out_period_summary_202512["out.period_summary_202512"]
  n_out_portfolio_by_segment["out.portfolio_by_segment"]
  n_out_portfolio_report["out.portfolio_report"]
  n_out_reg_est["out.reg_est"]
  n_stg_card_transactions["stg.card_transactions"]
  n_stg_customers["stg.customers"]
  n_stg_loan_payments["stg.loan_payments"]
  n_stg_loans["stg.loans"]
  n_00_load_raw --> n_ctrl_client_segments
  n_00_load_raw --> n_ctrl_macro_parameters
  n_00_load_raw --> n_ctrl_reporting_periods
  n_00_load_raw --> n_stg_card_transactions
  n_00_load_raw --> n_stg_customers
  n_00_load_raw --> n_stg_loan_payments
  n_00_load_raw --> n_stg_loans
  n_01_customer_clean --> n_out_customers_clean
  n_02_loan_enrichment --> n_out_loan_enriched
  n_03_risk_format --> n_out_customer_risk
  n_04_payment_delinquency --> n_out_delinquency
  n_04_payment_delinquency --> n_out_loan_status
  n_05_card_fraud_summary --> n_out_card_summary
  n_05_card_fraud_summary --> n_out_fraud_freq
  n_07_portfolio_report --> n_out_portfolio_by_segment
  n_07_portfolio_report --> n_out_portfolio_report
  n_08_period_driver -.-> n_out_period_summary__pid
  n_08_period_driver --> n_out_period_summary_202509
  n_08_period_driver --> n_out_period_summary_202510
  n_08_period_driver --> n_out_period_summary_202511
  n_08_period_driver --> n_out_period_summary_202512
  n_09_payment_model --> n_out_reg_est
  n_10_repayment_forecast --> n_out_fc
  n_10_repayment_forecast --> n_out_fc_est
  n_10_repayment_forecast --> n_out_monthly_repay
  n_11_executive_report --> n_out_exec_forecast
  n_11_executive_report --> n_out_exec_report
  n_card_transactions_csv -->|"loads"| n_stg_card_transactions
  n_ctrl_client_segments --> n_02_loan_enrichment
  n_ctrl_macro_parameters --> n_04_payment_delinquency
  n_ctrl_macro_parameters --> n_07_portfolio_report
  n_ctrl_macro_parameters --> n_11_executive_report
  n_ctrl_reporting_periods --> n_08_period_driver
  n_ctrl_client_segments_csv -->|"loads"| n_ctrl_client_segments
  n_ctrl_macro_parameters_csv -->|"loads"| n_ctrl_macro_parameters
  n_ctrl_reporting_periods_csv -->|"loads"| n_ctrl_reporting_periods
  n_customers_csv -->|"loads"| n_stg_customers
  n_loan_payments_csv -->|"loads"| n_stg_loan_payments
  n_loans_csv -->|"loads"| n_stg_loans
  n_out_customers_clean --> n_02_loan_enrichment
  n_out_customers_clean --> n_03_risk_format
  n_out_delinquency --> n_08_period_driver
  n_out_fc --> n_11_executive_report
  n_out_loan_enriched --> n_07_portfolio_report
  n_out_loan_enriched --> n_09_payment_model
  n_out_loan_enriched --> n_11_executive_report
  n_out_loan_status --> n_11_executive_report
  n_out_monthly_repay --> n_11_executive_report
  n_stg_card_transactions --> n_05_card_fraud_summary
  n_stg_customers --> n_01_customer_clean
  n_stg_loan_payments --> n_04_payment_delinquency
  n_stg_loan_payments --> n_10_repayment_forecast
  n_stg_loans --> n_02_loan_enrichment
  linkStyle 16 stroke-dasharray:5 4
  classDef file fill:#fefce8,stroke:#a16207,color:#422006
  classDef program fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
  classDef macro fill:#f3e8ff,stroke:#7e22ce,color:#3b0764
  classDef stg fill:#fdf2f8,stroke:#be185d,color:#500724
  classDef ctrl fill:#ecfdf5,stroke:#047857,color:#022c22
  classDef out fill:#f3f4f6,stroke:#6b7280,color:#111827
  classDef dynamic fill:#fff7ed,stroke:#c2410c,stroke-dasharray:4 3,color:#7c2d12
  class n_ctrl_client_segments,n_ctrl_macro_parameters,n_ctrl_reporting_periods ctrl
  class n_out_period_summary__pid dynamic
  class n_card_transactions_csv,n_ctrl_client_segments_csv,n_ctrl_macro_parameters_csv,n_ctrl_reporting_periods_csv,n_customers_csv,n_loan_payments_csv,n_loans_csv file
  class n_out_card_summary,n_out_customer_risk,n_out_customers_clean,n_out_delinquency,n_out_exec_forecast,n_out_exec_report,n_out_fc,n_out_fc_est,n_out_fraud_freq,n_out_loan_enriched,n_out_loan_status,n_out_monthly_repay,n_out_period_summary_202509,n_out_period_summary_202510,n_out_period_summary_202511,n_out_period_summary_202512,n_out_portfolio_by_segment,n_out_portfolio_report,n_out_reg_est out
  class n_00_load_raw,n_01_customer_clean,n_02_loan_enrichment,n_03_risk_format,n_04_payment_delinquency,n_05_card_fraud_summary,n_07_portfolio_report,n_08_period_driver,n_09_payment_model,n_10_repayment_forecast,n_11_executive_report program
  class n_stg_card_transactions,n_stg_customers,n_stg_loan_payments,n_stg_loans stg
```

## 3. Code lineage: shared code and macros

This diagram leaves the data out and shows only how the code is connected. Macros are the purple
hexagons.

- Program 06 is a library: it defines two macros, `%summarize` and `%flag_high`, and produces no data.
- Programs 07 and 08 copy program 06 in with `%include` and use its macros.
- Program 08 also has macros inside macros: `%period_driver` calls `%run_period`, which calls
  `%summarize` from program 06. That is three levels of nesting, and the last level is in a different file.

An arrow labelled "used by" points from a macro to the program or macro that calls it; "included by"
points from program 06 to the programs that copy it in.

```mermaid
flowchart LR
  n_06_macro_library(["06_macro_library"])
  n_07_portfolio_report(["07_portfolio_report"])
  n_08_period_driver(["08_period_driver"])
  n_flag_high{{"%flag_high"}}
  n_period_driver{{"%period_driver"}}
  n_run_period{{"%run_period"}}
  n_summarize{{"%summarize"}}
  n_06_macro_library -.->|"included by"| n_07_portfolio_report
  n_06_macro_library -.->|"included by"| n_08_period_driver
  n_06_macro_library -->|"defines"| n_flag_high
  n_06_macro_library -->|"defines"| n_summarize
  n_08_period_driver -->|"defines"| n_period_driver
  n_08_period_driver -->|"defines"| n_run_period
  n_flag_high -.->|"used by"| n_07_portfolio_report
  n_period_driver -.->|"used by"| n_08_period_driver
  n_run_period -.->|"used by"| n_period_driver
  n_summarize -.->|"used by"| n_07_portfolio_report
  n_summarize -.->|"used by"| n_08_period_driver
  n_summarize -.->|"used by"| n_run_period
  linkStyle 0,1,6,7,8,9,10,11 stroke-dasharray:5 4
  classDef file fill:#fefce8,stroke:#a16207,color:#422006
  classDef program fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
  classDef macro fill:#f3e8ff,stroke:#7e22ce,color:#3b0764
  classDef stg fill:#fdf2f8,stroke:#be185d,color:#500724
  classDef ctrl fill:#ecfdf5,stroke:#047857,color:#022c22
  classDef out fill:#f3f4f6,stroke:#6b7280,color:#111827
  classDef dynamic fill:#fff7ed,stroke:#c2410c,stroke-dasharray:4 3,color:#7c2d12
  class n_flag_high,n_period_driver,n_run_period,n_summarize macro
  class n_06_macro_library,n_07_portfolio_report,n_08_period_driver program
```

## 4. Migration order

A program can only be tested after the programs whose results it reads have been converted, because it
needs their output as input. An arrow from A to B means B depends on A (the label says how many of A's
tables B reads), so programs on the left are migrated first.

```mermaid
flowchart LR
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_01_customer_clean(["01_customer_clean"])
  n_00_load_raw(["00_load_raw"]) -- "2 tables" --> n_02_loan_enrichment(["02_loan_enrichment"])
  n_00_load_raw(["00_load_raw"]) -- "2 tables" --> n_04_payment_delinquency(["04_payment_delinquency"])
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_05_card_fraud_summary(["05_card_fraud_summary"])
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_07_portfolio_report(["07_portfolio_report"])
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_08_period_driver(["08_period_driver"])
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_10_repayment_forecast(["10_repayment_forecast"])
  n_00_load_raw(["00_load_raw"]) -- "1 table" --> n_11_executive_report(["11_executive_report"])
  n_01_customer_clean(["01_customer_clean"]) -- "1 table" --> n_02_loan_enrichment(["02_loan_enrichment"])
  n_01_customer_clean(["01_customer_clean"]) -- "1 table" --> n_03_risk_format(["03_risk_format"])
  n_02_loan_enrichment(["02_loan_enrichment"]) -- "1 table" --> n_07_portfolio_report(["07_portfolio_report"])
  n_02_loan_enrichment(["02_loan_enrichment"]) -- "1 table" --> n_09_payment_model(["09_payment_model"])
  n_02_loan_enrichment(["02_loan_enrichment"]) -- "1 table" --> n_11_executive_report(["11_executive_report"])
  n_04_payment_delinquency(["04_payment_delinquency"]) -- "1 table" --> n_08_period_driver(["08_period_driver"])
  n_04_payment_delinquency(["04_payment_delinquency"]) -- "1 table" --> n_11_executive_report(["11_executive_report"])
  n_10_repayment_forecast(["10_repayment_forecast"]) -- "2 tables" --> n_11_executive_report(["11_executive_report"])
  n_06_macro_library(["06_macro_library"]) -.->|"%include"| n_07_portfolio_report
  n_06_macro_library(["06_macro_library"]) -.->|"%include"| n_08_period_driver
  linkStyle 16,17 stroke-dasharray:5 4
  classDef program fill:#dbeafe,stroke:#1d4ed8,color:#1e3a8a
  class n_00_load_raw,n_01_customer_clean,n_02_loan_enrichment,n_03_risk_format,n_04_payment_delinquency,n_05_card_fraud_summary,n_06_macro_library,n_07_portfolio_report,n_08_period_driver,n_09_payment_model,n_10_repayment_forecast,n_11_executive_report program
```

## Legend

| Shape and colour | Meaning |
|---|---|
| Yellow cylinder | a source file (CSV) |
| Blue rounded box | a SAS program |
| Purple hexagon | a SAS macro |
| Rectangle | a SAS table: pink `stg` (loaded source data), green `ctrl` (settings), grey `out` (results) |
| Orange dashed box | a table whose name is only known when the program runs (LOW confidence) |
| Dashed arrow | "included by" (`%include`), "used by" (a macro call), or a LOW-confidence link |

## What this lineage does not cover

- It works at table level: it tells you that program 11 reads `out.loan_status`, not which columns it
  uses. The column-by-column trace in section 1b was done by reading the code.
- It reads the code without running it, plus one log of the code SAS actually ran (MPRINT, for program
  08). SAS code that writes and runs other SAS code on the fly (`CALL EXECUTE`) would be missed.
- It recognises libraries that point to folders (`libname stg "<folder>"`), not libraries that connect
  to a database.
- The diagrams leave out the library nodes and the cases where a program reads a table it has just
  written itself (program 00 re-reads `stg.customers` to check it; program 07 re-reads
  `out.portfolio_by_segment` between two macro calls).
