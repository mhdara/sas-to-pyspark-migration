# Power BI dashboard

The dashboard shows the results of the project in one place: how many programs each method converted, which
checks passed, how the data migration went, and how the SAS programs and tables are connected. It is built in
Power BI Desktop from the 20 CSV files in [`outputs/bi/`](../outputs/bi/), which
[`python/lineage_bi.py`](../python/lineage_bi.py) produces from the project's results. The `.pbix` file itself
is not in this repository.

[Power BI dashboard screenshot will be added here]

<!-- To add it: save the screenshot as powerbi/dashboard.png and replace the line above with
![Power BI dashboard](dashboard.png) -->

## The data model

![Power BI model view: the migration-results tables on the left, the bank data on the right, standalone tables at the bottom](model.png)

*Model view in Power BI Desktop. Left: the migration results (DimProgram, DimMethod and DimTable with their result tables). Right: the bank's data (DimSegment → DimCustomer → DimLoan → FactLoanPayment, plus FactCardTransaction and DimDate). Bottom: standalone tables without relationships.*

The tables are shaped for Power BI before they are loaded (one row means one clear thing in every table), so
the dashboard only needs relationships and a few simple measures, not heavy transformations. There are two
separate parts.

**The migration results.** Three lookup tables describe the programs, the methods and the SAS tables. The
result tables point to them:

| Lookup table | Result tables that use it | Joined on |
|---|---|---|
| DimProgram (one row per SAS program) | FactClassification, FactConversionAttempt, FactConversionStatus, FactValidationCheck | `program_id` |
| DimMethod (one row per method) | FactClassification, FactConversionAttempt, FactConversionStatus, FactValidationCheck | `method_id` = `method` |
| DimTable (one row per SAS table) | FactReconciliationCheck, FactValidationCheck | `table_id` |

**The bank's data.** This part lets the dashboard show the data the SAS programs work on. Customers belong to
segments, loans belong to customers, and payments belong to loans, so the lookups form a chain: DimSegment →
DimCustomer → DimLoan → FactLoanPayment. Card transactions link to DimCustomer, and both payments and card
transactions link to a date table (DimDate). The loan whose customer (999) does not exist shows up as
"(Blank)", which makes that planted problem visible.

A few tables are used on their own, without relationships: FactLineageEdge, FactLineagePath, FactForecast,
ShowcaseNames and the two summary tables. All relationships are many-to-one and filter in one direction.

What one row means in each result table:

| Table | One row is |
|---|---|
| FactConversionStatus | one program converted by one method, with its final result |
| FactConversionAttempt | one attempt (one call to a model), with time, tokens, cost and any error |
| FactValidationCheck | one check comparing a converted program's output with SAS |
| FactReconciliationCheck | one check comparing a SAS table with its Parquet copy |
| FactClassification | one label (business area, technical type or complexity) given to one program by one method |
| FactLoanPayment | one loan in one month, after the duplicate was removed (taken from SAS's own output) |
| FactCardTransaction | one card transaction |
| FactForecast | one month, with SAS's forecast, Python's forecast and the difference |
| FactLineageEdge | one connection in the lineage graph, either data (a file loaded into a table, a table read or written by a program) or code (`%include`, a macro defined or called) |
| FactLineagePath | one result table together with one item behind it (a program, table, macro or source file) and how many steps away it is |

## Measures

```
Validated          = CALCULATE(COUNTROWS(FactConversionStatus), FactConversionStatus[final_status] = "VALIDATED")
Validation Rate    = DIVIDE([Validated], COUNTROWS(FactConversionStatus))
Checks Passed %    = DIVIDE(SUM(FactValidationCheck[passed]), COUNTROWS(FactValidationCheck))
Migration Checks Passed % = CALCULATE(DIVIDE(SUM(FactReconciliationCheck[passed]), COUNTROWS(FactReconciliationCheck)),
                                      FactReconciliationCheck[expected_to_pass] = 1)
Conversion Cost (USD) = SUM(FactConversionAttempt[cost_usd])
Max Forecast Diff  = MAX(FactForecast[abs_diff])
Late Payment Rate  = AVERAGE(FactLoanPayment[delinquent_flag])
```

`Migration Checks Passed %` leaves out the three checks that are meant to fail (the deliberately cut-off
names), so it measures only the real migration.

## Pages

These are the pages the dashboard was designed with.

| Page | What it shows |
|---|---|
| Migration overview | share of programs converted, share of migration checks passed, total cost, and converted programs per method |
| Code conversion | the result of every program for every method, and SAS's forecast against Python's, month by month |
| Data migration | every migration check, and the four accented showcase names as they appear in the CSV, in SAS and in Parquet |
| Migration risks | the trap checks, per method |
| Cost and effort | cost and model time per method, and every attempt with its error message |
| Code inventory and lineage | the facts about each program; the lineage network, with one view for data connections and one for code connections; and a back-trace: choose a result table and a list shows everything behind it, nearest first, down to the source files |
| Bank data | customers by segment and income band, repayments and late payments by month, card spending |
