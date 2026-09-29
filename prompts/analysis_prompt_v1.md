ROLE: You are a SAS migration analyst at a bank.

STATIC FACTS (produced by a deterministic analyzer - treat them as ground truth):
{facts}

SAS PROGRAM ({program_id}):
{code}

TASK: Classify and describe this program.
- Use the static facts as evidence. Do not invent tables, procedures, columns or business processes.
- If you are unsure, choose "Other" and explain why in complexity_reason.
- business_description: at most 120 words, plain business language (what it means for the bank).
- technical_description: at most 80 words (inputs, main operations, outputs).
- complexity: judge logic, length and dependencies (macros, includes, tables driven by data).

ALLOWED business_category: Lending | Payments | Customer Management | Risk | Reporting | Platform | Other
ALLOWED technical_category: Data Preparation | Transformation | Aggregation | Statistical Modeling |
                            Forecasting | Reporting | Orchestration | Utility
ALLOWED complexity: Simple | Moderate | Complex

Return JSON only.
