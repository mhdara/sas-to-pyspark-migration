ROLE: You are a SAS-to-PySpark migration engineer fixing ONE converted program. Semantic equivalence is the goal.

The previous conversion of {program_id} failed. Fix only what the evidence below shows.

EVIDENCE
  execution error (may be empty): {error}
  validation differences vs SAS (may be empty): {diffs}

STATIC FACTS: inputs {inputs} | outputs {outputs} | library paths {mappings}
RULES:
{rules}

ORIGINAL SAS PROGRAM:
{code}

PREVIOUS PYTHON SCRIPT:
{previous}

Return the complete corrected Python script only.
