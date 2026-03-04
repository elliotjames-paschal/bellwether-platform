# Fix Pipeline Error

When given a pipeline error traceback, do the following:

## 1. Parse the error

Extract from the traceback:
- **File path** and **line number** where the crash occurred
- **Exception type** (ValueError, TypeError, KeyError, LossySetitemError, etc.)
- **The failing expression** (e.g., `int(election_date.year)`)

## 2. Read the failing code

Read the file at the crash location. Read enough context (30-50 lines around the error) to understand:
- What function/loop the error is in
- What data types the variables are expected to be
- Where the data comes from (CSV column, JSON field, function argument)

## 3. Identify the full distribution of edge cases

Based on the exception type and the data source, enumerate ALL possible bad inputs that could cause this class of error. Common ones for this pipeline:

- **NaN / NaT / None** — pandas columns often contain these
- **Float where int expected** — CSV columns load as float64 (e.g., `2026.0`)
- **Partial/malformed strings** — dates like `"2025-09-"`, empty strings
- **Wrong dtype column** — writing float into int-typed pandas column
- **Missing keys** — dict lookups on data that doesn't exist
- **Type mismatches** — Timestamp vs string vs date objects

## 4. Write tests

Add tests to `packages/pipelines/tests/test_election_dates.py` (or create a new test file if the error is in a different domain). Each test should:
- Replicate the exact code path that crashed
- Feed it one specific bad input
- Assert it either produces the correct fallback or doesn't crash

Name tests descriptively: `test_truncation_with_nan_year_doesnt_crash`

## 5. Run tests locally

```bash
python3 -m pytest packages/pipelines/tests/ -v
```

All tests must pass before proceeding.

## 6. Fix the code

Apply the minimal fix to handle the edge cases. Prefer:
- `try/except` with fallback over dropping data
- Type coercion at the boundary (where data enters) over defensive checks everywhere
- Fixing the data source if possible

## 7. Re-run tests

Confirm all tests still pass after the fix.

## 8. Run the failing script locally

If the data files exist locally, run the actual script end-to-end:
```bash
python3 packages/pipelines/<script_that_crashed>.py
```

## 9. Commit

Only after local tests AND the script run clean.

## Input

Paste the error traceback from Sherlock (from `tail -f ~/logs/bellwether_<jobid>.err`).

$ARGUMENTS
