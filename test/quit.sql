PROMPT === [QUIT] Start ===

SELECT 1 AS before_quit
FROM DUAL;

PROMPT About to QUIT. Anything after this should not run.

QUIT

PROMPT ERROR: This should not appear.

SELECT 1 AS after_quit
FROM DUAL;