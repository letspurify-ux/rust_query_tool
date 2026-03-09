//! Shared SQL text helpers used across execution, formatting, and IntelliSense.
use once_cell::sync::Lazy;
use std::collections::HashSet;

/// Shared Oracle SQL keywords used by parser, IntelliSense, and formatter.
pub const ORACLE_SQL_KEYWORDS: &[&str] = &[
    "ABSENT",
    "ACCEPT",
    "ACCESSIBLE",
    "ACCOUNT",
    "ADD",
    "ADMINISTER",
    "ADVISE",
    "AFTER",
    "ALL",
    "ALTER",
    "ANALYZE",
    "AND",
    "ANTI",
    "ANY",
    "ANYDATA",
    "ANYDATASET",
    "ANYTYPE",
    "APPEND",
    "APPLY",
    "ARCHIVE",
    "AS",
    "ASC",
    "ASOF",
    "ASSOCIATE",
    "AT",
    "AUDIT",
    "AUTHID",
    "AUTOMATIC",
    "AUTONOMOUS_TRANSACTION",
    "AVG",
    "BASICFILE",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BFILE",
    "BINARY_DOUBLE",
    "BINARY_FLOAT",
    "BINARY_INTEGER",
    "BITMAP",
    "BLOB",
    "BODY",
    "BOOLEAN",
    "BREADTH",
    "BREAK",
    "BREAKS",
    "BULK",
    "BY",
    "CACHE",
    "CALL",
    "CALLING",
    "CASCADE",
    "CASE",
    "CAST",
    "CHECK",
    "CHUNK",
    "CLASS",
    "CLEAR",
    "CLOB",
    "CLOSE",
    "CLUSTER",
    "COALESCE",
    "COLLATE",
    "COLLECT",
    "COLSEP",
    "COLUMN",
    "COLUMNS",
    "COMMENT",
    "COMMIT",
    "COMMIT_LOGGING",
    "COMMIT_WAIT",
    "COMPILE",
    "COMPLETE",
    "COMPOUND",
    "COMPRESS",
    "COMPUTE",
    "COMPUTES",
    "CONDITIONAL",
    "CONN",
    "CONNECT",
    "CONNECT_BY_ISCYCLE",
    "CONNECT_BY_ISLEAF",
    "CONNECT_BY_ROOT",
    "CONSTRAINT",
    "CONTAINER",
    "CONTENT",
    "CONTEXT",
    "CONTINUE",
    "COUNT",
    "CREATE",
    "CROSS",
    "CUME_DIST",
    "CURRENT",
    "CURRENT_DATE",
    "CURRENT_SCHEMA",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURRVAL",
    "CURSOR",
    "CYCLE",
    "DATABASE",
    "DATE",
    "DAY",
    "DBTIMEZONE",
    "DEBUG",
    "DECLARE",
    "DECODE",
    "DEDUPLICATE",
    "DEFAULT",
    "DEFERRABLE",
    "DEFERRED",
    "DEFINE",
    "DEFINER",
    "DELETE",
    "DELETING",
    "DENSE_RANK",
    "DEPTH",
    "DESC",
    "DESCRIBE",
    "DETERMINISTIC",
    "DIMENSION",
    "DIRECTORY",
    "DISABLE",
    "DISASSOCIATE",
    "DISC",
    "DISCONNECT",
    "DISTINCT",
    "DO",
    "DOCUMENT",
    "DROP",
    "DUAL",
    "EACH",
    "EDITION",
    "EDITIONABLE",
    "EDITIONING",
    "ELSE",
    "ELSEIF",
    "ELSIF",
    "EMPTY",
    "ENABLE",
    "ENABLE_STORAGE_IN_ROW",
    "END",
    "ERROR",
    "ERRORS",
    "EVENTS",
    "EXCEPT",
    "EXCEPTION",
    "EXCEPTIONS",
    "EXCEPTION_INIT",
    "EXCLUDE",
    "EXEC",
    "EXECUTE",
    "EXISTS",
    "EXIT",
    "EXPIRE",
    "EXPLAIN",
    "EXTERNAL",
    "EXTERNALLY",
    "FAST",
    "FEEDBACK",
    "FETCH",
    "FIRST",
    "FIRST_VALUE",
    "FLASHBACK",
    "FOLLOWING",
    "FOLLOWS",
    "FOR",
    "FORALL",
    "FORCE",
    "FOREIGN",
    "FORMAT",
    "FREEPOOLS",
    "FROM",
    "FULL",
    "FUNCTION",
    "GENERATED",
    "GLOBAL",
    "GLOBALLY",
    "GOTO",
    "GRANT",
    "GROUP",
    "HASH",
    "HAVING",
    "HEAP",
    "HOST",
    "HOUR",
    "IDENTIFIED",
    "IDENTITY",
    "IF",
    "IGNORE",
    "IMMEDIATE",
    "IN",
    "INCLUDE",
    "INCLUDING",
    "INCREMENT",
    "INDEX",
    "INITIALLY",
    "INITRANS",
    "INNER",
    "INSERT",
    "INSERTING",
    "INSTEAD",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVALIDATE",
    "INVISIBLE",
    "IOT",
    "IS",
    "ISOLATION_LEVEL",
    "ITERATE",
    "JAVA",
    "JOIN",
    "JSON",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_OBJECTAGG",
    "JSON_QUERY",
    "JSON_TABLE",
    "JSON_VALUE",
    "KEEP",
    "KEY",
    "LAG",
    "LANGUAGE",
    "LAST",
    "LAST_VALUE",
    "LATERAL",
    "LEAD",
    "LEFT",
    "LESS",
    "LEVEL",
    "LIBRARY",
    "LIKE",
    "LIMIT",
    "LINK",
    "LIST",
    "LISTAGG",
    "LOB",
    "LOCAL",
    "LOCALTIMESTAMP",
    "LOCK",
    "LOCKED",
    "LOGGING",
    "LONG",
    "LOOP",
    "MAIN",
    "MAPPING",
    "MATCH",
    "MATCHED",
    "MATCH_RECOGNIZE",
    "MATERIALIZED",
    "MAX",
    "MAXTRANS",
    "MAXVALUE",
    "MEASURES",
    "MEMBER",
    "MERGE",
    "METADATA",
    "MIN",
    "MINUS",
    "MINUTE",
    "MINVALUE",
    "MODEL",
    "MONITORING",
    "MONTH",
    "NAME",
    "NATURAL",
    "NAV",
    "NCHAR",
    "NCLOB",
    "NESTED",
    "NEVER",
    "NEW_VALUE",
    "NEXT",
    "NEXTVAL",
    "NLS_CALENDAR",
    "NLS_COMP",
    "NLS_CURRENCY",
    "NLS_DATE_FORMAT",
    "NLS_ISO_CURRENCY",
    "NLS_LANGUAGE",
    "NLS_LENGTH_SEMANTICS",
    "NLS_NCHAR_CONV_EXCP",
    "NLS_NUMERIC_CHARACTERS",
    "NLS_SORT",
    "NLS_TERRITORY",
    "NLS_TIMESTAMP_FORMAT",
    "NLS_TIMESTAMP_TZ_FORMAT",
    "NOARCHIVE",
    "NOAUDIT",
    "NOCACHE",
    "NOCOMPRESS",
    "NOCOPY",
    "NOCYCLE",
    "NOEXCEPTIONS",
    "NOFORCE",
    "NOLOGGING",
    "NOMONITORING",
    "NONE",
    "NONEDITIONABLE",
    "NONEDITIONING",
    "NOPARALLEL",
    "NORELY",
    "NOSORT",
    "NOT",
    "NOTHING",
    "NOVALIDATE",
    "NOWAIT",
    "NTH_VALUE",
    "NTILE",
    "NULL",
    "NULLS",
    "NUMBER",
    "NVARCHAR2",
    "NVL",
    "OBJECT",
    "OF",
    "OFF",
    "OFFSET",
    "OMIT",
    "ON",
    "ONE",
    "ONLY",
    "OPEN",
    "OPTIMIZER_MODE",
    "OR",
    "ORDER",
    "ORDINALITY",
    "ORGANIZATION",
    "OSERROR",
    "OTHERS",
    "OUT",
    "OUTER",
    "OVER",
    "OVERFLOW",
    "OVERLAY",
    "PACKAGE",
    "PACKAGE_BODY",
    "PARALLEL",
    "PARALLEL_ENABLE",
    "PARAMETERS",
    "PARTITION",
    "PASSING",
    "PASSWORD",
    "PATH",
    "PATTERN",
    "PAUSE",
    "PCTFREE",
    "PCTUSED",
    "PCTVERSION",
    "PER",
    "PERCENT",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "PERCENT_RANK",
    "PERIOD",
    "PIPE",
    "PIPELINED",
    "PIVOT",
    "PLSCOPE_SETTINGS",
    "PLSQL_CCFLAGS",
    "PLSQL_CODE_TYPE",
    "PLSQL_DEBUG",
    "PLSQL_OPTIMIZE_LEVEL",
    "PLSQL_WARNINGS",
    "PLS_INTEGER",
    "POINT",
    "POSITION",
    "PRAGMA",
    "PRECEDES",
    "PRECEDING",
    "PRESERVE",
    "PRIMARY",
    "PRINT",
    "PRIOR",
    "PRIVATE",
    "PROCEDURE",
    "PROFILE",
    "PROMPT",
    "PUBLIC",
    "PURGE",
    "QUALIFY",
    "QUIT",
    "QUOTES",
    "RAISE",
    "RANGE",
    "RANK",
    "RAW",
    "READ",
    "RECOGNIZE",
    "RECORD",
    "RECURSIVE",
    "RECYCLEBIN",
    "REF",
    "REFCURSOR",
    "REFERENCE",
    "REFERENCES",
    "REFERENCING",
    "REFRESH",
    "RELY",
    "REM",
    "REMARK",
    "RENAME",
    "REPEAT",
    "REPEATABLE",
    "REPLACE",
    "RESOURCE",
    "RESPECT",
    "RESTORE",
    "RESULT_CACHE",
    "RESUMABLE",
    "RETENTION",
    "RETURN",
    "RETURNING",
    "RETURNS",
    "REUSE",
    "REVERSE",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLLBACK",
    "ROW",
    "ROWID",
    "ROWNUM",
    "ROWS",
    "ROW_NUMBER",
    "RULES",
    "SAMPLE",
    "SAVEPOINT",
    "SCHEMA",
    "SCN",
    "SEARCH",
    "SECOND",
    "SECUREFILE",
    "SEED",
    "SELECT",
    "SEMI",
    "SEQUENCE",
    "SEQUENTIAL",
    "SERIAL",
    "SERIALIZABLE",
    "SERVEROUTPUT",
    "SESSION",
    "SESSIONTIMEZONE",
    "SET",
    "SETTINGS",
    "SHARE",
    "SHARING",
    "SHOW",
    "SHUTDOWN",
    "SIBLINGS",
    "SIMPLE_INTEGER",
    "SINGLE",
    "SIZE",
    "SKIP",
    "SOME",
    "SOURCE",
    "SPECIFICATION",
    "SPOOL",
    "SQLERROR",
    "SQL_TRACE",
    "START",
    "STARTUP",
    "STATISTICS_LEVEL",
    "STORAGE",
    "STORE",
    "STRAIGHT_JOIN",
    "SUBMULTISET",
    "SUBPARTITION",
    "SUBSET",
    "SUBSTRING",
    "SUBTYPE",
    "SUM",
    "SYNONYM",
    "SYSDATE",
    "SYSTEM",
    "SYSTIMESTAMP",
    "SYS_CONNECT_BY_PATH",
    "SYS_OUTPUT",
    "SYS_REFCURSOR",
    "TABLE",
    "TABLESAMPLE",
    "TABLESPACE",
    "TEMPORARY",
    "THAN",
    "THEN",
    "TIES",
    "TIME",
    "TIMESTAMP",
    "TIME_ZONE",
    "TIMING",
    "TO",
    "TOP",
    "TO_CHAR",
    "TO_DATE",
    "TO_NUMBER",
    "TRACEFILE_IDENTIFIER",
    "TRANSACTION",
    "TRIGGER",
    "TRIMSPOOL",
    "TRUNCATE",
    "TYPE",
    "UNBOUNDED",
    "UNCONDITIONAL",
    "UNDEFINE",
    "UNION",
    "UNIQUE",
    "UNLIMITED",
    "UNLOCK",
    "UNPIVOT",
    "UNTIL",
    "UPDATE",
    "UPDATING",
    "UPSERT",
    "USAGE",
    "USE",
    "USER",
    "USING",
    "USING_NLS_COMP",
    "VALIDATE",
    "VALUES",
    "VAR",
    "VARCHAR2",
    "VARIABLE",
    "VARRAY",
    "VERIFY",
    "VERSIONS",
    "VIEW",
    "VISIBLE",
    "WAIT",
    "WELLFORMED",
    "WHEN",
    "WHENEVER",
    "WHERE",
    "WHILE",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "WRAPPED",
    "WRAPPER",
    "WRITE",
    "XML",
    "XMLATTRIBUTES",
    "XMLCAST",
    "XMLCDATA",
    "XMLCOLATTVAL",
    "XMLCOMMENT",
    "XMLCONCAT",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLFOREST",
    "XMLPARSE",
    "XMLPI",
    "XMLQUERY",
    "XMLROOT",
    "XMLSEQUENCE",
    "XMLSERIALIZE",
    "XMLTABLE",
    "XMLTRANSFORM",
    "XMLTYPE",
    "YEAR",
    "ZONE",
    "_ORACLE_SCRIPT",
];

/// Formatter clause boundaries that should start on a new line.
pub(crate) const FORMAT_CLAUSE_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "UNION",
    "INTERSECT",
    "MINUS",
    "EXCEPT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "VALUES",
    "SET",
    "INTO",
    "OFFSET",
    "FETCH",
    "LIMIT",
    "CONNECT",
    "START",
    "RETURNING",
    "MODEL",
    "WINDOW",
    "MATCH_RECOGNIZE",
    "QUALIFY",
    "WITH",
];

/// `CREATE TABLE ...` suffix keywords used by formatter to split storage clauses.
pub(crate) const FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS: &[&str] = &[
    "PCTFREE",
    "PCTUSED",
    "INITRANS",
    "MAXTRANS",
    "COMPRESS",
    "NOCOMPRESS",
    "LOGGING",
    "NOLOGGING",
    "STORAGE",
    "TABLESPACE",
    "USING",
    "ENABLE",
    "DISABLE",
    "CACHE",
    "NOCACHE",
    "PARALLEL",
    "NOPARALLEL",
    "MONITORING",
    "NOMONITORING",
    "ORGANIZATION",
    "INCLUDING",
    "LOB",
    "PARTITION",
    "SUBPARTITION",
    "SHARING",
];

/// JOIN modifier keywords used by SQL formatter line-break rules.
pub(crate) const FORMAT_JOIN_MODIFIER_KEYWORDS: &[&str] =
    &["LEFT", "RIGHT", "FULL", "INNER", "CROSS"];

/// Condition keywords that should align in multiline SQL formatter output.
pub(crate) const FORMAT_CONDITION_KEYWORDS: &[&str] = &["ON", "AND", "OR", "WHEN"];

/// Block-start keywords used by SQL formatter indentation for PL/SQL blocks.
pub(crate) const FORMAT_BLOCK_START_KEYWORDS: &[&str] = &["DECLARE", "IF", "REPEAT"];

/// Supported qualifiers for `END ...` in formatter block indentation logic.
pub(crate) const FORMAT_BLOCK_END_QUALIFIER_KEYWORDS: &[&str] =
    &["LOOP", "IF", "CASE", "REPEAT", "FOR", "WHILE"];

/// Shared SQL keyword lookup set for lexer/highlighting and IntelliSense checks.
pub static ORACLE_SQL_KEYWORDS_SET: Lazy<HashSet<&'static str>> =
    Lazy::new(|| ORACLE_SQL_KEYWORDS.iter().copied().collect());

const WITH_MAIN_QUERY_KEYWORDS: &[&str] = &[
    "WITH", "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "VALUES", "TABLE",
];

pub(crate) const SUBQUERY_HEAD_KEYWORDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "VALUES", "WITH", "TABLE",
];

const WITH_PLSQL_DECLARATION_KEYWORDS: &[&str] = &["FUNCTION", "PROCEDURE"];

/// Top-level `WITH ...` clause keywords that indicate non-PL/SQL clause usage
/// (e.g. `WITH READ ONLY`, `WITH CHECK OPTION`, `WITH ROWID`).
const WITH_NON_PLSQL_CLAUSE_KEYWORDS: &[&str] = &[
    "READ",
    "CHECK",
    "NO",
    "DATA",
    "TIES",
    "CONSTRAINT",
    "ROWID",
    "SEQUENCE",
    "COMMIT",
    "SCN",
    "OBJECT",
    "PRIMARY",
    "REDUCED",
    "OIDS",
    "LOCAL",
    "CASCADED",
    // GRANT/REVOKE option clauses (e.g. WITH GRANT OPTION, WITH ADMIN OPTION,
    // WITH HIERARCHY OPTION) are non-PL/SQL and should immediately exit
    // WITH FUNCTION/PROCEDURE declaration tracking.
    "GRANT",
    "ADMIN",
    "DELEGATE",
    "HIERARCHY",
];

const EXTERNAL_LANGUAGE_TARGET_KEYWORDS: &[&str] = &["C", "JAVA", "JAVASCRIPT", "PYTHON", "MLE"];

const EXTERNAL_LANGUAGE_CLAUSE_KEYWORDS: &[&str] = &[
    "EXTERNAL",
    "LANGUAGE",
    "NAME",
    "LIBRARY",
    "AGENT",
    "CREDENTIAL",
    "PARAMETERS",
    "CALLING",
    "WITH",
];

const SQLPLUS_SET_OPTION_KEYWORDS: &[&str] = &[
    "APPINFO",
    "ARRAYSIZE",
    "AUTOCOMMIT",
    "AUTOPRINT",
    "AUTORECOVERY",
    "AUTOTRACE",
    "BLOCKTERMINATOR",
    "CMDSEP",
    "COLINVISIBLE",
    "COLSEP",
    "CONCAT",
    "COPYCOMMIT",
    "COPYTYPECHECK",
    "DEFINE",
    "DESCRIBE",
    "ECHO",
    "EDITFILE",
    "EMBEDDED",
    "ESCAPE",
    "FEEDBACK",
    "FLAGGER",
    "FLUSH",
    "HEADING",
    "HEADSEP",
    "INSTANCE",
    "LINESIZE",
    "LOBOFFSET",
    "LONG",
    "LONGCHUNKSIZE",
    "MARKUP",
    "NEWPAGE",
    "NULL",
    "NUMFORMAT",
    "NUMWIDTH",
    "PAGESIZE",
    "PAUSE",
    "RECSEP",
    "RECSEPCHAR",
    "ROWLIMIT",
    "SERVEROUTPUT",
    "SHIFTINOUT",
    "SHOWMODE",
    "SQLBLANKLINES",
    "SQLCASE",
    "SQLCONTINUE",
    "SQLFORMAT",
    "SQLNUMBER",
    "SQLPLUSCOMPATIBILITY",
    "SQLPREFIX",
    "SQLPROMPT",
    "SQLTERMINATOR",
    "SUFFIX",
    "TAB",
    "TERMOUT",
    "TIMING",
    "TRIMOUT",
    "TRIMSPOOL",
    "UNDERLINE",
    "VERIFY",
    "WRAP",
];

pub(crate) const FORMAT_COLUMN_CONSTRAINT_KEYWORDS: &[&str] = &[
    "CONSTRAINT",
    "NOT",
    "NULL",
    "DEFAULT",
    "PRIMARY",
    "UNIQUE",
    "CHECK",
    "REFERENCES",
    "ENABLE",
    "DISABLE",
    "USING",
    "COLLATE",
    "GENERATED",
    "IDENTITY",
];

const TABLE_FUNCTION_ITEM_LEADING_KEYWORDS: &[&str] = &[
    "NESTED",
    "PATH",
    "COLUMNS",
    "EXISTS",
    "FOR",
    "ORDINALITY",
    "ERROR",
    "NULL",
    "DEFAULT",
    "ON",
    "FORMAT",
    "WRAPPER",
    "WITHOUT",
    "WITH",
    "CONDITIONAL",
    "UNCONDITIONAL",
    "KEEP",
    "OMIT",
    "QUOTES",
];

const STATEMENT_HEAD_KEYWORDS: &[&str] = &[
    "DECLARE",
    "BEGIN",
    "WITH",
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "CALL",
    "EXPLAIN",
    "ANALYZE",
    "ADMINISTER",
    "ARCHIVE",
    "COMMENT",
    "SET",
    "SHOW",
    "STORE",
    "GET",
    "SAVE",
    "USE",
    "DESCRIBE",
    "DESC",
    "EXEC",
    "EXECUTE",
    "START",
    "STARTUP",
    "PROMPT",
    "RUN",
    "R",
    "REM",
    "REMARK",
    "CONNECT",
    "CONN",
    "DISCONNECT",
    "DISC",
    "SPOOL",
    "DEFINE",
    "WHENEVER",
    "VARIABLE",
    "VAR",
    "PRINT",
    "ACCEPT",
    "PAUSE",
    "UNDEFINE",
    "COLUMN",
    "BREAK",
    "CLEAR",
    "COMPUTE",
    "EXIT",
    "QUIT",
    "SHUTDOWN",
    "HOST",
    "TIMING",
    "TTITLE",
    "BTITLE",
    "REPHEADER",
    "REPFOOTER",
    "PASSWORD",
    "PASSW",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "PURGE",
    "FLASHBACK",
    "RECOVER",
    "SAVEPOINT",
    "LOCK",
    "COMMIT",
    "ROLLBACK",
    "AUDIT",
    "NOAUDIT",
    "ASSOCIATE",
    "DISASSOCIATE",
    "GRANT",
    "REVOKE",
    "VALUES",
    "TABLE",
];

/// O(1) lookup set for `STATEMENT_HEAD_KEYWORDS` (80+ entries).
static STATEMENT_HEAD_KEYWORDS_SET: Lazy<HashSet<&'static str>> =
    Lazy::new(|| STATEMENT_HEAD_KEYWORDS.iter().copied().collect());

#[inline]
fn matches_keyword(keyword: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| keyword.eq_ignore_ascii_case(candidate))
}

#[inline]
fn is_password_command_keyword(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "PASSW" | "PASSWO" | "PASSWOR" | "PASSWORD"
    )
}

#[inline]
pub(crate) fn is_identifier_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '$' || ch == '#'
}

/// Character-level identifier *start* check.
///
/// Unlike [`is_identifier_char`], this rejects numeric starts while still
/// allowing non-ASCII alphabetic characters.
#[inline]
pub(crate) fn is_identifier_start_char(ch: char) -> bool {
    ch.is_alphabetic() || ch == '_' || ch == '$' || ch == '#'
}

/// Byte-level identifier check (equivalent to `is_identifier_char` for ASCII).
///
/// Covers alphanumeric, `_`, `$`, `#`.  Used as *continue* predicate by
/// syntax highlighting, editor word expansion, and script parsing.
#[inline]
pub(crate) fn is_identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$' || byte == b'#'
}

/// Returns true when `byte` can *start* an SQL identifier token.
///
/// Digits are excluded: identifiers may contain digits but cannot begin with one.
#[inline]
pub(crate) fn is_identifier_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte == b'$' || byte == b'#'
}

/// Returns the matching closing delimiter for an Oracle q-quoted string.
///
/// `q'[hello]'`  →  `[` opens, `]` closes.
/// `q'!hello!'`  →  `!` opens and closes.
#[inline]
pub(crate) fn is_valid_q_quote_delimiter(delimiter: char) -> bool {
    delimiter.is_ascii() && !delimiter.is_ascii_whitespace() && delimiter != '\''
}

/// Byte version of [`is_valid_q_quote_delimiter`].
#[inline]
pub(crate) fn is_valid_q_quote_delimiter_byte(delimiter: u8) -> bool {
    is_valid_q_quote_delimiter(char::from(delimiter))
}

#[inline]
pub(crate) fn q_quote_closing(delimiter: char) -> char {
    match delimiter {
        '[' => ']',
        '(' => ')',
        '{' => '}',
        '<' => '>',
        other => other,
    }
}

/// Byte version of [`q_quote_closing`].
#[inline]
pub(crate) fn q_quote_closing_byte(delimiter: u8) -> u8 {
    match delimiter {
        b'[' => b']',
        b'(' => b')',
        b'{' => b'}',
        b'<' => b'>',
        other => other,
    }
}

/// Returns true when `text_upper` starts with `keyword` as a standalone token.
///
/// `text_upper` and `keyword` are expected to already be uppercased.
pub(crate) fn starts_with_keyword_token(text_upper: &str, keyword: &str) -> bool {
    if text_upper == keyword {
        return true;
    }
    let Some(rest) = text_upper.strip_prefix(keyword) else {
        return false;
    };
    match rest.as_bytes().first() {
        None => true,
        Some(&b) if b < 0x80 => b.is_ascii_whitespace() || matches!(b, b';' | b',' | b'(' | b')'),
        // Non-ASCII byte: decode and check for Unicode whitespace
        _ => rest.chars().next().is_none_or(|c| c.is_whitespace()),
    }
}

/// Strips surrounding double quotes from SQL identifiers and unescapes doubled quotes.
pub(crate) fn strip_identifier_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(inner) = trimmed.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        return inner.replace("\"\"", "\"");
    }
    trimmed.to_string()
}

/// Returns true when a line starts with SQL*Plus `REM`/`REMARK` comment commands.
pub(crate) fn is_sqlplus_remark_comment_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    matches!(
        trimmed.split_whitespace().next(),
        Some(first)
            if first.eq_ignore_ascii_case("REM") || first.eq_ignore_ascii_case("REMARK")
    )
}

/// Returns true when a line is a SQL*Plus-style comment-only line.
///
/// Recognizes:
/// - `-- ...`
/// - `REM ...`
/// - `REMARK ...`
pub(crate) fn is_sqlplus_comment_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("--") || is_sqlplus_remark_comment_line(trimmed)
}

/// Returns true if `word` is one of the shared Oracle SQL keywords.
#[inline]
pub(crate) fn is_oracle_sql_keyword(word: &str) -> bool {
    ORACLE_SQL_KEYWORDS_SET.contains(word)
}

/// Returns true when a keyword can start the main query after a WITH clause.
pub(crate) fn is_with_main_query_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_MAIN_QUERY_KEYWORDS)
}

/// Returns true when a keyword starts an Oracle top-level `WITH FUNCTION/PROCEDURE` declaration.
pub(crate) fn is_with_plsql_declaration_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_PLSQL_DECLARATION_KEYWORDS)
}

/// Returns true when a top-level `WITH` token clearly belongs to a non-PL/SQL
/// clause (for example `WITH READ ONLY` in view definitions).
pub(crate) fn is_with_non_plsql_clause_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_NON_PLSQL_CLAUSE_KEYWORDS)
}

/// Returns true when a token can reasonably start a new top-level statement.
///
/// Used as a recovery signal when the parser stayed inside an Oracle
/// `WITH FUNCTION/PROCEDURE` declaration mode but encountered another
/// statement head instead of a main query keyword.
pub(crate) fn is_statement_head_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    STATEMENT_HEAD_KEYWORDS_SET.contains(upper.as_str()) || is_password_command_keyword(word)
}

/// Returns true when `word` is a SQL*Plus `SET` option keyword.
pub(crate) fn is_sqlplus_set_option_keyword(word: &str) -> bool {
    matches_keyword(word, SQLPLUS_SET_OPTION_KEYWORDS)
}

pub(crate) fn is_auto_terminated_tool_command(line: &str) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.starts_with("@@") || trimmed.starts_with('@') {
        return true;
    }

    if trimmed.starts_with('!') {
        return true;
    }

    let Some(first) = next_meaningful_word(trimmed, 0).map(|(word, _)| word) else {
        return false;
    };

    let first_upper = first.to_ascii_uppercase();
    match first_upper.as_str() {
        // Keywords requiring second-word disambiguation
        "START" => {
            let second = next_meaningful_word(trimmed, 1).map(|(word, _)| word);
            second.is_some_and(|word| !word.eq_ignore_ascii_case("WITH"))
        }
        "R" => next_meaningful_word(trimmed, 1).is_none(),
        "CONNECT" => next_meaningful_word(trimmed, 1)
            .map(|(word, _)| word)
            .is_some_and(|second| !second.eq_ignore_ascii_case("BY")),
        "SET" => {
            let Some(second) = next_meaningful_word(trimmed, 1).map(|(word, _)| word) else {
                return false;
            };
            is_sqlplus_set_option_keyword(second)
        }
        "SHOW" => next_meaningful_word(trimmed, 1).is_some(),
        // PASSWORD abbreviations
        "PASSW" | "PASSWO" | "PASSWOR" | "PASSWORD" => true,
        // Simple auto-terminated keywords (no second-word check needed)
        "DISC" | "DISCONNECT" | "CONN" | "RUN" | "EXIT" | "QUIT" | "STARTUP" | "SHUTDOWN"
        | "RECOVER" | "ARCHIVE" | "HOST" | "TIMING" | "TTITLE" | "BTITLE" | "REPHEADER"
        | "REPFOOTER" | "PROMPT" | "REM" | "REMARK" | "SPOOL" | "STORE" | "GET" | "SAVE"
        | "DESCRIBE" | "DESC" | "EXEC" | "EXECUTE" | "DEFINE" | "UNDEFINE" | "VARIABLE" | "VAR"
        | "PRINT" | "ACCEPT" | "PAUSE" | "WHENEVER" | "COLUMN" | "BREAK" | "CLEAR" | "COMPUTE" => {
            true
        }
        _ => false,
    }
}

fn next_meaningful_word(line: &str, skip_words: usize) -> Option<(&str, usize)> {
    let mut idx = 0usize;
    let mut seen_words = 0usize;

    while idx < line.len() {
        if line[idx..].starts_with("--") {
            return None;
        }

        if line[idx..].starts_with("/*") {
            let block_start = idx + 2;
            let block_end = line[block_start..].find("*/")?;
            idx = block_start + block_end + 2;
            continue;
        }

        let ch = line[idx..].chars().next()?;
        let ch_len = ch.len_utf8();

        if ch.is_whitespace() {
            idx += ch_len;
            continue;
        }

        let mut end = idx;
        while end < line.len() {
            let word_ch = line[end..].chars().next()?;
            if word_ch.is_whitespace()
                || line[end..].starts_with("/*")
                || line[end..].starts_with("--")
            {
                break;
            }
            end += word_ch.len_utf8();
        }

        if seen_words == skip_words {
            return Some((&line[idx..end], idx));
        }

        seen_words += 1;
        idx = end;
    }

    None
}

/// Returns true when a keyword can head a subquery body after `(`.
pub(crate) fn is_subquery_head_keyword(word: &str) -> bool {
    matches_keyword(word, SUBQUERY_HEAD_KEYWORDS)
}

/// Returns true when a CTE state machine should recover back to normal parsing.
pub(crate) fn is_cte_recovery_keyword(word: &str) -> bool {
    is_subquery_head_keyword(word)
}

/// Returns true when a token is a valid Oracle `EXTERNAL LANGUAGE` target.
pub(crate) fn is_external_language_target_keyword(word: &str) -> bool {
    matches_keyword(word, EXTERNAL_LANGUAGE_TARGET_KEYWORDS)
}

/// Returns true when a token participates in Oracle EXTERNAL routine clauses.
pub(crate) fn is_external_language_clause_keyword(word: &str) -> bool {
    matches_keyword(word, EXTERNAL_LANGUAGE_CLAUSE_KEYWORDS)
}

/// Returns true when a token starts a CREATE TABLE column constraint section.
pub(crate) fn is_format_column_constraint_keyword(word: &str) -> bool {
    matches_keyword(word, FORMAT_COLUMN_CONSTRAINT_KEYWORDS)
}

/// Returns true when a token is a leading clause keyword for table-function columns.
pub(crate) fn is_table_function_item_leading_keyword(word: &str) -> bool {
    matches_keyword(word, TABLE_FUNCTION_ITEM_LEADING_KEYWORDS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn oracle_sql_keyword_lookup_uses_uppercase_tokens() {
        assert!(is_oracle_sql_keyword("SELECT"));
        assert!(!is_oracle_sql_keyword("select"));
    }

    #[test]
    fn statement_head_keyword_includes_ddl_and_tcl_roots() {
        assert!(is_statement_head_keyword("CREATE"));
        assert!(is_statement_head_keyword("ALTER"));
        assert!(is_statement_head_keyword("DROP"));
        assert!(is_statement_head_keyword("TRUNCATE"));
        assert!(is_statement_head_keyword("RENAME"));
        assert!(is_statement_head_keyword("GRANT"));
        assert!(is_statement_head_keyword("REVOKE"));
        assert!(is_statement_head_keyword("COMMIT"));
        assert!(is_statement_head_keyword("ROLLBACK"));
    }

    #[test]
    fn with_non_plsql_clause_keyword_detects_common_oracle_clauses() {
        for keyword in [
            "READ",
            "CHECK",
            "NO",
            "DATA",
            "TIES",
            "ROWID",
            "OBJECT",
            "PRIMARY",
            "REDUCED",
            "CASCADED",
            "CONSTRAINT",
            "GRANT",
            "ADMIN",
            "DELEGATE",
            "HIERARCHY",
        ] {
            assert!(
                is_with_non_plsql_clause_keyword(keyword),
                "{keyword} should be recognized as a non-PL/SQL WITH clause keyword"
            );
        }
        assert!(!is_with_non_plsql_clause_keyword("FUNCTION"));
        assert!(!is_with_non_plsql_clause_keyword("SELECT"));
    }

    #[test]
    fn statement_head_keywords_do_not_contain_duplicates() {
        let mut seen = HashSet::new();
        for keyword in STATEMENT_HEAD_KEYWORDS {
            assert!(
                seen.insert(*keyword),
                "duplicate statement head keyword: {keyword}"
            );
        }
    }

    #[test]
    fn formatter_keyword_groups_stay_in_shared_keyword_pool() {
        assert!(FORMAT_CLAUSE_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_CREATE_SUFFIX_BREAK_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_JOIN_MODIFIER_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_CONDITION_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_BLOCK_START_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_BLOCK_END_QUALIFIER_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_COLUMN_CONSTRAINT_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
    }

    #[test]
    fn shared_keyword_pool_includes_oracle_trigger_and_edition_keywords() {
        for keyword in [
            "BREADTH",
            "COMPOUND",
            "CYCLE",
            "DEPTH",
            "DO",
            "FOLLOWS",
            "INSTEAD",
            "JSON",
            "JSON_ARRAY",
            "JSON_ARRAYAGG",
            "JSON_EXISTS",
            "JSON_OBJECT",
            "JSON_OBJECTAGG",
            "JSON_QUERY",
            "JSON_TABLE",
            "JSON_VALUE",
            "NOFORCE",
            "NONEDITIONING",
            "PRECEDES",
            "REFERENCING",
        ] {
            assert!(
                is_oracle_sql_keyword(keyword),
                "missing shared keyword: {keyword}"
            );
        }
    }

    #[test]
    fn shared_keyword_pool_includes_parser_and_highlighter_keywords() {
        for keyword in [
            "PACKAGE_BODY",
            "RECOGNIZE",
            "REPEATABLE",
            "SHARE",
            "SUBMULTISET",
            "TABLESAMPLE",
            "WRAPPED",
            "XML",
        ] {
            assert!(
                is_oracle_sql_keyword(keyword),
                "missing shared keyword: {keyword}"
            );
        }
    }

    #[test]
    fn auto_terminated_tool_command_detects_connect_aliases() {
        assert!(is_auto_terminated_tool_command("CONNECT scott/tiger"));
        assert!(is_auto_terminated_tool_command("CONN scott/tiger"));
        assert!(is_auto_terminated_tool_command("DISCONNECT"));
        assert!(is_auto_terminated_tool_command("DISC"));
        assert!(is_auto_terminated_tool_command("@child.sql"));
        assert!(is_auto_terminated_tool_command("  @@child.sql"));
        assert!(is_auto_terminated_tool_command("PROMPT hello"));
        assert!(is_auto_terminated_tool_command("REM comment"));
        assert!(is_auto_terminated_tool_command("REMARK comment"));
        assert!(is_auto_terminated_tool_command("HOST ls"));
        assert!(is_auto_terminated_tool_command("! ls"));
        assert!(is_auto_terminated_tool_command("EXIT"));
        assert!(is_auto_terminated_tool_command("QUIT"));
        assert!(is_auto_terminated_tool_command("STARTUP"));
        assert!(is_auto_terminated_tool_command("SHUTDOWN IMMEDIATE"));
        assert!(is_auto_terminated_tool_command("ARCHIVE LOG LIST"));
        assert!(is_auto_terminated_tool_command("RECOVER DATABASE"));
        assert!(is_auto_terminated_tool_command("SPOOL out.log"));
        assert!(is_auto_terminated_tool_command("STORE SET out.sql REPLACE"));
        assert!(is_auto_terminated_tool_command("GET script.sql"));
        assert!(is_auto_terminated_tool_command("SAVE script.sql"));
        assert!(is_auto_terminated_tool_command("DESCRIBE emp"));
        assert!(is_auto_terminated_tool_command("DESC emp"));
        assert!(is_auto_terminated_tool_command(
            "EXEC dbms_output.put_line('x')"
        ));
        assert!(is_auto_terminated_tool_command(
            "EXECUTE dbms_output.put_line('x')"
        ));
        assert!(is_auto_terminated_tool_command("DEFINE v = 1"));
        assert!(is_auto_terminated_tool_command("UNDEFINE v"));
        assert!(is_auto_terminated_tool_command("WHENEVER SQLERROR EXIT"));
        assert!(is_auto_terminated_tool_command("COLUMN ename FORMAT A20"));
        assert!(is_auto_terminated_tool_command("CLEAR COLUMNS"));
        assert!(is_auto_terminated_tool_command(
            "SHOW PARAMETER open_cursors"
        ));
        assert!(is_auto_terminated_tool_command("SHOW ERRORS"));
        assert!(is_auto_terminated_tool_command("PASSWO scott"));
        assert!(is_auto_terminated_tool_command("PASSWOR scott"));
        assert!(is_auto_terminated_tool_command("PASSWORD scott"));
    }

    #[test]
    fn auto_terminated_tool_command_detects_recover_and_archive_heads() {
        assert!(is_auto_terminated_tool_command("RECOVER DATABASE"));
        assert!(is_auto_terminated_tool_command("ARCHIVE LOG LIST"));
    }

    #[test]
    fn statement_head_keyword_detects_password_abbreviations() {
        assert!(is_statement_head_keyword("PASSW"));
        assert!(is_statement_head_keyword("PASSWO"));
        assert!(is_statement_head_keyword("PASSWOR"));
        assert!(is_statement_head_keyword("PASSWORD"));
    }

    #[test]
    fn sqlplus_set_option_keyword_detects_supported_set_subcommands() {
        assert!(is_sqlplus_set_option_keyword("SQLFORMAT"));
        assert!(is_sqlplus_set_option_keyword("APPINFO"));
        assert!(is_sqlplus_set_option_keyword("TERMOUT"));
        assert!(!is_sqlplus_set_option_keyword("UNKNOWN"));
    }

    #[test]
    fn auto_terminated_tool_command_ignores_connect_by_sql_clause() {
        assert!(!is_auto_terminated_tool_command(
            "CONNECT BY PRIOR id = parent_id"
        ));
        assert!(!is_auto_terminated_tool_command(
            "CONNECT /*hierarchical*/ BY PRIOR id = parent_id"
        ));
        assert!(!is_auto_terminated_tool_command(
            "CONNECT /*+ hint */ BY PRIOR id = parent_id"
        ));
    }

    #[test]
    fn auto_terminated_tool_command_set_with_block_comment_is_detected() {
        assert!(is_auto_terminated_tool_command(
            "SET /*sqlplus*/ TERMOUT ON"
        ));
        assert!(is_auto_terminated_tool_command(
            "SET /*a*/ /*b*/ PAGESIZE 100"
        ));
    }

    #[test]
    fn auto_terminated_tool_command_ignores_leading_line_comment_before_set() {
        assert!(!is_auto_terminated_tool_command(
            "-- comment\nSET TERMOUT ON"
        ));
    }

    #[test]
    fn auto_terminated_tool_command_ignores_comment_only_block_comment_line() {
        assert!(!is_auto_terminated_tool_command("/* comment */"));
    }

    #[test]
    fn auto_terminated_tool_command_ignores_unterminated_block_comment() {
        assert!(!is_auto_terminated_tool_command("SET /* unterminated"));
    }

    #[test]
    fn auto_terminated_tool_command_ignores_start_with_sql_clause() {
        assert!(is_auto_terminated_tool_command("START child.sql"));
        assert!(!is_auto_terminated_tool_command("START WITH"));
        assert!(!is_auto_terminated_tool_command("START /*tree*/ WITH"));
        assert!(!is_auto_terminated_tool_command(
            "START WITH parent_id IS NULL"
        ));
    }

    #[test]
    fn auto_terminated_tool_command_requires_arguments_for_start_and_connect() {
        assert!(!is_auto_terminated_tool_command("START"));
        assert!(!is_auto_terminated_tool_command("CONNECT"));
        assert!(is_auto_terminated_tool_command("START script.sql"));
        assert!(is_auto_terminated_tool_command("CONNECT scott/tiger"));
    }

    #[test]
    fn parser_and_intellisense_keyword_groups_are_shared() {
        assert!(is_with_plsql_declaration_keyword("FUNCTION"));
        assert!(is_with_plsql_declaration_keyword("procedure"));
        assert!(is_external_language_target_keyword("javascript"));
        assert!(is_external_language_target_keyword("mle"));
        assert!(is_external_language_clause_keyword("LANGUAGE"));
        assert!(is_external_language_clause_keyword("AGENT"));
        assert!(is_external_language_clause_keyword("CREDENTIAL"));
        assert!(is_external_language_clause_keyword("parameters"));
        assert!(is_format_column_constraint_keyword("generated"));
        assert!(is_table_function_item_leading_keyword("ORDINALITY"));
        assert!(is_table_function_item_leading_keyword("quotes"));
    }

    #[test]
    fn shared_keyword_pool_includes_additional_oracle_keywords() {
        for keyword in [
            "EDITIONING",
            "OMIT",
            "ORDINALITY",
            "OVERLAY",
            "POSITION",
            "SUBSET",
            "SUBSTRING",
            "XMLCAST",
            "XMLTABLE",
        ] {
            assert!(
                is_oracle_sql_keyword(keyword),
                "missing shared keyword: {keyword}"
            );
        }
    }

    #[test]
    fn statement_head_keywords_include_common_oracle_ddl_dcl_and_tcl() {
        for keyword in [
            "ALTER",
            "CREATE",
            "DROP",
            "TRUNCATE",
            "RENAME",
            "GRANT",
            "REVOKE",
            "COMMIT",
            "ROLLBACK",
            "SAVEPOINT",
            "LOCK",
            "FLASHBACK",
            "PURGE",
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
        ] {
            assert!(
                is_statement_head_keyword(keyword),
                "missing statement head keyword: {keyword}"
            );
        }
    }
}
