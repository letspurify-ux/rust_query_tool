//! Shared SQL text helpers used across execution, formatting, and IntelliSense.
use once_cell::sync::Lazy;
use std::borrow::Cow;
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
    "CHAR",
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
    "CONSTRUCTOR",
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
    "FINAL",
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
    "INSTANTIABLE",
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
    "MAP",
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
    "OVERRIDING",
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
    "SQLCODE",
    "SQLERRM",
    "SQLERROR",
    "SQL_TRACE",
    "START",
    "STARTUP",
    "STATIC",
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
    "USING",
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

/// Formatter clause starters that should remain stable layout anchors during
/// the secondary indentation pass.
pub(crate) const FORMAT_LAYOUT_CLAUSE_START_KEYWORDS: &[&str] = &[
    "SELECT",
    "WITH",
    "FROM",
    "WHERE",
    "GROUP",
    "HAVING",
    "ORDER",
    "VALUES",
    "SET",
    "CONNECT",
    "START",
    "UNION",
    "INTERSECT",
    "MINUS",
    "EXCEPT",
    "RETURNING",
    "MODEL",
    "WINDOW",
    "MATCH_RECOGNIZE",
    "PIVOT",
    "UNPIVOT",
    "QUALIFY",
    "OFFSET",
    "FETCH",
    "LIMIT",
    "SEARCH",
    "CYCLE",
];

pub(crate) const FORMAT_SET_OPERATOR_KEYWORDS: &[&str] = &["UNION", "INTERSECT", "MINUS", "EXCEPT"];

/// Leading keywords that keep the following line on the same continuation
/// depth when a comment splits the expression or clause body.
pub(crate) const FORMAT_COMMENT_CONTINUATION_KEYWORDS: &[&str] = &[
    "AND", "OR", "IN", "IS", "LIKE", "BETWEEN", "NOT", "EXISTS", "USING", "INTO", "ON", "JOIN",
];

/// Clause/subclause headers that should keep the next line at the same depth
/// after an inline comment split.
const FORMAT_INLINE_COMMENT_HEADER_SAME_DEPTH_KEYWORDS: &[&str] = &["WITH"];

/// Clause/subclause headers whose body should continue one level deeper than
/// the owning query base after an inline comment split.
const FORMAT_INLINE_COMMENT_HEADER_QUERY_BASE_KEYWORDS: &[&str] = &[
    "FROM",
    "WHERE",
    "HAVING",
    "USING",
    "INTO",
    "ON",
    "JOIN",
    "CONNECT",
    "START",
    "UNION",
    "INTERSECT",
    "MINUS",
    "EXCEPT",
    "MODEL",
    "WINDOW",
    "MATCH_RECOGNIZE",
    "PIVOT",
    "UNPIVOT",
    "QUALIFY",
    "SEARCH",
    "CYCLE",
];

/// Clause/subclause headers whose body should continue one level deeper than
/// the current header line after an inline comment split.
const FORMAT_INLINE_COMMENT_HEADER_CURRENT_LINE_KEYWORDS: &[&str] = &[
    "SELECT",
    "VALUES",
    "SET",
    "RETURNING",
    "OFFSET",
    "FETCH",
    "LIMIT",
    "MEASURES",
    "REFERENCE",
    "SUBSET",
    "PATTERN",
    "DEFINE",
    "RULES",
    "COLUMNS",
    "KEEP",
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
    &["LEFT", "RIGHT", "FULL", "INNER", "CROSS", "NATURAL"];

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
    // Recursive subquery factoring clauses that can appear before the main query
    // and should keep WITH FUNCTION/PROCEDURE parsing attached to the same statement.
    "SEARCH", "CYCLE",
];

pub(crate) const SUBQUERY_HEAD_KEYWORDS: &[&str] = &[
    "SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CALL", "VALUES", "WITH", "TABLE",
];

const WITH_PLSQL_DECLARATION_KEYWORDS: &[&str] = &["FUNCTION", "PROCEDURE", "PACKAGE", "TYPE"];

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

/// Query-head `WITH` constructs that are neither CTEs nor PL/SQL declarations
/// (e.g. SQL Server `WITH XMLNAMESPACES (...) SELECT ...`).
const WITH_NON_CTE_QUERY_HEAD_KEYWORDS: &[&str] = &["XMLNAMESPACES"];

const EXTERNAL_LANGUAGE_TARGET_KEYWORDS: &[&str] = &[
    "C",
    "JAVA",
    "JAVASCRIPT",
    "PYTHON",
    "R",
    "RUST",
    "WASM",
    "MLE",
];

const EXTERNAL_LANGUAGE_CLAUSE_KEYWORDS: &[&str] = &[
    "EXTERNAL",
    "LANGUAGE",
    "NAME",
    "LIBRARY",
    "MODULE",
    "SIGNATURE",
    "ENV",
    "ENVIRONMENT",
    "AGENT",
    "CREDENTIAL",
    "PARAMETERS",
    "CALLING",
    "WITH",
    "IMPORT",
    "IMPORTS",
    "EXPORT",
    "EXPORTS",
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
    !delimiter.is_whitespace() && delimiter != '\''
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

/// Updates `in_block_comment` state for a single trimmed line.
///
/// This properly handles lines that contain both `*/` (closing) and `/*` (opening)
/// on the same line (e.g. `*/ SELECT /* ... `).  Both `line_auto_format_depths` and
/// `apply_parser_depth_indentation` must use this instead of ad-hoc `contains("*/")`.
pub(crate) fn update_block_comment_state(trimmed: &str, in_block_comment: &mut bool) {
    let bytes = trimmed.as_bytes();
    let mut i = 0usize;

    while i < bytes.len() {
        if *in_block_comment {
            if i + 1 < bytes.len() && bytes[i] == b'*' && bytes[i + 1] == b'/' {
                *in_block_comment = false;
                i += 2;
                continue;
            }
            i += 1;
        } else {
            if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
                *in_block_comment = true;
                i += 2;
                continue;
            }
            // Stop scanning at line comment
            if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
                break;
            }
            // Skip string literals to avoid false matches on /* */ inside strings
            if bytes[i] == b'\'' {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if i < bytes.len() && bytes[i] == b'\'' {
                            i += 1;
                            continue;
                        }
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            i += 1;
        }
    }
}

/// Returns the prefix before a trailing inline comment, if the line ends with
/// `-- ...` or `/* ... */` after skipping quoted SQL literals.
///
/// Returns `None` when the line has no trailing inline comment or when a block
/// comment is followed by additional significant SQL text.
pub(crate) fn trailing_inline_comment_prefix(line: &str) -> Option<&str> {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut q_quote_end: Option<u8> = None;

    while idx < bytes.len() {
        let current = bytes[idx];
        let next = bytes.get(idx.saturating_add(1)).copied();

        if let Some(closing) = q_quote_end {
            if current == closing && next == Some(b'\'') {
                q_quote_end = None;
                idx = idx.saturating_add(2);
                continue;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_single_quote {
            if current == b'\'' {
                if next == Some(b'\'') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_single_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_double_quote {
            if current == b'"' {
                if next == Some(b'"') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_double_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if current == b'-' && next == Some(b'-') {
            return line.get(..idx);
        }

        if current == b'/' && next == Some(b'*') {
            let comment_start = idx;
            idx = idx.saturating_add(2);
            while idx + 1 < bytes.len() {
                if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                    let comment_end = idx.saturating_add(2);
                    let suffix = line.get(comment_end..).unwrap_or_default().trim();
                    if suffix.is_empty() {
                        return line.get(..comment_start);
                    }
                    return None;
                }
                idx = idx.saturating_add(1);
            }
            return None;
        }

        if (current == b'q' || current == b'Q') && next == Some(b'\'') {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(2)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    idx = idx.saturating_add(3);
                    continue;
                }
            }
        }
        if (current == b'n' || current == b'N' || current == b'u' || current == b'U')
            && matches!(next, Some(b'q' | b'Q'))
            && bytes.get(idx.saturating_add(2)) == Some(&b'\'')
        {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(3)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    idx = idx.saturating_add(4);
                    continue;
                }
            }
        }
        if current == b'\'' {
            in_single_quote = true;
            idx = idx.saturating_add(1);
            continue;
        }
        if current == b'"' {
            in_double_quote = true;
            idx = idx.saturating_add(1);
            continue;
        }

        idx = idx.saturating_add(1);
    }

    None
}

/// Returns the last non-whitespace ASCII byte before a trailing inline comment.
pub(crate) fn trailing_significant_byte_before_inline_comment(line: &str) -> Option<u8> {
    line_trailing_identifiers_before_inline_comment(line, 0).0
}

pub(crate) fn line_ends_with_open_paren_before_inline_comment(line: &str) -> bool {
    trailing_significant_byte_before_inline_comment(line) == Some(b'(')
}

pub(crate) fn line_ends_with_comma_before_inline_comment(line: &str) -> bool {
    trailing_significant_byte_before_inline_comment(line) == Some(b',')
}

pub(crate) fn line_is_standalone_open_paren_before_inline_comment(line: &str) -> bool {
    let prefix = trailing_inline_comment_prefix(line).unwrap_or(line);
    prefix.trim() == "("
}

/// Returns true if `word` is one of the shared Oracle SQL keywords.
#[inline]
pub(crate) fn is_oracle_sql_keyword(word: &str) -> bool {
    ORACLE_SQL_KEYWORDS_SET.contains(word)
}

/// Returns true for PL/SQL control-flow keywords that may also appear as aliases.
pub(crate) fn is_plsql_control_keyword(word: &str) -> bool {
    let upper: Cow<'_, str> = if word.bytes().any(|b| b.is_ascii_lowercase()) {
        Cow::Owned(word.to_ascii_uppercase())
    } else {
        Cow::Borrowed(word)
    };

    matches!(
        upper.as_ref(),
        "IF" | "THEN"
            | "ELSE"
            | "ELSIF"
            | "CASE"
            | "LOOP"
            | "FOR"
            | "WHILE"
            | "REPEAT"
            | "DECLARE"
            | "BEGIN"
            | "END"
    )
}

/// Returns true when a keyword can start the main query after a WITH clause.
pub(crate) fn is_with_main_query_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_MAIN_QUERY_KEYWORDS)
}

/// Returns true when a keyword starts an Oracle top-level `WITH` PL/SQL declaration.
pub(crate) fn is_with_plsql_declaration_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_PLSQL_DECLARATION_KEYWORDS)
}

/// Returns true when a top-level `WITH` PL/SQL declaration uses `AS/IS` to
/// open a declaration body that stays active until a matching `END`.
pub(crate) fn with_plsql_declaration_starts_routine_body(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "FUNCTION" | "PROCEDURE" | "PACKAGE"
    )
}

/// Returns true when a top-level `WITH` token clearly belongs to a non-PL/SQL
/// clause (for example `WITH READ ONLY` in view definitions).
pub(crate) fn is_with_non_plsql_clause_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_NON_PLSQL_CLAUSE_KEYWORDS)
}

/// Returns true when a top-level `WITH` starts a query-head clause that is not
/// a CTE/PLSQL declaration (for example `WITH XMLNAMESPACES (...) SELECT ...`).
pub(crate) fn is_with_non_cte_query_head_keyword(word: &str) -> bool {
    matches_keyword(word, WITH_NON_CTE_QUERY_HEAD_KEYWORDS)
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

/// Returns the first meaningful token-like word on a line, skipping
/// leading whitespace and comments.
pub(crate) fn first_meaningful_word(line: &str) -> Option<&str> {
    next_meaningful_word(line, 0).map(|(word, _)| word)
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

/// Returns true when `text_upper` starts with a formatter clause that should
/// behave as a stable layout anchor in the indentation pass.
pub(crate) fn starts_with_format_layout_clause(text_upper: &str) -> bool {
    FORMAT_LAYOUT_CLAUSE_START_KEYWORDS
        .iter()
        .any(|keyword| starts_with_keyword_token(text_upper, keyword))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatSetOperatorKind {
    Union,
    Intersect,
    Minus,
    Except,
}

impl FormatSetOperatorKind {
    pub(crate) fn from_clause_start(text_upper: &str) -> Option<Self> {
        if starts_with_keyword_token(text_upper, "UNION") {
            Some(Self::Union)
        } else if starts_with_keyword_token(text_upper, "INTERSECT") {
            Some(Self::Intersect)
        } else if starts_with_keyword_token(text_upper, "MINUS") {
            Some(Self::Minus)
        } else if starts_with_keyword_token(text_upper, "EXCEPT") {
            Some(Self::Except)
        } else {
            None
        }
    }
}

pub(crate) fn is_format_set_operator_keyword(word: &str) -> bool {
    matches_keyword(word, FORMAT_SET_OPERATOR_KEYWORDS)
}

pub(crate) fn starts_with_format_set_operator(text_upper: &str) -> bool {
    FORMAT_SET_OPERATOR_KEYWORDS
        .iter()
        .any(|keyword| starts_with_keyword_token(text_upper, keyword))
}

pub(crate) fn starts_with_format_join_clause(text_upper: &str) -> bool {
    if starts_with_keyword_token(text_upper, "JOIN")
        || starts_with_keyword_token(text_upper, "APPLY")
    {
        return true;
    }

    let starts_with_join_modifier = starts_with_keyword_token(text_upper, "OUTER")
        || FORMAT_JOIN_MODIFIER_KEYWORDS
            .iter()
            .any(|keyword| starts_with_keyword_token(text_upper, keyword));

    starts_with_join_modifier && (text_upper.contains(" JOIN") || text_upper.contains(" APPLY"))
}

pub(crate) fn is_format_join_condition_clause(text_upper: &str) -> bool {
    starts_with_keyword_token(text_upper, "ON") || starts_with_keyword_token(text_upper, "USING")
}

pub(crate) fn starts_with_format_for_update_split_header(text_upper: &str) -> bool {
    starts_with_keyword_token(text_upper, "FOR")
        && !text_upper.contains(" LOOP")
        && !text_upper.contains(" IN ")
        && !text_upper.contains(" UPDATE")
}

pub(crate) fn starts_with_format_for_update_clause(text_upper: &str) -> bool {
    starts_with_format_for_update_split_header(text_upper)
        || (starts_with_keyword_token(text_upper, "FOR") && text_upper.contains(" UPDATE"))
}

fn starts_with_auto_format_structural_continuation_boundary_without_expression_owner_impl(
    line: &str,
) -> bool {
    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return false;
    }

    let trimmed_upper = trimmed.to_ascii_uppercase();
    line_starts_query_head(&trimmed_upper)
        || starts_with_format_layout_clause(&trimmed_upper)
        || starts_with_keyword_token(&trimmed_upper, "INTO")
        || starts_with_keyword_token(&trimmed_upper, "USING")
        || starts_with_format_join_clause(&trimmed_upper)
        || is_format_join_condition_clause(&trimmed_upper)
        || starts_with_format_for_update_clause(&trimmed_upper)
        || format_query_owner_pending_header_kind(trimmed).is_some()
        || format_indented_paren_pending_header_kind(trimmed).is_some()
        || starts_with_format_model_subclause(&trimmed_upper)
        || starts_with_format_match_recognize_subclause(&trimmed_upper)
        || starts_with_auto_format_owner_boundary_without_expression_owner(trimmed)
}

/// Shared structural boundary helper for continuation/comment carry.
///
/// This intentionally excludes generic expression owners such as `MULTISET`
/// so callers can keep operator RHS continuation on those lines when needed.
pub(crate) fn starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
    line: &str,
) -> bool {
    starts_with_auto_format_structural_continuation_boundary_without_expression_owner_impl(
        auto_format_structural_tail(line),
    )
}

/// Returns true when a CREATE query-body DDL header line owns the following
/// query body through a trailing `AS`.
pub(crate) fn line_is_create_query_body_header(line: &str) -> bool {
    if !line_ends_with_keyword(line, "AS") {
        return false;
    }

    let trimmed_upper = line.trim_start().to_ascii_uppercase();
    if !starts_with_keyword_token(&trimmed_upper, "CREATE") {
        return false;
    }

    let words: Vec<&str> = trimmed_upper.split_whitespace().collect();
    let mut idx = 1usize;

    while idx < words.len() {
        match words[idx] {
            "OR" if words.get(idx + 1).copied() == Some("REPLACE") => {
                idx += 2;
            }
            "NO" if words.get(idx + 1).copied() == Some("FORCE") => {
                idx += 2;
            }
            "FORCE" | "EDITIONABLE" | "NONEDITIONABLE" | "EDITIONING" => {
                idx += 1;
            }
            _ => break,
        }
    }

    if matches!(words.get(idx).copied(), Some("VIEW")) {
        return true;
    }

    if matches!(words.get(idx).copied(), Some("MATERIALIZED"))
        && matches!(words.get(idx + 1).copied(), Some("VIEW"))
    {
        return true;
    }

    if matches!(words.get(idx).copied(), Some("TABLE")) {
        return true;
    }

    (matches!(words.get(idx).copied(), Some("GLOBAL" | "PRIVATE")))
        && matches!(words.get(idx + 1).copied(), Some("TEMPORARY"))
        && matches!(words.get(idx + 2).copied(), Some("TABLE"))
}

pub(crate) fn line_starts_query_head(trimmed_upper: &str) -> bool {
    first_meaningful_word(trimmed_upper).is_some_and(is_subquery_head_keyword)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatQueryOwnerKind {
    Clause,
    FromItem,
    Condition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatPlsqlChildQueryOwnerKind {
    ControlBody,
    CursorDeclaration,
    OpenCursorFor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingFormatPlsqlChildQueryOwnerHeaderKind {
    CursorDeclaration,
    OpenCursorFor,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingFormatQueryOwnerHeaderKind {
    ReferenceOn,
    JoinLike,
    ConditionNot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SplitQueryOwnerLookaheadKind {
    GenericExpression,
    DirectFromItem,
}

impl FormatQueryOwnerKind {
    /// Returns the minimum structural owner depth that a split owner-header
    /// line should keep before the child query opens.
    pub(crate) fn header_depth_floor(
        self,
        query_base_depth: Option<usize>,
        condition_header_depth: Option<usize>,
    ) -> Option<usize> {
        match self {
            Self::Clause | Self::FromItem => query_base_depth,
            Self::Condition => condition_header_depth
                .or_else(|| query_base_depth.map(|depth| depth.saturating_add(1))),
        }
    }

    /// Returns the analyzer owner-base depth that should feed the next nested
    /// query head after this owner line.
    pub(crate) fn auto_format_child_query_owner_base_depth(
        self,
        resolved_owner_depth: usize,
        query_base_depth: Option<usize>,
    ) -> usize {
        match self {
            Self::Condition => query_base_depth
                .map(|depth| resolved_owner_depth.max(depth.saturating_add(1)))
                .unwrap_or(resolved_owner_depth.saturating_add(1)),
            Self::Clause | Self::FromItem => resolved_owner_depth,
        }
    }

    /// Returns the structural formatter depth for the next nested query head
    /// relative to this owner line and the surrounding resolved query base.
    pub(crate) fn formatter_child_query_head_depth(
        self,
        resolved_owner_depth: usize,
        query_base_depth: Option<usize>,
    ) -> usize {
        match self {
            Self::Clause | Self::Condition => query_base_depth
                .map(|depth| depth.saturating_add(2))
                .map(|depth| depth.max(resolved_owner_depth.saturating_add(1)))
                .unwrap_or(resolved_owner_depth.saturating_add(1)),
            Self::FromItem => resolved_owner_depth.saturating_add(1),
        }
    }
}

impl PendingFormatQueryOwnerHeaderKind {
    pub(crate) fn owner_kind_for_line(self, line: &str) -> Option<FormatQueryOwnerKind> {
        match self {
            Self::ReferenceOn => Some(FormatQueryOwnerKind::Clause),
            Self::JoinLike => {
                if line_ends_with_keyword(line, "APPLY") {
                    Some(FormatQueryOwnerKind::FromItem)
                } else if line_ends_with_keyword(line, "JOIN") {
                    Some(FormatQueryOwnerKind::Clause)
                } else {
                    None
                }
            }
            Self::ConditionNot => {
                let trimmed_upper = line.trim_start().to_ascii_uppercase();
                (starts_with_keyword_token(&trimmed_upper, "EXISTS")
                    || starts_with_keyword_token(&trimmed_upper, "IN")
                    || line_ends_with_keyword(line, "EXISTS")
                    || line_ends_with_keyword(line, "IN"))
                .then_some(FormatQueryOwnerKind::Condition)
            }
        }
    }

    pub(crate) fn line_completes(self, line: &str) -> bool {
        match self {
            Self::ReferenceOn => line_ends_with_keyword(line, "ON"),
            Self::JoinLike => {
                line_ends_with_keyword(line, "JOIN") || line_ends_with_keyword(line, "APPLY")
            }
            Self::ConditionNot => self.owner_kind_for_line(line).is_some(),
        }
    }

    pub(crate) fn line_can_continue(self, line: &str) -> bool {
        if self.line_completes(line) {
            return true;
        }

        let trimmed_upper = line.trim_start().to_ascii_uppercase();
        match self {
            Self::ReferenceOn => !starts_with_auto_format_owner_boundary(line),
            // JOIN/APPLY modifier chains are the only structural continuation
            // that can legally extend this pending owner family.
            // Everything else must terminate the pending header immediately.
            Self::JoinLike => starts_with_pending_query_owner_join_modifier(&trimmed_upper),
            Self::ConditionNot => self.line_completes(line),
        }
    }

    pub(crate) fn normalized_current_line_depth(
        self,
        current_depth: usize,
        query_base_depth: Option<usize>,
        condition_header_depth: Option<usize>,
    ) -> usize {
        match self {
            Self::ReferenceOn => current_depth,
            Self::JoinLike => query_base_depth.unwrap_or(current_depth),
            Self::ConditionNot => FormatQueryOwnerKind::Condition
                .header_depth_floor(query_base_depth, condition_header_depth)
                .map(|depth_floor| current_depth.max(depth_floor))
                .unwrap_or(current_depth),
        }
    }
}

fn starts_with_auto_format_owner_boundary_impl(
    line: &str,
    include_expression_query_owner: bool,
) -> bool {
    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return false;
    }

    let trimmed_upper = trimmed.to_ascii_uppercase();
    starts_with_format_layout_clause(&trimmed_upper)
        || starts_with_format_set_operator(&trimmed_upper)
        || format_query_owner_header_kind(trimmed).is_some()
        || format_query_owner_pending_header_kind(trimmed).is_some()
        || format_indented_paren_owner_header_kind(trimmed).is_some()
        || format_indented_paren_pending_header_kind(trimmed).is_some()
        || (include_expression_query_owner
            && line_ends_with_format_expression_query_owner_keyword(trimmed))
        || format_plsql_child_query_owner_kind(&trimmed_upper).is_some()
        || format_plsql_child_query_owner_pending_header_kind(trimmed).is_some()
}

/// Returns true when `line` begins a structural formatter boundary that must
/// terminate any pending split-owner/header continuation.
///
/// This intentionally covers every nested-owner family that can redirect the
/// indentation state machine onto a different stack:
/// - query-owner headers and pending fragments (`FROM`, `JOIN`, `LATERAL`,
///   `REFERENCE ... ON`, `NOT EXISTS`, ...)
/// - multiline owner headers and pending fragments (`WITHIN GROUP`,
///   `WINDOW ... AS`, `NESTED [PATH] ... COLUMNS`, ...)
/// - generic expression query owners (`CURSOR`, `MULTISET`)
/// - PL/SQL child-query owners (`BEGIN`, `CURSOR ... IS`, `OPEN ... FOR`, ...)
pub(crate) fn starts_with_auto_format_owner_boundary(line: &str) -> bool {
    starts_with_auto_format_owner_boundary_impl(line, true)
}

/// Returns true when `line` begins a shared structural owner/layout boundary
/// that should stop comment/header continuation, while still allowing generic
/// expression RHS lines such as `MULTISET` to remain operator continuations
/// when a caller needs that behavior.
pub(crate) fn starts_with_auto_format_owner_boundary_without_expression_owner(line: &str) -> bool {
    starts_with_auto_format_owner_boundary_impl(line, false)
}

pub(crate) fn format_plsql_child_query_owner_kind(
    text_upper: &str,
) -> Option<FormatPlsqlChildQueryOwnerKind> {
    let trimmed = text_upper.trim();

    if starts_with_keyword_token(trimmed, "BEGIN")
        || starts_with_keyword_token(trimmed, "EXCEPTION")
        || starts_with_keyword_token(trimmed, "ELSE")
        || starts_with_keyword_token(trimmed, "ELSIF")
        || starts_with_keyword_token(trimmed, "ELSEIF")
    {
        Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
    } else if starts_with_keyword_token(trimmed, "CURSOR")
        && (trimmed.contains(" IS") || trimmed.contains(" AS"))
    {
        Some(FormatPlsqlChildQueryOwnerKind::CursorDeclaration)
    } else if starts_with_keyword_token(trimmed, "OPEN") && trimmed.contains(" FOR") {
        Some(FormatPlsqlChildQueryOwnerKind::OpenCursorFor)
    } else {
        None
    }
}

impl PendingFormatPlsqlChildQueryOwnerHeaderKind {
    pub(crate) fn line_completes(self, line: &str) -> bool {
        let trimmed_upper = line.trim_start().to_ascii_uppercase();

        match self {
            Self::CursorDeclaration => {
                starts_with_keyword_token(&trimmed_upper, "IS")
                    || starts_with_keyword_token(&trimmed_upper, "AS")
                    || line_ends_with_keyword(line, "IS")
                    || line_ends_with_keyword(line, "AS")
            }
            Self::OpenCursorFor => {
                starts_with_keyword_token(&trimmed_upper, "FOR")
                    || line_ends_with_keyword(line, "FOR")
            }
        }
    }

    pub(crate) fn line_can_continue(self, line: &str) -> bool {
        if self.line_completes(line) {
            return true;
        }

        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.ends_with(';') {
            return false;
        }

        !starts_with_auto_format_owner_boundary(trimmed)
    }
}

pub(crate) fn format_plsql_child_query_owner_pending_header_kind(
    line: &str,
) -> Option<PendingFormatPlsqlChildQueryOwnerHeaderKind> {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();

    if starts_with_keyword_token(&trimmed_upper, "CURSOR")
        && format_plsql_child_query_owner_kind(&trimmed_upper)
            != Some(FormatPlsqlChildQueryOwnerKind::CursorDeclaration)
        && !line.trim_end().ends_with(';')
    {
        return Some(PendingFormatPlsqlChildQueryOwnerHeaderKind::CursorDeclaration);
    }

    (starts_with_keyword_token(&trimmed_upper, "OPEN")
        && format_plsql_child_query_owner_kind(&trimmed_upper)
            != Some(FormatPlsqlChildQueryOwnerKind::OpenCursorFor)
        && !line.trim_end().ends_with(';'))
    .then_some(PendingFormatPlsqlChildQueryOwnerHeaderKind::OpenCursorFor)
}

pub(crate) fn format_query_owner_pending_header_kind(
    line: &str,
) -> Option<PendingFormatQueryOwnerHeaderKind> {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();
    if starts_with_keyword_token(&trimmed_upper, "REFERENCE")
        && !line_ends_with_keyword(line, "ON")
        && !line_ends_with_open_paren_before_inline_comment(line)
    {
        return Some(PendingFormatQueryOwnerHeaderKind::ReferenceOn);
    }

    if line_ends_with_keyword(line, "NOT")
        && !line_ends_with_identifier_sequence(line, &["IS", "NOT"])
        && !line_ends_with_open_paren_before_inline_comment(line)
    {
        return Some(PendingFormatQueryOwnerHeaderKind::ConditionNot);
    }

    (starts_with_pending_query_owner_join_modifier(&trimmed_upper)
        && !line_ends_with_keyword(line, "JOIN")
        && !line_ends_with_keyword(line, "APPLY")
        && !line_ends_with_open_paren_before_inline_comment(line))
    .then_some(PendingFormatQueryOwnerHeaderKind::JoinLike)
}

fn starts_with_pending_query_owner_join_modifier(text_upper: &str) -> bool {
    starts_with_keyword_token(text_upper, "OUTER")
        || FORMAT_JOIN_MODIFIER_KEYWORDS
            .iter()
            .any(|keyword| starts_with_keyword_token(text_upper, keyword))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatIndentedParenOwnerKind {
    AnalyticOver,
    WithinGroup,
    Keep,
    ModelSubclause,
    Window,
    MatchRecognize,
    Pivot,
    Unpivot,
    StructuredColumns,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingFormatIndentedParenOwnerHeaderKind {
    WindowAs,
    WithinGroup,
    NestedPathColumns,
}

const FORMAT_ANALYTIC_OVER_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[
    &["PARTITION", "BY"],
    &["ORDER", "BY"],
    &["ROWS"],
    &["RANGE"],
    &["GROUPS"],
    &["EXCLUDE"],
];

const FORMAT_WITHIN_GROUP_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[&["ORDER", "BY"]];

const FORMAT_KEEP_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[
    &["DENSE_RANK", "FIRST", "ORDER", "BY"],
    &["DENSE_RANK", "LAST", "ORDER", "BY"],
];

const FORMAT_MODEL_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[
    &["PARTITION", "BY"],
    &["DIMENSION", "BY"],
    &["MEASURES"],
    &["REFERENCE"],
    &["RULES"],
    &["UPDATE"],
    &["UPSERT"],
    &["UPSERT", "ALL"],
    &["AUTOMATIC", "ORDER"],
    &["SEQUENTIAL", "ORDER"],
    &["ITERATE"],
    &["UNTIL"],
    &["IGNORE", "NAV"],
    &["KEEP", "NAV"],
    &["UNIQUE", "DIMENSION"],
    &["UNIQUE", "SINGLE", "REFERENCE"],
    &["RETURN", "ALL", "ROWS"],
    &["RETURN", "UPDATED", "ROWS"],
];

// MODEL rule modifiers may already be attached to a preceding `RULES` line.
// Phase 2 still needs to recognize them as owner-relative headers when users
// split them manually, but phase 1 should not aggressively force a new break
// between `RULES` and its modifiers.
const FORMAT_MODEL_PHASE1_EXCLUDED_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[
    &["UPDATE"],
    &["UPSERT"],
    &["UPSERT", "ALL"],
    &["AUTOMATIC", "ORDER"],
    &["SEQUENTIAL", "ORDER"],
    &["ITERATE"],
    &["UNTIL"],
];

const FORMAT_MATCH_RECOGNIZE_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[
    &["PARTITION", "BY"],
    &["ORDER", "BY"],
    &["MEASURES"],
    &["ONE", "ROW", "PER"],
    &["ALL", "ROWS", "PER"],
    &["WITH", "UNMATCHED", "ROWS"],
    &["WITHOUT", "UNMATCHED", "ROWS"],
    &["SHOW", "EMPTY", "MATCHES"],
    &["OMIT", "EMPTY", "MATCHES"],
    &["AFTER", "MATCH", "SKIP"],
    &["SUBSET"],
    &["PATTERN"],
    &["DEFINE"],
];

const FORMAT_PIVOT_UNPIVOT_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[&["FOR"]];

const FORMAT_STRUCTURED_COLUMNS_SUBCLAUSE_KEYWORD_SEQUENCES: &[&[&str]] = &[&["NESTED"]];

fn strip_keyword_token_prefix<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let trimmed = text.trim_start();
    starts_with_keyword_token(trimmed, keyword)
        .then(|| trimmed.get(keyword.len()..))
        .flatten()
}

fn starts_with_keyword_sequence(text_upper: &str, sequence: &[&str]) -> bool {
    if sequence.is_empty() {
        return false;
    }

    let mut remaining = text_upper.trim_start();
    for keyword in sequence {
        let Some(next) = strip_keyword_token_prefix(remaining, keyword) else {
            return false;
        };
        remaining = next.trim_start();
    }

    true
}

fn starts_with_any_keyword_sequence(text_upper: &str, sequences: &[&[&str]]) -> bool {
    sequences
        .iter()
        .any(|sequence| starts_with_keyword_sequence(text_upper, sequence))
}

fn leading_meaningful_words(line: &str, max_words: usize) -> Vec<&str> {
    let mut words = Vec::with_capacity(max_words);
    for skip_words in 0..max_words {
        let Some((word, _)) = next_meaningful_word(line, skip_words) else {
            break;
        };
        words.push(word);
    }
    words
}

fn leading_words_match_keyword_prefix(words: &[&str], sequence: &[&str]) -> usize {
    words
        .iter()
        .zip(sequence.iter())
        .take_while(|(word, expected)| word.eq_ignore_ascii_case(expected))
        .count()
}

fn leading_words_match_keyword_sequence(
    first_word: &str,
    second_word: Option<&str>,
    third_word: Option<&str>,
    sequence: &[&str],
) -> bool {
    match sequence {
        [first] => first_word.eq_ignore_ascii_case(first),
        [first, second] => {
            first_word.eq_ignore_ascii_case(first)
                && second_word.is_some_and(|word| word.eq_ignore_ascii_case(second))
        }
        [first, second, third] => {
            first_word.eq_ignore_ascii_case(first)
                && second_word.is_some_and(|word| word.eq_ignore_ascii_case(second))
                && third_word.is_some_and(|word| word.eq_ignore_ascii_case(third))
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatBodyHeaderContinuationState {
    Sequence {
        candidate_mask: u64,
        matched_words: usize,
    },
    Freeform,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FormatBodyHeaderLineState {
    pub(crate) is_header: bool,
    pub(crate) next_state: Option<FormatBodyHeaderContinuationState>,
}

impl FormatIndentedParenOwnerKind {
    fn body_header_sequences(self) -> &'static [&'static [&'static str]] {
        match self {
            Self::AnalyticOver | Self::Window => FORMAT_ANALYTIC_OVER_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::WithinGroup => FORMAT_WITHIN_GROUP_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::Keep => FORMAT_KEEP_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::ModelSubclause => FORMAT_MODEL_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::MatchRecognize => FORMAT_MATCH_RECOGNIZE_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::Pivot | Self::Unpivot => FORMAT_PIVOT_UNPIVOT_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::StructuredColumns => FORMAT_STRUCTURED_COLUMNS_SUBCLAUSE_KEYWORD_SEQUENCES,
        }
    }

    fn split_body_header_sequences(self) -> &'static [&'static [&'static str]] {
        match self {
            Self::AnalyticOver | Self::Window => &[
                &["PARTITION", "BY"],
                &["ORDER", "BY"],
                &["ROWS"],
                &["RANGE"],
                &["GROUPS"],
                &["EXCLUDE"],
            ],
            Self::WithinGroup => FORMAT_WITHIN_GROUP_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::Keep => FORMAT_KEEP_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::ModelSubclause => FORMAT_MODEL_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::MatchRecognize => &[
                &["PARTITION", "BY"],
                &["ORDER", "BY"],
                &["MEASURES"],
                &["ONE", "ROW", "PER", "MATCH"],
                &["ALL", "ROWS", "PER", "MATCH"],
                &["WITH", "UNMATCHED", "ROWS"],
                &["WITHOUT", "UNMATCHED", "ROWS"],
                &["SHOW", "EMPTY", "MATCHES"],
                &["OMIT", "EMPTY", "MATCHES"],
                &["AFTER", "MATCH", "SKIP"],
                &["SUBSET"],
                &["PATTERN"],
                &["DEFINE"],
            ],
            Self::Pivot | Self::Unpivot => &[&["FOR", "IN"]],
            Self::StructuredColumns => &[&["NESTED", "PATH", "COLUMNS"], &["NESTED", "COLUMNS"]],
        }
    }

    fn split_body_header_sequence_is_freeform(self, sequence_idx: usize) -> bool {
        match self {
            Self::AnalyticOver | Self::Window => (2..=5).contains(&sequence_idx),
            Self::MatchRecognize => sequence_idx == 9,
            Self::WithinGroup
            | Self::Keep
            | Self::ModelSubclause
            | Self::Pivot
            | Self::Unpivot
            | Self::StructuredColumns => false,
        }
    }

    fn freeform_body_header_continues(self, text_upper: &str) -> bool {
        let trimmed_upper = text_upper.trim_start();
        !trimmed_upper.is_empty()
            && !line_has_leading_significant_close_paren(trimmed_upper)
            && self
                .best_split_body_header_prefix_match(trimmed_upper)
                .is_none()
            && !starts_with_format_layout_clause(trimmed_upper)
            && !starts_with_format_set_operator(trimmed_upper)
            && !starts_with_keyword_token(trimmed_upper, "MODEL")
            && !starts_with_keyword_token(trimmed_upper, "MATCH_RECOGNIZE")
            && !starts_with_keyword_token(trimmed_upper, "WINDOW")
    }

    fn best_split_body_header_prefix_match(self, text_upper: &str) -> Option<(u64, usize)> {
        let sequences = self.split_body_header_sequences();
        let max_words = sequences
            .iter()
            .fold(0usize, |acc, sequence| acc.max(sequence.len()));
        if max_words == 0 {
            return None;
        }

        let words = leading_meaningful_words(text_upper.trim_start(), max_words);
        if words.is_empty() {
            return None;
        }

        let mut best_mask = 0u64;
        let mut best_matched_words = 0usize;
        for (idx, sequence) in sequences.iter().enumerate() {
            let matched_words = leading_words_match_keyword_prefix(&words, sequence);
            if matched_words == 0 {
                continue;
            }

            if matched_words > best_matched_words {
                best_mask = 1u64 << idx;
                best_matched_words = matched_words;
            } else if matched_words == best_matched_words {
                best_mask |= 1u64 << idx;
            }
        }

        (best_matched_words > 0).then_some((best_mask, best_matched_words))
    }

    fn best_split_body_header_continuation_match(
        self,
        candidate_mask: u64,
        matched_words: usize,
        text_upper: &str,
    ) -> Option<(u64, usize)> {
        let sequences = self.split_body_header_sequences();
        let max_remaining_words = sequences
            .iter()
            .enumerate()
            .filter(|(idx, _)| (candidate_mask & (1u64 << idx)) != 0)
            .fold(0usize, |acc, (_, sequence)| {
                acc.max(sequence.len().saturating_sub(matched_words))
            });
        if max_remaining_words == 0 {
            return None;
        }

        let words = leading_meaningful_words(text_upper.trim_start(), max_remaining_words);
        if words.is_empty() {
            return None;
        }

        let mut best_mask = 0u64;
        let mut best_total_matched_words = 0usize;
        for (idx, sequence) in sequences.iter().enumerate() {
            if (candidate_mask & (1u64 << idx)) == 0 || matched_words >= sequence.len() {
                continue;
            }

            let matched_suffix_words =
                leading_words_match_keyword_prefix(&words, &sequence[matched_words..]);
            if matched_suffix_words == 0 {
                continue;
            }

            let total_matched_words = matched_words.saturating_add(matched_suffix_words);
            if total_matched_words > best_total_matched_words {
                best_mask = 1u64 << idx;
                best_total_matched_words = total_matched_words;
            } else if total_matched_words == best_total_matched_words {
                best_mask |= 1u64 << idx;
            }
        }

        (best_total_matched_words > 0).then_some((best_mask, best_total_matched_words))
    }

    fn next_body_header_continuation_state(
        self,
        candidate_mask: u64,
        matched_words: usize,
    ) -> Option<FormatBodyHeaderContinuationState> {
        let mut incomplete_mask = 0u64;
        let mut completed_freeform_header = false;

        for (idx, sequence) in self.split_body_header_sequences().iter().enumerate() {
            if (candidate_mask & (1u64 << idx)) == 0 {
                continue;
            }

            if matched_words < sequence.len() {
                incomplete_mask |= 1u64 << idx;
            } else if self.split_body_header_sequence_is_freeform(idx) {
                completed_freeform_header = true;
            }
        }

        if completed_freeform_header {
            Some(FormatBodyHeaderContinuationState::Freeform)
        } else if incomplete_mask != 0 {
            Some(FormatBodyHeaderContinuationState::Sequence {
                candidate_mask: incomplete_mask,
                matched_words,
            })
        } else {
            None
        }
    }

    pub(crate) fn body_header_line_state(
        self,
        text_upper: &str,
        previous_state: Option<FormatBodyHeaderContinuationState>,
    ) -> FormatBodyHeaderLineState {
        let trimmed_upper = text_upper.trim_start();
        if trimmed_upper.is_empty() {
            return FormatBodyHeaderLineState::default();
        }

        if let Some(previous_state) = previous_state {
            match previous_state {
                FormatBodyHeaderContinuationState::Sequence {
                    candidate_mask,
                    matched_words,
                } => {
                    if let Some((matched_mask, total_matched_words)) = self
                        .best_split_body_header_continuation_match(
                            candidate_mask,
                            matched_words,
                            trimmed_upper,
                        )
                    {
                        return FormatBodyHeaderLineState {
                            is_header: true,
                            next_state: self.next_body_header_continuation_state(
                                matched_mask,
                                total_matched_words,
                            ),
                        };
                    }
                }
                FormatBodyHeaderContinuationState::Freeform => {
                    if self.freeform_body_header_continues(trimmed_upper) {
                        return FormatBodyHeaderLineState {
                            is_header: true,
                            next_state: Some(FormatBodyHeaderContinuationState::Freeform),
                        };
                    }
                }
            }
        }

        if let Some((matched_mask, matched_words)) =
            self.best_split_body_header_prefix_match(trimmed_upper)
        {
            return FormatBodyHeaderLineState {
                is_header: true,
                next_state: self.next_body_header_continuation_state(matched_mask, matched_words),
            };
        }

        FormatBodyHeaderLineState::default()
    }

    pub(crate) fn starts_body_header(self, text_upper: &str) -> bool {
        starts_with_any_keyword_sequence(text_upper, self.body_header_sequences())
    }

    #[cfg(test)]
    fn starts_contextual_body_header(
        self,
        text_upper: &str,
        previous_line_upper: Option<&str>,
    ) -> bool {
        let previous_state = previous_line_upper
            .and_then(|previous| self.body_header_line_state(previous, None).next_state);
        self.body_header_line_state(text_upper, previous_state)
            .is_header
    }

    pub(crate) fn starts_body_header_words(
        self,
        first_word: &str,
        second_word: Option<&str>,
        third_word: Option<&str>,
    ) -> bool {
        self.body_header_sequences().iter().any(|sequence| {
            leading_words_match_keyword_sequence(first_word, second_word, third_word, sequence)
        })
    }

    fn phase1_excluded_body_header_sequences(self) -> &'static [&'static [&'static str]] {
        match self {
            Self::MatchRecognize => &[&["PARTITION", "BY"], &["ORDER", "BY"]],
            Self::ModelSubclause => FORMAT_MODEL_PHASE1_EXCLUDED_SUBCLAUSE_KEYWORD_SEQUENCES,
            Self::AnalyticOver
            | Self::WithinGroup
            | Self::Keep
            | Self::Window
            | Self::Pivot
            | Self::Unpivot
            | Self::StructuredColumns => &[],
        }
    }

    pub(crate) fn starts_phase1_body_header_words(
        self,
        first_word: &str,
        second_word: Option<&str>,
        third_word: Option<&str>,
    ) -> bool {
        self.starts_body_header_words(first_word, second_word, third_word)
            && !self
                .phase1_excluded_body_header_sequences()
                .iter()
                .any(|sequence| {
                    leading_words_match_keyword_sequence(
                        first_word,
                        second_word,
                        third_word,
                        sequence,
                    )
                })
    }

    /// Returns the normalized owner depth that formatter phase 2 should use
    /// before pushing the multiline owner stack for this kind.
    pub(crate) fn formatter_owner_depth(
        self,
        fallback_depth: usize,
        query_base_depth: Option<usize>,
        general_paren_continuation_depth: Option<usize>,
    ) -> usize {
        match self {
            Self::Window | Self::MatchRecognize | Self::Pivot | Self::Unpivot => {
                query_base_depth.unwrap_or(fallback_depth)
            }
            Self::ModelSubclause => query_base_depth
                .map(|depth| depth.saturating_add(1))
                .unwrap_or(fallback_depth),
            Self::AnalyticOver | Self::WithinGroup | Self::Keep | Self::StructuredColumns => {
                general_paren_continuation_depth.unwrap_or(fallback_depth)
            }
        }
    }

    /// Returns the standard body depth relative to a multiline owner line.
    pub(crate) fn body_depth(self, owner_depth: usize) -> usize {
        owner_depth.saturating_add(1)
    }

    /// Returns the owner-relative depth for structural body headers that must
    /// snap back to the active owner's body depth after nested expressions.
    #[cfg(test)]
    pub(crate) fn formatter_body_header_depth(
        self,
        text_upper: &str,
        previous_line_upper: Option<&str>,
        owner_depth: usize,
    ) -> Option<usize> {
        self.starts_contextual_body_header(text_upper, previous_line_upper)
            .then(|| self.body_depth(owner_depth))
    }
}

impl PendingFormatIndentedParenOwnerHeaderKind {
    pub(crate) fn owner_kind(self) -> FormatIndentedParenOwnerKind {
        match self {
            Self::WindowAs => FormatIndentedParenOwnerKind::Window,
            Self::WithinGroup => FormatIndentedParenOwnerKind::WithinGroup,
            Self::NestedPathColumns => FormatIndentedParenOwnerKind::StructuredColumns,
        }
    }

    pub(crate) fn line_completes(self, line: &str) -> bool {
        let trimmed_upper = line.trim_start().to_ascii_uppercase();

        match self {
            Self::WindowAs => {
                starts_with_keyword_token(&trimmed_upper, "AS")
                    || line_ends_with_keyword(line, "AS")
            }
            Self::WithinGroup => {
                starts_with_keyword_token(&trimmed_upper, "GROUP")
                    || line_ends_with_keyword(line, "GROUP")
            }
            Self::NestedPathColumns => {
                starts_with_keyword_token(&trimmed_upper, "COLUMNS")
                    || line_ends_with_keyword(line, "COLUMNS")
            }
        }
    }

    pub(crate) fn line_can_continue(self, line: &str) -> bool {
        if self.line_completes(line) {
            return true;
        }

        !starts_with_auto_format_owner_boundary(line)
    }
}

pub(crate) fn format_indented_paren_pending_header_kind(
    line: &str,
) -> Option<PendingFormatIndentedParenOwnerHeaderKind> {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();
    if line_ends_with_keyword(line, "WITHIN")
        && !line_ends_with_keyword(line, "GROUP")
        && !line_ends_with_open_paren_before_inline_comment(line)
    {
        return Some(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup);
    }

    if starts_with_keyword_token(&trimmed_upper, "WINDOW")
        && !line_ends_with_keyword(line, "AS")
        && !line_ends_with_open_paren_before_inline_comment(line)
    {
        return Some(PendingFormatIndentedParenOwnerHeaderKind::WindowAs);
    }

    let nested_second_word = next_meaningful_word(line.trim_start(), 1).map(|(word, _)| word);
    (starts_with_keyword_token(&trimmed_upper, "NESTED")
        && !nested_second_word.is_some_and(|word| word.eq_ignore_ascii_case("TABLE"))
        && !line_ends_with_keyword(line, "COLUMNS")
        && !line_ends_with_open_paren_before_inline_comment(line))
    .then_some(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns)
}

pub(crate) fn format_indented_paren_owner_header_continues(
    pending_kind: FormatIndentedParenOwnerKind,
    line: &str,
) -> bool {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();

    match pending_kind {
        FormatIndentedParenOwnerKind::Pivot => starts_with_keyword_token(&trimmed_upper, "XML"),
        FormatIndentedParenOwnerKind::Unpivot => {
            starts_with_keyword_token(&trimmed_upper, "INCLUDE")
                || starts_with_keyword_token(&trimmed_upper, "EXCLUDE")
                || starts_with_keyword_token(&trimmed_upper, "NULLS")
        }
        FormatIndentedParenOwnerKind::AnalyticOver
        | FormatIndentedParenOwnerKind::WithinGroup
        | FormatIndentedParenOwnerKind::Keep
        | FormatIndentedParenOwnerKind::ModelSubclause
        | FormatIndentedParenOwnerKind::Window
        | FormatIndentedParenOwnerKind::MatchRecognize
        | FormatIndentedParenOwnerKind::StructuredColumns => false,
    }
}

/// Returns true when `text_upper` is the trailing token of a split MODEL
/// subclause header that can still own a following standalone `(` line.
///
/// This covers headers whose final token is not itself recognized as a direct
/// MODEL subclause start, such as `PARTITION / BY / (` or
/// `RULES / AUTOMATIC / ORDER / (`.
pub(crate) fn starts_with_format_model_multiline_owner_tail(text_upper: &str) -> bool {
    starts_with_keyword_token(text_upper, "BY")
        || starts_with_keyword_token(text_upper, "ORDER")
        || starts_with_keyword_token(text_upper, "ALL")
}

/// Returns the formatter query-owner kind when `line` ends on the owner header
/// token itself and the opening parenthesis starts on a later line.
pub(crate) fn format_query_owner_header_kind(line: &str) -> Option<FormatQueryOwnerKind> {
    if line_ends_with_format_direct_from_item_query_owner_keyword(line)
        || line_ends_with_keyword(line, "APPLY")
    {
        return Some(FormatQueryOwnerKind::FromItem);
    }

    let trimmed_upper = line.trim_start().to_ascii_uppercase();
    let starts_query_head =
        first_meaningful_word(&trimmed_upper).is_some_and(is_subquery_head_keyword);
    let starts_non_query_owner_subclause = starts_query_head
        || starts_with_keyword_token(&trimmed_upper, "MODEL")
        || starts_with_format_model_subclause(&trimmed_upper)
        || starts_with_keyword_token(&trimmed_upper, "MATCH_RECOGNIZE")
        || starts_with_format_match_recognize_subclause(&trimmed_upper)
        || starts_with_keyword_token(&trimmed_upper, "WINDOW");
    if starts_with_keyword_token(&trimmed_upper, "REFERENCE") && line_ends_with_keyword(line, "ON")
    {
        return Some(FormatQueryOwnerKind::Clause);
    }

    if line_ends_with_keyword(line, "FROM")
        || line_ends_with_keyword(line, "USING")
        || line_ends_with_keyword(line, "JOIN")
    {
        return Some(FormatQueryOwnerKind::Clause);
    }

    let starts_set_operator = starts_with_format_set_operator(&trimmed_upper);
    if !starts_non_query_owner_subclause
        && !starts_with_keyword_token(&trimmed_upper, "FOR")
        && !starts_set_operator
        && (line_ends_with_keyword(line, "IN")
            || line_ends_with_keyword(line, "EXISTS")
            || line_ends_with_keyword(line, "ANY")
            || line_ends_with_keyword(line, "SOME")
            || line_ends_with_keyword(line, "ALL"))
    {
        return Some(FormatQueryOwnerKind::Condition);
    }

    None
}

/// Returns the formatter query-owner kind when `line` owns a nested query body
/// through a trailing parenthesized group and should therefore participate in
/// the query-owner indentation stack.
pub(crate) fn format_query_owner_kind(line: &str) -> Option<FormatQueryOwnerKind> {
    line_ends_with_open_paren_before_inline_comment(line)
        .then(|| format_query_owner_header_kind(line))
        .flatten()
}

/// Detects split query-owner headers whose nested query starts on a later
/// standalone `(` line followed by a query head.
///
/// The formatter's analyzer and indentation phases both use this helper so the
/// owner-family classification stays consistent for nested wrapper/query-owner
/// constructs such as `CURSOR`, `MULTISET`, `LATERAL`, and `TABLE`.
pub(crate) fn split_query_owner_lookahead_kind(
    line: &str,
    next_code_is_standalone_open_paren: bool,
    head_trimmed_upper: Option<&str>,
) -> Option<SplitQueryOwnerLookaheadKind> {
    if line_ends_with_open_paren_before_inline_comment(line)
        || !next_code_is_standalone_open_paren
        || !head_trimmed_upper.is_some_and(line_starts_query_head)
    {
        return None;
    }

    if line_ends_with_format_expression_query_owner_keyword(line)
        && format_query_owner_header_kind(line).is_none()
        && format_indented_paren_owner_header_kind(line).is_none()
        && format_query_owner_pending_header_kind(line).is_none()
        && format_indented_paren_pending_header_kind(line).is_none()
    {
        return Some(SplitQueryOwnerLookaheadKind::GenericExpression);
    }

    (line_ends_with_format_direct_from_item_query_owner_keyword(line)
        && format_query_owner_pending_header_kind(line).is_none()
        && format_indented_paren_pending_header_kind(line).is_none())
    .then_some(SplitQueryOwnerLookaheadKind::DirectFromItem)
}

/// Returns true when `text_upper` starts with a MODEL subclause whose body is
/// owned by a trailing parenthesized block.
pub(crate) fn starts_with_format_model_subclause(text_upper: &str) -> bool {
    FormatIndentedParenOwnerKind::ModelSubclause.starts_body_header(text_upper)
}

pub(crate) fn starts_with_format_match_recognize_subclause(text_upper: &str) -> bool {
    FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header(text_upper)
}

/// Returns the structured formatter owner kind when `line` ends on the owner
/// header token itself and the parenthesized body starts on a later line.
pub(crate) fn format_indented_paren_owner_header_kind(
    line: &str,
) -> Option<FormatIndentedParenOwnerKind> {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();

    if line_ends_with_keyword(line, "OVER") {
        Some(FormatIndentedParenOwnerKind::AnalyticOver)
    } else if line_ends_with_identifier_sequence(line, &["WITHIN", "GROUP"]) {
        Some(FormatIndentedParenOwnerKind::WithinGroup)
    } else if line_ends_with_keyword(line, "KEEP") {
        Some(FormatIndentedParenOwnerKind::Keep)
    } else if starts_with_format_model_subclause(&trimmed_upper) {
        Some(FormatIndentedParenOwnerKind::ModelSubclause)
    } else if starts_with_keyword_token(&trimmed_upper, "WINDOW")
        && line_ends_with_keyword(line, "AS")
    {
        Some(FormatIndentedParenOwnerKind::Window)
    } else if line_ends_with_keyword(line, "MATCH_RECOGNIZE") {
        Some(FormatIndentedParenOwnerKind::MatchRecognize)
    } else if line_ends_with_pivot_owner(line) {
        Some(FormatIndentedParenOwnerKind::Pivot)
    } else if line_ends_with_unpivot_owner(line) {
        Some(FormatIndentedParenOwnerKind::Unpivot)
    } else if line_ends_with_keyword(line, "COLUMNS") {
        Some(FormatIndentedParenOwnerKind::StructuredColumns)
    } else {
        None
    }
}

fn line_trailing_identifiers_before_inline_comment(
    line: &str,
    max_identifiers: usize,
) -> (Option<u8>, Vec<&str>) {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_block_comment = false;
    let mut q_quote_end: Option<u8> = None;
    let mut last_significant_byte: Option<u8> = None;
    let mut trailing_identifiers = Vec::with_capacity(max_identifiers);

    while idx < bytes.len() {
        let current = bytes[idx];
        let next = bytes.get(idx.saturating_add(1)).copied();

        if in_block_comment {
            if current == b'*' && next == Some(b'/') {
                in_block_comment = false;
                idx = idx.saturating_add(2);
                continue;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if let Some(closing) = q_quote_end {
            if current == closing && next == Some(b'\'') {
                q_quote_end = None;
                idx = idx.saturating_add(2);
                continue;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_single_quote {
            if current == b'\'' {
                if next == Some(b'\'') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_single_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_double_quote {
            if current == b'"' {
                if next == Some(b'"') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_double_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if current == b'-' && next == Some(b'-') {
            break;
        }
        if current == b'/' && next == Some(b'*') {
            in_block_comment = true;
            idx = idx.saturating_add(2);
            continue;
        }
        if (current == b'q' || current == b'Q') && next == Some(b'\'') {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(2)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    idx = idx.saturating_add(3);
                    continue;
                }
            }
        }
        if (current == b'n' || current == b'N' || current == b'u' || current == b'U')
            && matches!(next, Some(b'q' | b'Q'))
            && bytes.get(idx.saturating_add(2)) == Some(&b'\'')
        {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(3)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    idx = idx.saturating_add(4);
                    continue;
                }
            }
        }
        if current == b'\'' {
            in_single_quote = true;
            idx = idx.saturating_add(1);
            continue;
        }
        if current == b'"' {
            in_double_quote = true;
            idx = idx.saturating_add(1);
            continue;
        }
        if current.is_ascii_whitespace() {
            idx = idx.saturating_add(1);
            continue;
        }

        if is_identifier_start_byte(current) {
            let start = idx;
            idx = idx.saturating_add(1);
            while idx < bytes.len() && is_identifier_byte(bytes[idx]) {
                idx = idx.saturating_add(1);
            }

            last_significant_byte = Some(b'a');
            if max_identifiers > 0 {
                if trailing_identifiers.len() == max_identifiers {
                    trailing_identifiers.remove(0);
                }
                if let Some(token) = line.get(start..idx) {
                    trailing_identifiers.push(token);
                }
            }
            continue;
        }

        last_significant_byte = Some(current);
        idx = idx.saturating_add(1);
    }

    (last_significant_byte, trailing_identifiers)
}

fn line_ends_with_identifier_sequence(line: &str, sequence: &[&str]) -> bool {
    if sequence.is_empty() {
        return true;
    }

    let (_, trailing_identifiers) =
        line_trailing_identifiers_before_inline_comment(line, sequence.len());
    trailing_identifiers.len() == sequence.len()
        && trailing_identifiers
            .iter()
            .zip(sequence.iter())
            .all(|(token, expected)| token.as_bytes().eq_ignore_ascii_case(expected.as_bytes()))
}

fn line_ends_with_pivot_owner(line: &str) -> bool {
    line_ends_with_identifier_sequence(line, &["PIVOT"])
        || line_ends_with_identifier_sequence(line, &["PIVOT", "XML"])
}

fn line_ends_with_unpivot_owner(line: &str) -> bool {
    line_ends_with_identifier_sequence(line, &["UNPIVOT"])
        || line_ends_with_identifier_sequence(line, &["UNPIVOT", "INCLUDE", "NULLS"])
        || line_ends_with_identifier_sequence(line, &["UNPIVOT", "EXCLUDE", "NULLS"])
}

pub(crate) fn line_ends_with_format_expression_query_owner_keyword(line: &str) -> bool {
    line_ends_with_identifier_sequence(line, &["CURSOR"])
        || line_ends_with_identifier_sequence(line, &["MULTISET"])
}

fn line_ends_with_format_table_from_item_query_owner_keyword(line: &str) -> bool {
    let trimmed_upper = line.trim_start().to_ascii_uppercase();
    (starts_with_keyword_token(&trimmed_upper, "TABLE")
        || line_ends_with_identifier_sequence(line, &["FROM", "TABLE"])
        || line_ends_with_identifier_sequence(line, &["JOIN", "TABLE"])
        || line_ends_with_identifier_sequence(line, &["APPLY", "TABLE"])
        || line_ends_with_identifier_sequence(line, &["USING", "TABLE"]))
        && line_ends_with_identifier_sequence(line, &["TABLE"])
}

pub(crate) fn line_ends_with_format_direct_from_item_query_owner_keyword(line: &str) -> bool {
    line_ends_with_identifier_sequence(line, &["LATERAL"])
        || line_ends_with_format_table_from_item_query_owner_keyword(line)
}

fn line_ends_with_keyword(line: &str, keyword: &str) -> bool {
    line_ends_with_identifier_sequence(line, &[keyword])
}

/// Returns the structured formatter block kind when a line owns a multiline
/// parenthesized body that should be tracked on a dedicated indentation stack.
pub(crate) fn format_indented_paren_owner_kind(line: &str) -> Option<FormatIndentedParenOwnerKind> {
    line_ends_with_open_paren_before_inline_comment(line)
        .then(|| format_indented_paren_owner_header_kind(line))
        .flatten()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SignificantParenEvent {
    Open,
    Close,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct SignificantParenProfile {
    pub(crate) events: Vec<SignificantParenEvent>,
    pub(crate) leading_close_count: usize,
}

/// Returns the ordered sequence of significant `(` / `)` tokens that appear on
/// `line`, excluding content inside comments or quoted literals. The profile
/// also tracks how many close-paren events occur before any other significant
/// token so indentation code can consume nested owner stacks in the same order
/// as the visible leading `)` sequence.
pub(crate) fn significant_paren_profile(line: &str) -> SignificantParenProfile {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    let mut profile = SignificantParenProfile::default();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_block_comment = false;
    let mut q_quote_end: Option<u8> = None;
    let mut still_in_leading_close_run = true;

    while idx < bytes.len() {
        let current = bytes[idx];
        let next = bytes.get(idx.saturating_add(1)).copied();

        if in_block_comment {
            if current == b'*' && next == Some(b'/') {
                in_block_comment = false;
                idx = idx.saturating_add(2);
                continue;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if let Some(closing) = q_quote_end {
            if current == closing && next == Some(b'\'') {
                q_quote_end = None;
                idx = idx.saturating_add(2);
                continue;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_single_quote {
            if current == b'\'' {
                if next == Some(b'\'') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_single_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if in_double_quote {
            if current == b'"' {
                if next == Some(b'"') {
                    idx = idx.saturating_add(2);
                    continue;
                }
                in_double_quote = false;
            }
            idx = idx.saturating_add(1);
            continue;
        }

        if current == b'-' && next == Some(b'-') {
            break;
        }
        if current == b'/' && next == Some(b'*') {
            in_block_comment = true;
            idx = idx.saturating_add(2);
            continue;
        }
        if (current == b'q' || current == b'Q') && next == Some(b'\'') {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(2)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    still_in_leading_close_run = false;
                    idx = idx.saturating_add(3);
                    continue;
                }
            }
        }
        if (current == b'n' || current == b'N' || current == b'u' || current == b'U')
            && matches!(next, Some(b'q' | b'Q'))
            && bytes.get(idx.saturating_add(2)) == Some(&b'\'')
        {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(3)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    q_quote_end = Some(q_quote_closing_byte(delimiter));
                    still_in_leading_close_run = false;
                    idx = idx.saturating_add(4);
                    continue;
                }
            }
        }
        if current == b'\'' {
            in_single_quote = true;
            still_in_leading_close_run = false;
            idx = idx.saturating_add(1);
            continue;
        }
        if current == b'"' {
            in_double_quote = true;
            still_in_leading_close_run = false;
            idx = idx.saturating_add(1);
            continue;
        }
        if current.is_ascii_whitespace() {
            idx = idx.saturating_add(1);
            continue;
        }

        match current {
            b'(' => {
                still_in_leading_close_run = false;
                profile.events.push(SignificantParenEvent::Open);
            }
            b')' => {
                if still_in_leading_close_run {
                    profile.leading_close_count = profile.leading_close_count.saturating_add(1);
                }
                profile.events.push(SignificantParenEvent::Close);
            }
            _ => {
                still_in_leading_close_run = false;
            }
        }

        idx = idx.saturating_add(1);
    }

    profile
}

pub(crate) fn significant_paren_depth_after_profile(
    mut depth: usize,
    paren_profile: &SignificantParenProfile,
) -> usize {
    for event in &paren_profile.events {
        match event {
            SignificantParenEvent::Open => {
                depth = depth.saturating_add(1);
            }
            SignificantParenEvent::Close => {
                depth = depth.saturating_sub(1);
            }
        }
    }

    depth
}

pub(crate) fn line_has_leading_significant_close_paren(line: &str) -> bool {
    significant_paren_profile(line).leading_close_count > 0
}

/// Returns the meaningful remainder of `line` after consuming any leading
/// close-paren run, including intervening whitespace or block comments.
pub(crate) fn trim_after_leading_close_parens(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    let mut consumed_close = false;

    loop {
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx = idx.saturating_add(1);
        }

        if idx + 1 < bytes.len() && bytes[idx] == b'/' && bytes[idx + 1] == b'*' {
            idx = idx.saturating_add(2);
            while idx + 1 < bytes.len() {
                if bytes[idx] == b'*' && bytes[idx + 1] == b'/' {
                    idx = idx.saturating_add(2);
                    break;
                }
                idx = idx.saturating_add(1);
            }
            continue;
        }

        if idx + 1 < bytes.len() && bytes[idx] == b'-' && bytes[idx + 1] == b'-' {
            return "";
        }

        if bytes.get(idx) == Some(&b')') {
            consumed_close = true;
            idx = idx.saturating_add(1);
            continue;
        }

        break;
    }

    if consumed_close {
        line.get(idx..).map(str::trim_start).unwrap_or("")
    } else {
        line.trim_start()
    }
}

/// Returns the structural classification tail for auto-formatting helpers.
///
/// Pure close lines stay as-is, but mixed leading-close lines consume the
/// close segment first so clause/header/continuation helpers can classify the
/// surviving structural tail (`) ORDER BY` -> `ORDER BY`, `) FOR UPDATE` ->
/// `FOR UPDATE`, `) AND EXISTS (` -> `AND EXISTS (`).
pub(crate) fn auto_format_structural_tail(line: &str) -> &str {
    let trimmed = line.trim_start();
    if trimmed.is_empty() {
        return trimmed;
    }

    if line_has_mixed_leading_close_continuation(trimmed) {
        trim_after_leading_close_parens(trimmed)
    } else {
        trimmed
    }
}

/// Returns true when the meaningful remainder of a leading-close line keeps
/// evaluating the same expression after the close has been consumed.
///
/// Examples:
/// - `) AND EXISTS (` -> true
/// - `), 0` -> true
/// - `)` / `),` / `) JOIN t` -> false
pub(crate) fn line_continues_expression_after_leading_close(line: &str) -> bool {
    let trimmed = trim_after_leading_close_parens(line);
    let Some(first_token) = first_meaningful_word(trimmed) else {
        return false;
    };

    match first_token {
        "," => {
            let remainder = trimmed.get(first_token.len()..).unwrap_or("");
            first_meaningful_word(remainder).is_some()
        }
        "+" | "-" | "*" | "/" | "%" | "^" | "=" | "<" | ">" | "<=" | ">=" | "<>" | "!=" | "||"
        | "|" => true,
        _ => matches!(
            first_token.to_ascii_uppercase().as_str(),
            "AND" | "OR" | "IS" | "IN" | "LIKE" | "BETWEEN" | "NOT"
        ),
    }
}

/// Returns true when a leading-close line must keep interpreting the remaining
/// tokens structurally after consuming the close segment.
///
/// This covers both expression continuations (`) AND ...`, `), value`) and
/// clause/query-header transitions such as `) ORDER BY ...` or
/// `) UPDATE demo ...`.
pub(crate) fn line_has_mixed_leading_close_continuation(line: &str) -> bool {
    if line_continues_expression_after_leading_close(line) {
        return true;
    }

    let trimmed = trim_after_leading_close_parens(line);
    let Some(first_token) = first_meaningful_word(trimmed) else {
        return false;
    };

    starts_with_auto_format_structural_continuation_boundary_without_expression_owner_impl(trimmed)
        || is_format_comment_continuation_keyword(first_token)
}

/// Returns true when a leading keyword should preserve the next line as a
/// continuation after a comment split.
pub(crate) fn is_format_comment_continuation_keyword(word: &str) -> bool {
    matches_keyword(word, FORMAT_LAYOUT_CLAUSE_START_KEYWORDS)
        || matches_keyword(word, FORMAT_COMMENT_CONTINUATION_KEYWORDS)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FormatTrailingMeaningfulToken<'a> {
    Word(&'a str),
    Symbol(&'a str),
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatTrailingContinuationOperatorKind {
    Keyword,
    Symbol,
}

fn push_format_trailing_meaningful_token<'a>(
    previous: &mut Option<FormatTrailingMeaningfulToken<'a>>,
    last: &mut Option<FormatTrailingMeaningfulToken<'a>>,
    token: FormatTrailingMeaningfulToken<'a>,
) {
    *previous = *last;
    *last = Some(token);
}

fn is_format_identifier_start_byte(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$' | b'#')
}

fn is_format_identifier_continue_byte(byte: u8) -> bool {
    is_format_identifier_start_byte(byte) || byte.is_ascii_digit()
}

fn trailing_meaningful_tokens_before_inline_comment(
    line: &str,
) -> (
    Option<FormatTrailingMeaningfulToken<'_>>,
    Option<FormatTrailingMeaningfulToken<'_>>,
) {
    let bytes = line.as_bytes();
    let mut idx = 0usize;
    let mut previous = None;
    let mut last = None;

    while idx < bytes.len() {
        let Some(remaining) = line.get(idx..) else {
            break;
        };

        if remaining.starts_with("--") {
            break;
        }

        if remaining.starts_with("/*") {
            let Some(block_end) = remaining.get(2..).and_then(|body| body.find("*/")) else {
                break;
            };
            idx = idx.saturating_add(block_end).saturating_add(4);
            continue;
        }

        let current = bytes[idx];
        let next = bytes.get(idx.saturating_add(1)).copied();

        if (current == b'q' || current == b'Q') && next == Some(b'\'') {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(2)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    let closing = q_quote_closing_byte(delimiter);
                    let mut local_idx = idx.saturating_add(3);
                    while local_idx < bytes.len() {
                        if bytes[local_idx] == closing
                            && bytes.get(local_idx.saturating_add(1)) == Some(&b'\'')
                        {
                            local_idx = local_idx.saturating_add(2);
                            break;
                        }
                        local_idx = local_idx.saturating_add(1);
                    }
                    push_format_trailing_meaningful_token(
                        &mut previous,
                        &mut last,
                        FormatTrailingMeaningfulToken::Other,
                    );
                    idx = local_idx.min(bytes.len());
                    continue;
                }
            }
        }

        if (current == b'n' || current == b'N' || current == b'u' || current == b'U')
            && matches!(next, Some(b'q' | b'Q'))
            && bytes.get(idx.saturating_add(2)) == Some(&b'\'')
        {
            if let Some(&delimiter) = bytes.get(idx.saturating_add(3)) {
                if is_valid_q_quote_delimiter_byte(delimiter) {
                    let closing = q_quote_closing_byte(delimiter);
                    let mut local_idx = idx.saturating_add(4);
                    while local_idx < bytes.len() {
                        if bytes[local_idx] == closing
                            && bytes.get(local_idx.saturating_add(1)) == Some(&b'\'')
                        {
                            local_idx = local_idx.saturating_add(2);
                            break;
                        }
                        local_idx = local_idx.saturating_add(1);
                    }
                    push_format_trailing_meaningful_token(
                        &mut previous,
                        &mut last,
                        FormatTrailingMeaningfulToken::Other,
                    );
                    idx = local_idx.min(bytes.len());
                    continue;
                }
            }
        }

        if current == b'\'' {
            let mut local_idx = idx.saturating_add(1);
            while local_idx < bytes.len() {
                if bytes[local_idx] == b'\'' {
                    if bytes.get(local_idx.saturating_add(1)) == Some(&b'\'') {
                        local_idx = local_idx.saturating_add(2);
                        continue;
                    }
                    local_idx = local_idx.saturating_add(1);
                    break;
                }
                local_idx = local_idx.saturating_add(1);
            }
            push_format_trailing_meaningful_token(
                &mut previous,
                &mut last,
                FormatTrailingMeaningfulToken::Other,
            );
            idx = local_idx.min(bytes.len());
            continue;
        }

        if current == b'"' {
            let mut local_idx = idx.saturating_add(1);
            while local_idx < bytes.len() {
                if bytes[local_idx] == b'"' {
                    local_idx = local_idx.saturating_add(1);
                    break;
                }
                local_idx = local_idx.saturating_add(1);
            }
            push_format_trailing_meaningful_token(
                &mut previous,
                &mut last,
                FormatTrailingMeaningfulToken::Other,
            );
            idx = local_idx.min(bytes.len());
            continue;
        }

        let Some(ch) = remaining.chars().next() else {
            break;
        };
        if ch.is_whitespace() {
            idx = idx.saturating_add(ch.len_utf8());
            continue;
        }

        if is_format_identifier_start_byte(current) {
            let start = idx;
            idx = idx.saturating_add(1);
            while idx < bytes.len() && is_format_identifier_continue_byte(bytes[idx]) {
                idx = idx.saturating_add(1);
            }
            push_format_trailing_meaningful_token(
                &mut previous,
                &mut last,
                line.get(start..idx)
                    .map(FormatTrailingMeaningfulToken::Word)
                    .unwrap_or(FormatTrailingMeaningfulToken::Other),
            );
            continue;
        }

        if current.is_ascii_digit() {
            idx = idx.saturating_add(1);
            while idx < bytes.len() {
                let next_byte = bytes[idx];
                if next_byte.is_ascii_whitespace()
                    || (next_byte == b'-' && bytes.get(idx.saturating_add(1)) == Some(&b'-'))
                    || (next_byte == b'/' && bytes.get(idx.saturating_add(1)) == Some(&b'*'))
                    || next_byte == b'\''
                    || next_byte == b'"'
                {
                    break;
                }
                if next_byte.is_ascii_punctuation() && next_byte != b'.' {
                    break;
                }
                idx = idx.saturating_add(1);
            }
            push_format_trailing_meaningful_token(
                &mut previous,
                &mut last,
                FormatTrailingMeaningfulToken::Other,
            );
            continue;
        }

        let start = idx;
        idx = idx.saturating_add(1);
        while idx < bytes.len() {
            let next_byte = bytes[idx];
            if next_byte.is_ascii_whitespace()
                || is_format_identifier_start_byte(next_byte)
                || next_byte.is_ascii_digit()
                || next_byte == b'\''
                || next_byte == b'"'
                || (next_byte == b'-' && bytes.get(idx.saturating_add(1)) == Some(&b'-'))
                || (next_byte == b'/' && bytes.get(idx.saturating_add(1)) == Some(&b'*'))
            {
                break;
            }
            idx = idx.saturating_add(1);
        }

        push_format_trailing_meaningful_token(
            &mut previous,
            &mut last,
            line.get(start..idx)
                .map(FormatTrailingMeaningfulToken::Symbol)
                .unwrap_or(FormatTrailingMeaningfulToken::Other),
        );
    }

    (previous, last)
}

fn format_trailing_continuation_operator_kind_from_token(
    previous: Option<FormatTrailingMeaningfulToken<'_>>,
    last: FormatTrailingMeaningfulToken<'_>,
) -> Option<FormatTrailingContinuationOperatorKind> {
    match last {
        FormatTrailingMeaningfulToken::Word(word) => matches!(
            word.to_ascii_uppercase().as_str(),
            "AND" | "OR" | "IN" | "IS" | "LIKE" | "BETWEEN" | "NOT" | "EXISTS"
        )
        .then_some(FormatTrailingContinuationOperatorKind::Keyword),
        FormatTrailingMeaningfulToken::Symbol(symbol) => match symbol {
            ":=" | "=" | "<" | ">" | "<=" | ">=" | "<>" | "!=" | "+" | "-" | "||" | "%" | "^"
            | "|" | "=>" => Some(FormatTrailingContinuationOperatorKind::Symbol),
            "*" => (!matches!(
                previous,
                Some(FormatTrailingMeaningfulToken::Word(word))
                    if word.eq_ignore_ascii_case("SELECT")
            ))
            .then_some(FormatTrailingContinuationOperatorKind::Symbol),
            "/" => previous
                .is_some()
                .then_some(FormatTrailingContinuationOperatorKind::Symbol),
            _ => None,
        },
        FormatTrailingMeaningfulToken::Other => None,
    }
}

/// Returns the trailing continuation-operator family for `line` after skipping
/// inline comments and quoted literals.
pub(crate) fn format_trailing_continuation_operator_kind(
    line: &str,
) -> Option<FormatTrailingContinuationOperatorKind> {
    let (previous, last) = trailing_meaningful_tokens_before_inline_comment(line);
    last.and_then(|last| format_trailing_continuation_operator_kind_from_token(previous, last))
}

/// Returns true when the last meaningful token before an inline comment or
/// end-of-line is an operator that keeps the next line as an RHS continuation.
pub(crate) fn line_has_trailing_format_continuation_operator(line: &str) -> bool {
    format_trailing_continuation_operator_kind(line).is_some()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FormatInlineCommentHeaderContinuationKind {
    SameDepth,
    OneDeeperThanQueryBase,
    OneDeeperThanCurrentLine,
}

/// Returns the continuation kind when an inline comment splits a clause or
/// subclause header and the next line should stay attached to that header.
pub(crate) fn format_inline_comment_header_continuation_kind(
    previous_word: Option<&str>,
    last_word: &str,
) -> Option<FormatInlineCommentHeaderContinuationKind> {
    let last_upper = last_word.to_ascii_uppercase();

    // Two-word combinations take priority over single-word matches so that
    // e.g. (LEFT, JOIN) is classified as OneDeeperThanCurrentLine rather
    // than falling through to JOIN's single-word OneDeeperThanQueryBase.
    let previous_upper = previous_word.map(str::to_ascii_uppercase);
    if let Some(ref previous_upper) = previous_upper {
        if matches!(
            (previous_upper.as_str(), last_upper.as_str()),
            ("WITHIN", "GROUP")
                | ("DENSE_RANK", "FIRST")
                | ("DENSE_RANK", "LAST")
                | ("FOR", "UPDATE")
                | ("GROUP", "BY")
                | ("ORDER", "BY")
                | ("PARTITION", "BY")
                | ("DIMENSION", "BY")
                | ("AFTER", "MATCH")
                | ("MATCH", "SKIP")
                | ("SKIP", "TO")
                | ("START", "WITH")
                | ("CONNECT", "BY")
        ) || (FORMAT_JOIN_MODIFIER_KEYWORDS.contains(&previous_upper.as_str())
            && last_upper == "JOIN")
            || (previous_upper == "SELECT"
                && matches!(last_upper.as_str(), "DISTINCT" | "UNIQUE" | "ALL"))
            || (matches!(previous_upper.as_str(), "BETWEEN" | "OF")
                && is_format_temporal_boundary_keyword(last_upper.as_str()))
        {
            let continuation_kind = if matches!(
                (previous_upper.as_str(), last_upper.as_str()),
                ("FOR", "UPDATE")
            ) {
                FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase
            } else {
                FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine
            };
            return Some(continuation_kind);
        }
    }

    if matches_keyword(
        last_upper.as_str(),
        FORMAT_INLINE_COMMENT_HEADER_SAME_DEPTH_KEYWORDS,
    ) {
        return Some(FormatInlineCommentHeaderContinuationKind::SameDepth);
    }
    if matches_keyword(
        last_upper.as_str(),
        FORMAT_INLINE_COMMENT_HEADER_QUERY_BASE_KEYWORDS,
    ) {
        return Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase);
    }
    if matches_keyword(
        last_upper.as_str(),
        FORMAT_INLINE_COMMENT_HEADER_CURRENT_LINE_KEYWORDS,
    ) {
        return Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine);
    }

    None
}

/// Returns the continuation kind for the leading structural header prefix on a
/// formatter/analyzer line.
///
/// This scans the leading meaningful words while they still form a keyword
/// prefix and reuses the inline-comment header classification so secondary
/// indentation can treat inline first-items (`SET col =`, `WHERE id =`,
/// `UPDATE SET col =`, ...) the same way as later list/body items.
pub(crate) fn format_leading_header_continuation_kind(
    line: &str,
) -> Option<FormatInlineCommentHeaderContinuationKind> {
    let words = leading_meaningful_words(line.trim_start(), 8);
    let mut continuation_kind = None;

    for idx in 0..words.len() {
        let word_upper = words[idx].to_ascii_uppercase();
        if !is_oracle_sql_keyword(&word_upper) {
            break;
        }

        let previous_word = if idx > 0 { Some(words[idx - 1]) } else { None };
        if let Some(kind) =
            format_inline_comment_header_continuation_kind(previous_word, words[idx])
        {
            continuation_kind = Some(kind);
        }
    }

    continuation_kind
}

/// Returns the shared structural continuation kind for a bare header line or
/// inline first-item prefix.
///
/// Both analyzer and formatter phase 2 use this helper so bare header splits
/// (`WHERE` -> next line operand, `FROM` -> next line item, `ON` -> next line
/// condition, ...) follow the same taxonomy as inline-comment header splits.
///
/// Structural carry still depends on the shared boundary helper above. Owner
/// header chains such as `WINDOW ... AS`, `PIVOT XML`, `RULES AUTOMATIC`, and
/// `MATCH_RECOGNIZE` subclauses are stopped there instead of being treated as
/// generic continuation consumers.
pub(crate) fn format_structural_header_continuation_kind(
    line: &str,
) -> Option<FormatInlineCommentHeaderContinuationKind> {
    format_leading_header_continuation_kind(auto_format_structural_tail(line))
}

pub(crate) fn format_bare_structural_header_continuation_kind(
    line: &str,
) -> Option<FormatInlineCommentHeaderContinuationKind> {
    let words_upper = leading_meaningful_words(auto_format_structural_tail(line), 8)
        .into_iter()
        .map(str::to_ascii_uppercase)
        .collect::<Vec<_>>();
    if words_upper.is_empty() || !words_upper.iter().all(|word| is_oracle_sql_keyword(word)) {
        return None;
    }

    let words = words_upper.iter().map(String::as_str).collect::<Vec<_>>();
    let exact = |expected: &[&str]| words.as_slice() == expected;

    if exact(&["WITH"]) {
        return Some(FormatInlineCommentHeaderContinuationKind::SameDepth);
    }

    if exact(&["SELECT"])
        || exact(&["VALUES"])
        || exact(&["SET"])
        || exact(&["RETURNING"])
        || exact(&["OFFSET"])
        || exact(&["FETCH"])
        || exact(&["LIMIT"])
        || exact(&["GROUP", "BY"])
        || exact(&["ORDER", "BY"])
        || exact(&["START", "WITH"])
        || exact(&["CONNECT", "BY"])
    {
        return Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine);
    }

    if exact(&["FROM"])
        || exact(&["WHERE"])
        || exact(&["HAVING"])
        || exact(&["USING"])
        || exact(&["INTO"])
        || exact(&["ON"])
        || exact(&["UNION"])
        || exact(&["INTERSECT"])
        || exact(&["MINUS"])
        || exact(&["EXCEPT"])
        || exact(&["QUALIFY"])
        || exact(&["SEARCH"])
        || exact(&["CYCLE"])
        || exact(&["FOR", "UPDATE"])
    {
        return Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase);
    }

    if words
        .last()
        .is_some_and(|last| matches!(*last, "JOIN" | "APPLY"))
        && starts_with_format_join_clause(&words.join(" "))
    {
        return Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine);
    }

    None
}

/// Returns true when a token starts a flashback/temporal boundary expression.
pub(crate) fn is_format_temporal_boundary_keyword(word: &str) -> bool {
    matches!(word.to_ascii_uppercase().as_str(), "TIMESTAMP" | "SCN")
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
        assert!(FORMAT_LAYOUT_CLAUSE_START_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_JOIN_MODIFIER_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_CONDITION_KEYWORDS
            .iter()
            .all(|keyword| is_oracle_sql_keyword(keyword)));
        assert!(FORMAT_COMMENT_CONTINUATION_KEYWORDS
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
    fn starts_with_format_layout_clause_tracks_extended_clause_heads() {
        assert!(starts_with_format_layout_clause("WINDOW w AS ("));
        assert!(starts_with_format_layout_clause(
            "QUALIFY ROW_NUMBER () = 1"
        ));
        assert!(starts_with_format_layout_clause("OFFSET 10 ROWS"));
        assert!(starts_with_format_layout_clause("FETCH FIRST 5 ROWS ONLY"));
        assert!(starts_with_format_layout_clause("LIMIT 50"));
        assert!(starts_with_format_layout_clause(
            "SEARCH DEPTH FIRST BY id SET ord"
        ));
        assert!(starts_with_format_layout_clause(
            "CYCLE id SET is_cycle TO 'Y' DEFAULT 'N'"
        ));
    }

    #[test]
    fn format_set_operator_helper_covers_except_and_other_set_operators() {
        assert!(is_format_set_operator_keyword("except"));
        assert_eq!(
            FormatSetOperatorKind::from_clause_start("EXCEPT DISTINCT"),
            Some(FormatSetOperatorKind::Except)
        );
        assert_eq!(
            FormatSetOperatorKind::from_clause_start("UNION ALL"),
            Some(FormatSetOperatorKind::Union)
        );
        assert_eq!(FormatSetOperatorKind::from_clause_start("ORDER BY"), None);
        assert!(starts_with_format_set_operator("INTERSECT"));
        assert!(!starts_with_format_set_operator("WHERE col IN"));
    }

    #[test]
    fn line_starts_query_head_recognizes_select_and_with_heads() {
        assert!(line_starts_query_head("SELECT empno"));
        assert!(line_starts_query_head("WITH bonus_cte AS"));
        assert!(!line_starts_query_head("ORDER BY empno"));
    }

    #[test]
    fn significant_paren_profile_tracks_event_order_and_leading_closes() {
        let profile = significant_paren_profile(") ) PARTITION BY (expr + (1))");

        assert_eq!(profile.leading_close_count, 2);
        assert_eq!(
            profile.events,
            vec![
                SignificantParenEvent::Close,
                SignificantParenEvent::Close,
                SignificantParenEvent::Open,
                SignificantParenEvent::Open,
                SignificantParenEvent::Close,
                SignificantParenEvent::Close,
            ]
        );
    }

    #[test]
    fn significant_paren_profile_ignores_comments_and_q_quotes() {
        let profile =
            significant_paren_profile("/* ) */ ) q'[ignored ( )]' /* ( */ (col) -- trailing )");

        assert_eq!(profile.leading_close_count, 1);
        assert_eq!(
            profile.events,
            vec![
                SignificantParenEvent::Close,
                SignificantParenEvent::Open,
                SignificantParenEvent::Close,
            ]
        );
    }

    #[test]
    fn significant_paren_depth_after_profile_tracks_nested_events() {
        let balanced = significant_paren_profile("(a + (b))");
        assert_eq!(significant_paren_depth_after_profile(0, &balanced), 0);

        let unbalanced = significant_paren_profile("(a + (b)");
        assert_eq!(significant_paren_depth_after_profile(1, &unbalanced), 2);
    }

    #[test]
    fn line_has_leading_significant_close_paren_ignores_comments_and_detects_real_closes() {
        assert!(line_has_leading_significant_close_paren(
            "/* leading note */ ) AND status = 'A'"
        ));
        assert!(line_has_leading_significant_close_paren(" ) "));
        assert!(!line_has_leading_significant_close_paren(
            "/* leading note */ deptno = 10"
        ));
        assert!(!line_has_leading_significant_close_paren("q'[)]'"));
    }

    #[test]
    fn trim_after_leading_close_parens_skips_comments_and_returns_remaining_code() {
        assert_eq!(
            trim_after_leading_close_parens(" ) /* nested */ EXCLUDE CURRENT ROW"),
            "EXCLUDE CURRENT ROW"
        );
        assert_eq!(trim_after_leading_close_parens(") -- comment only"), "");
        assert_eq!(
            trim_after_leading_close_parens("PARTITION BY deptno"),
            "PARTITION BY deptno"
        );
    }

    #[test]
    fn line_continues_expression_after_leading_close_distinguishes_mixed_and_pure_close_lines() {
        assert!(line_continues_expression_after_leading_close(
            ") AND EXISTS ("
        ));
        assert!(line_continues_expression_after_leading_close(
            ") /* gap */ IS NULL"
        ));
        assert!(line_continues_expression_after_leading_close("), 0"));

        assert!(!line_continues_expression_after_leading_close(")"));
        assert!(!line_continues_expression_after_leading_close("),"));
        assert!(!line_continues_expression_after_leading_close(
            ") JOIN dept d"
        ));
    }

    #[test]
    fn line_has_mixed_leading_close_continuation_covers_clause_and_query_head_reclassification() {
        assert!(line_has_mixed_leading_close_continuation(") AND EXISTS ("));
        assert!(line_has_mixed_leading_close_continuation(
            ") ORDER BY empno"
        ));
        assert!(line_has_mixed_leading_close_continuation(
            ") UPDATE demo SET flag = 'Y'"
        ));
        assert!(line_has_mixed_leading_close_continuation(
            ") FOR UPDATE NOWAIT"
        ));

        assert!(!line_has_mixed_leading_close_continuation(")"));
        assert!(!line_has_mixed_leading_close_continuation("),"));
        assert!(!line_has_mixed_leading_close_continuation(") bonus_view"));
        assert!(!line_has_mixed_leading_close_continuation(") THEN"));
    }

    #[test]
    fn format_comment_continuation_keywords_cover_clause_and_condition_heads() {
        assert!(is_format_comment_continuation_keyword("WINDOW"));
        assert!(is_format_comment_continuation_keyword("QUALIFY"));
        assert!(is_format_comment_continuation_keyword("FETCH"));
        assert!(is_format_comment_continuation_keyword("LIMIT"));
        assert!(is_format_comment_continuation_keyword("AND"));
        assert!(is_format_comment_continuation_keyword("JOIN"));
        assert!(!is_format_comment_continuation_keyword("DUAL"));
    }

    #[test]
    fn trailing_format_continuation_operator_helper_tracks_shared_keyword_and_symbol_taxonomy() {
        assert!(line_has_trailing_format_continuation_operator(
            "WHERE e.empno IS"
        ));
        assert!(line_has_trailing_format_continuation_operator(
            "WHERE e.ename LIKE"
        ));
        assert!(line_has_trailing_format_continuation_operator(
            "WHERE e.sal BETWEEN"
        ));
        assert!(line_has_trailing_format_continuation_operator(
            "WHERE e.empno ="
        ));
        assert!(line_has_trailing_format_continuation_operator(
            "pkg_lock.request =>"
        ));
        assert!(line_has_trailing_format_continuation_operator(
            "e.qty /* gap */ <="
        ));

        assert!(!line_has_trailing_format_continuation_operator("SELECT *"));
        assert!(!line_has_trailing_format_continuation_operator(
            "FOR UPDATE NOWAIT"
        ));
        assert!(!line_has_trailing_format_continuation_operator(
            "WHERE e.empno"
        ));
    }

    #[test]
    fn format_temporal_boundary_keywords_cover_timestamp_and_scn() {
        assert!(is_format_temporal_boundary_keyword("timestamp"));
        assert!(is_format_temporal_boundary_keyword("SCN"));
        assert!(!is_format_temporal_boundary_keyword("DATE"));
    }

    #[test]
    fn trailing_inline_comment_helpers_ignore_q_quotes_and_preserve_structural_tail() {
        assert_eq!(
            trailing_inline_comment_prefix("q'[-- kept literal]' -- real comment"),
            Some("q'[-- kept literal]' ")
        );
        assert!(line_ends_with_open_paren_before_inline_comment(
            "nq'<, )>' ( -- wrapper"
        ));
        assert!(line_is_standalone_open_paren_before_inline_comment(
            "( /* wrap */"
        ));
        assert!(line_ends_with_comma_before_inline_comment(
            "q'[/* literal */]' , -- trailing comma"
        ));
    }

    #[test]
    fn format_inline_comment_header_continuation_kind_tracks_clause_and_subclause_headers() {
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "WITH"),
            Some(FormatInlineCommentHeaderContinuationKind::SameDepth)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "LIMIT"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "MEASURES"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "REFERENCE"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "SUBSET"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "COLUMNS"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("ORDER"), "BY"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("FOR"), "UPDATE"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("MATCH"), "SKIP"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("WITHIN"), "GROUP"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("DENSE_RANK"), "LAST"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "KEEP"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("BETWEEN"), "TIMESTAMP"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("DENSE_RANK"), "VALUE"),
            None
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(Some("LEFT"), "JOIN"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "SEARCH"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "CYCLE"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_inline_comment_header_continuation_kind(None, "DUAL"),
            None
        );
    }

    #[test]
    fn format_leading_header_continuation_kind_tracks_inline_clause_item_prefixes() {
        assert_eq!(
            format_leading_header_continuation_kind("SET e.updated_at ="),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_leading_header_continuation_kind("UPDATE SET t.val ="),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_leading_header_continuation_kind("WHERE e.emp_id ="),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_leading_header_continuation_kind("ORDER BY salary DESC"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_leading_header_continuation_kind("e.job_title ="),
            None
        );
    }

    #[test]
    fn format_structural_header_continuation_kind_tracks_bare_headers_and_inline_prefixes() {
        assert_eq!(
            format_structural_header_continuation_kind("FROM"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_structural_header_continuation_kind("ON"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_structural_header_continuation_kind("WHERE e.emp_id ="),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_structural_header_continuation_kind("USING d,"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert_eq!(
            format_structural_header_continuation_kind("LEFT OUTER JOIN"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
    }

    #[test]
    fn structural_tail_helpers_consume_mixed_leading_close_before_reclassifying_headers() {
        assert_eq!(
            auto_format_structural_tail(") ORDER BY empno"),
            "ORDER BY empno"
        );
        assert_eq!(
            auto_format_structural_tail("/*c*/ ) FOR UPDATE"),
            "FOR UPDATE"
        );
        assert_eq!(
            format_bare_structural_header_continuation_kind(") ORDER BY"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_bare_structural_header_continuation_kind(") GROUP BY"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanCurrentLine)
        );
        assert_eq!(
            format_structural_header_continuation_kind(") FOR UPDATE -- lock"),
            Some(FormatInlineCommentHeaderContinuationKind::OneDeeperThanQueryBase)
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                ") ORDER BY empno"
            )
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                ") FOR UPDATE"
            )
        );
    }

    #[test]
    fn structural_continuation_boundary_helper_blocks_owner_relative_subclauses() {
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "AUTOMATIC ORDER"
            )
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "MEASURES match_number () AS match_no"
            )
        );
        assert_eq!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "emp e"
            ),
            false
        );
    }

    #[test]
    fn format_indented_paren_owner_kind_detects_analytic_over_without_matching_overlay() {
        assert_eq!(
            format_indented_paren_owner_kind("SUM (sal) OVER ( -- analytic window"),
            Some(FormatIndentedParenOwnerKind::AnalyticOver)
        );
        assert_eq!(
            format_indented_paren_owner_kind("REFERENCE ref_limits ON ( -- model reference"),
            Some(FormatIndentedParenOwnerKind::ModelSubclause)
        );
        assert_eq!(
            format_indented_paren_owner_kind("OVERLAY (name PLACING 'X' FROM 1 FOR 1)"),
            None
        );
    }

    #[test]
    fn format_indented_paren_owner_kind_covers_stack_managed_multiline_owners() {
        assert_eq!(
            format_indented_paren_owner_kind("WINDOW w_dept AS ( -- named window"),
            Some(FormatIndentedParenOwnerKind::Window)
        );
        assert_eq!(
            format_indented_paren_owner_kind("LISTAGG (ename, ', ') WITHIN GROUP ("),
            Some(FormatIndentedParenOwnerKind::WithinGroup)
        );
        assert_eq!(
            format_indented_paren_owner_kind("MAX (sal) KEEP ("),
            Some(FormatIndentedParenOwnerKind::Keep)
        );
        assert_eq!(
            format_indented_paren_owner_kind("MATCH_RECOGNIZE ( -- pattern input"),
            Some(FormatIndentedParenOwnerKind::MatchRecognize)
        );
        assert_eq!(
            format_indented_paren_owner_kind("FROM src PIVOT ( -- rotate rows"),
            Some(FormatIndentedParenOwnerKind::Pivot)
        );
        assert_eq!(
            format_indented_paren_owner_kind("FROM src UNPIVOT ( -- rotate cols"),
            Some(FormatIndentedParenOwnerKind::Unpivot)
        );
        assert_eq!(
            format_indented_paren_owner_kind("NESTED PATH '$.items[*]' COLUMNS ( -- nested"),
            Some(FormatIndentedParenOwnerKind::StructuredColumns)
        );
    }

    #[test]
    fn format_indented_paren_owner_kind_covers_modified_pivot_unpivot_owners() {
        assert_eq!(
            format_indented_paren_owner_kind("FROM src PIVOT XML ( -- xml pivot"),
            Some(FormatIndentedParenOwnerKind::Pivot)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("PIVOT XML"),
            Some(FormatIndentedParenOwnerKind::Pivot)
        );
        assert_eq!(
            format_indented_paren_owner_kind("FROM src UNPIVOT INCLUDE NULLS ( -- include nulls"),
            Some(FormatIndentedParenOwnerKind::Unpivot)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("UNPIVOT EXCLUDE NULLS"),
            Some(FormatIndentedParenOwnerKind::Unpivot)
        );
    }

    #[test]
    fn format_window_and_match_recognize_subclause_helpers_cover_extended_body_headers() {
        assert!(FormatIndentedParenOwnerKind::Window
            .starts_body_header("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"));
        assert!(FormatIndentedParenOwnerKind::Window
            .starts_body_header("RANGE BETWEEN 1 PRECEDING AND CURRENT ROW"));
        assert!(FormatIndentedParenOwnerKind::Window
            .starts_body_header("GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING"));
        assert!(FormatIndentedParenOwnerKind::Window.starts_body_header("EXCLUDE CURRENT ROW"));
        assert!(starts_with_format_match_recognize_subclause(
            "WITH UNMATCHED ROWS"
        ));
        assert!(starts_with_format_match_recognize_subclause(
            "WITHOUT UNMATCHED ROWS"
        ));
        assert!(starts_with_format_match_recognize_subclause(
            "SHOW EMPTY MATCHES"
        ));
        assert!(starts_with_format_match_recognize_subclause(
            "OMIT EMPTY MATCHES"
        ));
    }

    #[test]
    fn format_match_recognize_body_header_words_cover_clause_and_output_modifier_sequences() {
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header_words(
                "PARTITION",
                Some("BY"),
                None,
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header_words(
                "ORDER",
                Some("BY"),
                None,
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header_words(
                "WITH",
                Some("UNMATCHED"),
                Some("ROWS"),
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header_words(
                "AFTER",
                Some("MATCH"),
                Some("SKIP"),
            )
        );
        assert!(
            !FormatIndentedParenOwnerKind::MatchRecognize.starts_body_header_words(
                "ORDER",
                Some("SIBLINGS"),
                Some("BY"),
            )
        );
        assert!(
            !FormatIndentedParenOwnerKind::MatchRecognize.starts_phase1_body_header_words(
                "PARTITION",
                Some("BY"),
                None,
            )
        );
        assert!(
            !FormatIndentedParenOwnerKind::MatchRecognize.starts_phase1_body_header_words(
                "ORDER",
                Some("BY"),
                None,
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_phase1_body_header_words(
                "WITH",
                Some("UNMATCHED"),
                Some("ROWS"),
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::MatchRecognize.starts_phase1_body_header_words(
                "AFTER",
                Some("MATCH"),
                Some("SKIP"),
            )
        );
    }

    #[test]
    fn format_model_subclause_helper_covers_extended_body_headers() {
        assert!(starts_with_format_model_subclause("UPDATE"));
        assert!(starts_with_format_model_subclause("UPSERT"));
        assert!(starts_with_format_model_subclause("IGNORE NAV"));
        assert!(starts_with_format_model_subclause("KEEP NAV"));
        assert!(starts_with_format_model_subclause("UPSERT ALL"));
        assert!(starts_with_format_model_subclause("AUTOMATIC ORDER"));
        assert!(starts_with_format_model_subclause("SEQUENTIAL ORDER"));
        assert!(starts_with_format_model_subclause("ITERATE (3)"));
        assert!(starts_with_format_model_subclause("UNTIL ("));
        assert!(starts_with_format_model_subclause("UNIQUE DIMENSION"));
        assert!(starts_with_format_model_subclause(
            "UNIQUE SINGLE REFERENCE"
        ));
        assert!(starts_with_format_model_subclause("RETURN ALL ROWS"));
        assert!(starts_with_format_model_subclause("RETURN UPDATED ROWS"));
    }

    #[test]
    fn format_model_multiline_owner_tail_helper_covers_split_owner_header_tails() {
        assert!(starts_with_format_model_multiline_owner_tail("BY"));
        assert!(starts_with_format_model_multiline_owner_tail("ORDER"));
        assert!(starts_with_format_model_multiline_owner_tail("ALL"));
        assert!(!starts_with_format_model_multiline_owner_tail("ROWS"));
        assert!(!starts_with_format_model_multiline_owner_tail("ON"));
    }

    #[test]
    fn format_model_subclause_phase1_breaks_keep_rules_headers_but_not_rules_modifiers() {
        assert!(
            FormatIndentedParenOwnerKind::ModelSubclause.starts_phase1_body_header_words(
                "IGNORE",
                Some("NAV"),
                None,
            )
        );
        assert!(
            FormatIndentedParenOwnerKind::ModelSubclause.starts_phase1_body_header_words(
                "RETURN",
                Some("UPDATED"),
                Some("ROWS"),
            )
        );
        assert!(!FormatIndentedParenOwnerKind::ModelSubclause
            .starts_phase1_body_header_words("UPDATE", None, None,));
        assert!(
            !FormatIndentedParenOwnerKind::ModelSubclause.starts_phase1_body_header_words(
                "UPSERT",
                Some("ALL"),
                None,
            )
        );
        assert!(
            !FormatIndentedParenOwnerKind::ModelSubclause.starts_phase1_body_header_words(
                "SEQUENTIAL",
                Some("ORDER"),
                None,
            )
        );
        assert!(!FormatIndentedParenOwnerKind::ModelSubclause
            .starts_phase1_body_header_words("ITERATE", None, None,));
        assert!(!FormatIndentedParenOwnerKind::ModelSubclause
            .starts_phase1_body_header_words("UNTIL", None, None,));
    }

    #[test]
    fn format_pivot_unpivot_formatter_body_headers_treat_split_in_as_owner_relative_only_after_for()
    {
        assert_eq!(
            FormatIndentedParenOwnerKind::Pivot.formatter_body_header_depth(
                "IN (",
                Some("FOR deptno"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::Unpivot.formatter_body_header_depth(
                "IN (",
                Some("FOR dept_tag"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::Pivot.formatter_body_header_depth(
                "IN (",
                Some("WHEN deptno"),
                2,
            ),
            None
        );
    }

    #[test]
    fn format_owner_relative_body_header_depth_detects_split_continuations() {
        assert_eq!(
            FormatIndentedParenOwnerKind::Window.formatter_body_header_depth(
                "BY deptno",
                Some("PARTITION"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::AnalyticOver.formatter_body_header_depth(
                "UNBOUNDED PRECEDING",
                Some("ROWS"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::Window.formatter_body_header_depth(
                "CURRENT ROW",
                Some("EXCLUDE"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::Window.formatter_body_header_depth(
                "BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                Some("ROWS"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::MatchRecognize.formatter_body_header_depth(
                "TO NEXT ROW",
                Some("AFTER MATCH SKIP"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::MatchRecognize.formatter_body_header_depth(
                "MATCH",
                Some("ONE ROW PER"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::ModelSubclause.formatter_body_header_depth(
                "UPDATED ROWS",
                Some("RETURN"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::StructuredColumns.formatter_body_header_depth(
                "PATH '$.items[*]' COLUMNS (",
                Some("NESTED"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::StructuredColumns.formatter_body_header_depth(
                "COLUMNS (",
                Some("NESTED '$.items[*]'"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::WithinGroup.formatter_body_header_depth(
                "BY ename",
                Some("ORDER"),
                2,
            ),
            Some(3)
        );
        assert_eq!(
            FormatIndentedParenOwnerKind::Keep.formatter_body_header_depth(
                "LAST ORDER BY sal",
                Some("DENSE_RANK"),
                2,
            ),
            Some(3)
        );
    }

    #[test]
    fn format_body_header_line_state_tracks_multi_step_sequences_and_freeform_continuations() {
        let owner = FormatIndentedParenOwnerKind::ModelSubclause;
        let return_state = owner.body_header_line_state("RETURN", None);
        assert!(return_state.is_header);
        let updated_state = owner.body_header_line_state("UPDATED", return_state.next_state);
        assert!(updated_state.is_header);
        let rows_state = owner.body_header_line_state("ROWS", updated_state.next_state);
        assert!(rows_state.is_header);
        assert_eq!(rows_state.next_state, None);

        let owner = FormatIndentedParenOwnerKind::MatchRecognize;
        let one_state = owner.body_header_line_state("ONE", None);
        assert!(one_state.is_header);
        let row_per_state = owner.body_header_line_state("ROW PER", one_state.next_state);
        assert!(row_per_state.is_header);
        let match_state = owner.body_header_line_state("MATCH", row_per_state.next_state);
        assert!(match_state.is_header);
        assert_eq!(match_state.next_state, None);

        let after_state = owner.body_header_line_state("AFTER", None);
        assert!(after_state.is_header);
        let match_skip_state = owner.body_header_line_state("MATCH SKIP", after_state.next_state);
        assert!(match_skip_state.is_header);
        let to_state = owner.body_header_line_state("TO NEXT ROW", match_skip_state.next_state);
        assert!(to_state.is_header);
        assert_eq!(
            to_state.next_state,
            Some(FormatBodyHeaderContinuationState::Freeform)
        );

        let owner = FormatIndentedParenOwnerKind::Window;
        let rows_state = owner.body_header_line_state("ROWS", None);
        assert!(rows_state.is_header);
        assert_eq!(
            rows_state.next_state,
            Some(FormatBodyHeaderContinuationState::Freeform)
        );
        let between_state = owner.body_header_line_state("BETWEEN", rows_state.next_state);
        assert!(between_state.is_header);
        assert_eq!(
            between_state.next_state,
            Some(FormatBodyHeaderContinuationState::Freeform)
        );
        let bound_state = owner.body_header_line_state(
            "UNBOUNDED PRECEDING AND CURRENT ROW",
            between_state.next_state,
        );
        assert!(bound_state.is_header);
        assert_eq!(
            bound_state.next_state,
            Some(FormatBodyHeaderContinuationState::Freeform)
        );
        let order_state = owner.body_header_line_state("ORDER BY deptno", bound_state.next_state);
        assert!(order_state.is_header);
        assert_eq!(order_state.next_state, None);

        let owner = FormatIndentedParenOwnerKind::WithinGroup;
        let order_state = owner.body_header_line_state("ORDER", None);
        assert!(order_state.is_header);
        let by_state = owner.body_header_line_state("BY ename", order_state.next_state);
        assert!(by_state.is_header);
        assert_eq!(by_state.next_state, None);

        let owner = FormatIndentedParenOwnerKind::Keep;
        let dense_rank_state = owner.body_header_line_state("DENSE_RANK", None);
        assert!(dense_rank_state.is_header);
        let last_state = owner.body_header_line_state("LAST", dense_rank_state.next_state);
        assert!(last_state.is_header);
        let order_state = owner.body_header_line_state("ORDER", last_state.next_state);
        assert!(order_state.is_header);
        let by_state = owner.body_header_line_state("BY sal", order_state.next_state);
        assert!(by_state.is_header);
        assert_eq!(by_state.next_state, None);
    }

    #[test]
    fn format_indented_paren_pending_header_kind_tracks_nested_path_columns_chains() {
        assert_eq!(
            format_indented_paren_pending_header_kind("WITHIN"),
            Some(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup)
        );
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup.line_can_continue("GROUP"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup.line_completes("GROUP"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup
            .line_can_continue("GROUP (ORDER BY e.ename)"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WithinGroup
            .line_completes("GROUP (ORDER BY e.ename)"));
        assert_eq!(
            format_indented_paren_pending_header_kind("NESTED"),
            Some(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns)
        );
        assert_eq!(
            format_indented_paren_pending_header_kind("NESTED PATH '$.items[*]'"),
            Some(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns)
        );
        assert_eq!(
            format_indented_paren_pending_header_kind("NESTED '$.items[*]'"),
            Some(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns)
        );
        assert_eq!(
            format_indented_paren_pending_header_kind("NESTED TABLE"),
            None
        );
        assert!(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns
            .line_can_continue("PATH '$.items[*]'"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns
            .line_can_continue("'$.items[*]'"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns
            .line_can_continue("COLUMNS"));
        assert!(
            PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns.line_completes("COLUMNS")
        );
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WindowAs
            .line_can_continue("AS (PARTITION BY deptno)"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::WindowAs
            .line_completes("AS (PARTITION BY deptno)"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns
            .line_can_continue("COLUMNS (deptno PATH '$.deptno')"));
        assert!(PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns
            .line_completes("COLUMNS (deptno PATH '$.deptno')"));
    }

    #[test]
    fn pending_indented_paren_owner_headers_stop_on_other_structural_owner_boundaries() {
        let within_group = PendingFormatIndentedParenOwnerHeaderKind::WithinGroup;
        let window_as = PendingFormatIndentedParenOwnerHeaderKind::WindowAs;
        let nested_columns = PendingFormatIndentedParenOwnerHeaderKind::NestedPathColumns;

        assert!(!within_group.line_can_continue("LATERAL"));
        assert!(!within_group.line_can_continue("CURSOR"));
        assert!(!window_as.line_can_continue("REFERENCE ref_limits"));
        assert!(!window_as.line_can_continue("OPEN c_emp"));
        assert!(!nested_columns.line_can_continue("LEFT OUTER"));
        assert!(!nested_columns.line_can_continue("BEGIN"));
    }

    #[test]
    fn format_indented_paren_owner_header_kind_covers_split_owner_heads() {
        assert_eq!(
            format_indented_paren_owner_header_kind("SUM (sal) OVER"),
            Some(FormatIndentedParenOwnerKind::AnalyticOver)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("WITHIN GROUP"),
            Some(FormatIndentedParenOwnerKind::WithinGroup)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("KEEP"),
            Some(FormatIndentedParenOwnerKind::Keep)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("WINDOW w_dept AS"),
            Some(FormatIndentedParenOwnerKind::Window)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("MATCH_RECOGNIZE"),
            Some(FormatIndentedParenOwnerKind::MatchRecognize)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("FROM src PIVOT"),
            Some(FormatIndentedParenOwnerKind::Pivot)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("RULES UPDATE"),
            Some(FormatIndentedParenOwnerKind::ModelSubclause)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("NESTED PATH '$.items[*]' COLUMNS"),
            Some(FormatIndentedParenOwnerKind::StructuredColumns)
        );
        assert_eq!(
            format_indented_paren_owner_header_kind("NESTED '$.items[*]' COLUMNS"),
            Some(FormatIndentedParenOwnerKind::StructuredColumns)
        );
    }

    #[test]
    fn format_query_owner_kind_covers_nested_query_owner_heads() {
        assert_eq!(
            format_query_owner_kind("FROM ("),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_kind("LEFT OUTER JOIN ("),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_kind("USING ("),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_kind("LATERAL ("),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_kind("CROSS APPLY ("),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_kind("OUTER APPLY ("),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_kind("FROM TABLE ("),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_kind("LEFT JOIN TABLE ("),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_kind("MERGE INTO dst d USING ("),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_kind("REFERENCE ref_limits ON ("),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_kind("WHERE col IN ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_kind("WHERE EXISTS ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_kind("WHERE NOT EXISTS ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_kind("WHERE score = ANY ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_kind("WHERE score < SOME ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_kind("WHERE score > ALL ("),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(format_query_owner_kind("FOR rec IN ("), None);
        assert_eq!(format_query_owner_kind("FOR qtr IN ("), None);
    }

    #[test]
    fn format_query_owner_header_kind_covers_split_owner_heads() {
        assert_eq!(
            format_query_owner_header_kind("FROM"),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_header_kind("LEFT OUTER JOIN"),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_header_kind("CROSS APPLY"),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_header_kind("FROM TABLE"),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_header_kind("LEFT JOIN TABLE"),
            Some(FormatQueryOwnerKind::FromItem)
        );
        assert_eq!(
            format_query_owner_header_kind("REFERENCE ref_limits ON"),
            Some(FormatQueryOwnerKind::Clause)
        );
        assert_eq!(
            format_query_owner_header_kind("WHERE EXISTS"),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            format_query_owner_header_kind("WHERE score < SOME"),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(format_query_owner_header_kind("INSERT ALL"), None);
        assert_eq!(format_query_owner_header_kind("UPSERT ALL"), None);
        assert_eq!(format_query_owner_header_kind("RETURN ALL ROWS"), None);
        assert_eq!(format_query_owner_header_kind("ALL ROWS PER MATCH"), None);
        assert_eq!(format_query_owner_header_kind("FOR rec IN"), None);
        assert_eq!(format_query_owner_header_kind("UNION ALL"), None);
        assert_eq!(format_query_owner_header_kind("CREATE TABLE"), None);
    }

    #[test]
    fn format_query_owner_pending_header_kind_tracks_split_join_and_apply_modifier_chains() {
        let natural_pending =
            format_query_owner_pending_header_kind("NATURAL").expect("pending NATURAL owner");
        assert!(natural_pending.line_can_continue("LEFT"));
        assert!(!natural_pending.line_completes("LEFT"));

        let join_pending =
            format_query_owner_pending_header_kind("LEFT OUTER").expect("pending LEFT OUTER owner");
        assert!(join_pending.line_can_continue("JOIN"));
        assert!(join_pending.line_completes("JOIN"));

        let apply_pending =
            format_query_owner_pending_header_kind("CROSS").expect("pending CROSS owner");
        assert!(apply_pending.line_can_continue("APPLY"));
        assert!(apply_pending.line_completes("APPLY"));
    }

    #[test]
    fn format_query_owner_pending_header_kind_tracks_split_not_condition_owner_chain() {
        let not_pending = format_query_owner_pending_header_kind("NOT").expect("pending NOT owner");

        assert!(not_pending.line_can_continue("EXISTS"));
        assert!(not_pending.line_can_continue("IN"));
        assert!(not_pending.line_completes("EXISTS"));
        assert!(not_pending.line_completes("IN"));
        assert_eq!(
            not_pending.owner_kind_for_line("EXISTS"),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert_eq!(
            not_pending.owner_kind_for_line("IN"),
            Some(FormatQueryOwnerKind::Condition)
        );
        assert!(!not_pending.line_can_continue("SELECT"));
    }

    #[test]
    fn create_query_body_header_detects_view_and_ctas_headers() {
        assert!(line_is_create_query_body_header(
            "CREATE OR REPLACE VIEW v_demo AS"
        ));
        assert!(line_is_create_query_body_header(
            "CREATE MATERIALIZED VIEW mv_demo AS"
        ));
        assert!(line_is_create_query_body_header("CREATE TABLE t_demo AS"));
        assert!(line_is_create_query_body_header(
            "CREATE GLOBAL TEMPORARY TABLE t_demo AS"
        ));
        assert!(line_is_create_query_body_header(
            "CREATE PRIVATE TEMPORARY TABLE ora$ptt_demo AS"
        ));
        assert!(!line_is_create_query_body_header(
            "CREATE TABLE t_demo (id NUMBER)"
        ));
        assert!(!line_is_create_query_body_header(
            "CREATE PACKAGE pkg_demo AS"
        ));
    }

    #[test]
    fn split_query_owner_lookahead_kind_detects_safe_split_owner_headers() {
        assert_eq!(
            split_query_owner_lookahead_kind("CURSOR", true, Some("SELECT empno")),
            Some(SplitQueryOwnerLookaheadKind::GenericExpression)
        );
        assert_eq!(
            split_query_owner_lookahead_kind("MULTISET", true, Some("WITH bonus_cte AS")),
            Some(SplitQueryOwnerLookaheadKind::GenericExpression)
        );
        assert_eq!(
            split_query_owner_lookahead_kind("LATERAL", true, Some("SELECT empno")),
            Some(SplitQueryOwnerLookaheadKind::DirectFromItem)
        );
        assert_eq!(
            split_query_owner_lookahead_kind("FROM TABLE", true, Some("SELECT empno")),
            Some(SplitQueryOwnerLookaheadKind::DirectFromItem)
        );
        assert_eq!(
            split_query_owner_lookahead_kind("ORDER BY", true, Some("SELECT empno")),
            None
        );
        assert_eq!(
            split_query_owner_lookahead_kind("CURSOR", false, Some("SELECT empno")),
            None
        );
    }

    #[test]
    fn pending_reference_owner_header_stops_on_other_structural_owner_boundaries() {
        let reference_pending = PendingFormatQueryOwnerHeaderKind::ReferenceOn;

        assert!(!reference_pending.line_can_continue("LATERAL"));
        assert!(!reference_pending.line_can_continue("CURSOR"));
        assert!(!reference_pending.line_can_continue("BEGIN"));
        assert!(!reference_pending.line_can_continue("OPEN c_emp"));
    }

    #[test]
    fn format_expression_query_owner_keywords_cover_safe_split_expression_wrappers() {
        assert!(line_ends_with_format_expression_query_owner_keyword(
            "CURSOR"
        ));
        assert!(line_ends_with_format_expression_query_owner_keyword(
            "MULTISET -- wrapper comment"
        ));
        assert!(!line_ends_with_format_expression_query_owner_keyword(
            "ORDER BY"
        ));
        assert!(!line_ends_with_format_expression_query_owner_keyword(
            "SELECT e.empno,"
        ));
    }

    #[test]
    fn auto_format_owner_boundary_helpers_share_owner_taxonomy_but_can_exclude_expression_rhs() {
        assert!(starts_with_auto_format_owner_boundary("WHERE EXISTS"));
        assert!(starts_with_auto_format_owner_boundary("WINDOW w_dept AS"));
        assert!(starts_with_auto_format_owner_boundary("MULTISET"));
        assert!(starts_with_auto_format_owner_boundary("CURSOR"));

        assert!(starts_with_auto_format_owner_boundary_without_expression_owner("WHERE EXISTS"));
        assert!(
            starts_with_auto_format_owner_boundary_without_expression_owner("WINDOW w_dept AS")
        );
        assert!(!starts_with_auto_format_owner_boundary_without_expression_owner("MULTISET"));
        assert!(starts_with_auto_format_owner_boundary_without_expression_owner("CURSOR"));
    }

    #[test]
    fn structural_continuation_boundary_helper_tracks_join_condition_and_for_update() {
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "LEFT OUTER JOIN dept d"
            )
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "ON d.deptno = e.deptno"
            )
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "FOR UPDATE OF e.sal"
            )
        );
        assert!(
            starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "CALL pkg_do_work()"
            )
        );
        assert!(
            !starts_with_auto_format_structural_continuation_boundary_without_expression_owner(
                "MULTISET"
            )
        );
    }

    #[test]
    fn mixed_leading_close_continuation_recognizes_join_condition_boundary() {
        assert!(line_has_mixed_leading_close_continuation(
            ") ON d.deptno = e.deptno"
        ));
        assert!(line_has_mixed_leading_close_continuation(") JOIN dept d"));
    }

    #[test]
    fn format_direct_from_item_query_owner_keywords_cover_safe_split_lateral_headers() {
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "LATERAL"
        ));
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "LATERAL -- derived rows"
        ));
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "TABLE"
        ));
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "TABLE -- derived rows"
        ));
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "FROM TABLE"
        ));
        assert!(line_ends_with_format_direct_from_item_query_owner_keyword(
            "LEFT JOIN TABLE"
        ));
        assert!(!line_ends_with_format_direct_from_item_query_owner_keyword(
            "OUTER APPLY"
        ));
        assert!(!line_ends_with_format_direct_from_item_query_owner_keyword(
            "TABLE collection_expr"
        ));
        assert!(!line_ends_with_format_direct_from_item_query_owner_keyword(
            "CREATE TABLE"
        ));
        assert!(!line_ends_with_format_direct_from_item_query_owner_keyword(
            "JOIN"
        ));
    }

    #[test]
    fn format_plsql_child_query_owner_kind_covers_nested_control_and_query_owners() {
        assert_eq!(
            format_plsql_child_query_owner_kind("BEGIN"),
            Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("EXCEPTION"),
            Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("ELSE"),
            Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("ELSIF v_ready THEN"),
            Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("ELSEIF v_ready THEN"),
            Some(FormatPlsqlChildQueryOwnerKind::ControlBody)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("CURSOR c_emp IS"),
            Some(FormatPlsqlChildQueryOwnerKind::CursorDeclaration)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("CURSOR c_emp (p_deptno NUMBER) AS"),
            Some(FormatPlsqlChildQueryOwnerKind::CursorDeclaration)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("OPEN c_emp FOR"),
            Some(FormatPlsqlChildQueryOwnerKind::OpenCursorFor)
        );
        assert_eq!(
            format_plsql_child_query_owner_kind("OPEN c_emp FOR SELECT empno FROM emp"),
            Some(FormatPlsqlChildQueryOwnerKind::OpenCursorFor)
        );
        assert_eq!(format_plsql_child_query_owner_kind("LOOP"), None);
        assert_eq!(format_plsql_child_query_owner_kind("END IF;"), None);
        assert_eq!(format_plsql_child_query_owner_kind("FOR rec IN ("), None);
    }

    #[test]
    fn format_plsql_child_query_owner_pending_header_kind_tracks_split_cursor_and_open_headers() {
        assert_eq!(
            format_plsql_child_query_owner_pending_header_kind("CURSOR c_emp"),
            Some(PendingFormatPlsqlChildQueryOwnerHeaderKind::CursorDeclaration)
        );
        assert_eq!(
            format_plsql_child_query_owner_pending_header_kind("OPEN c_emp"),
            Some(PendingFormatPlsqlChildQueryOwnerHeaderKind::OpenCursorFor)
        );
        assert_eq!(
            format_plsql_child_query_owner_pending_header_kind("CURSOR c_emp IS"),
            None
        );
        assert_eq!(
            format_plsql_child_query_owner_pending_header_kind("OPEN c_emp FOR"),
            None
        );
    }

    #[test]
    fn pending_plsql_child_query_owner_header_kind_recognizes_split_completion_lines() {
        let cursor_kind = PendingFormatPlsqlChildQueryOwnerHeaderKind::CursorDeclaration;
        assert!(cursor_kind.line_completes("IS"));
        assert!(cursor_kind.line_completes(") AS"));
        assert!(cursor_kind.line_can_continue("(p_deptno NUMBER,"));
        assert!(cursor_kind.line_can_continue("p_ename VARCHAR2(30)"));
        assert!(!cursor_kind.line_can_continue("SELECT empno"));

        let open_kind = PendingFormatPlsqlChildQueryOwnerHeaderKind::OpenCursorFor;
        assert!(open_kind.line_completes("FOR"));
        assert!(open_kind.line_completes("FOR /* owner */"));
        assert!(open_kind.line_can_continue("c_emp"));
        assert!(!open_kind.line_can_continue("SELECT empno"));
    }

    #[test]
    fn format_query_owner_kind_depth_rules_keep_nested_heads_relative_to_owner_context() {
        assert_eq!(
            FormatQueryOwnerKind::Clause.header_depth_floor(Some(2), None),
            Some(2)
        );
        assert_eq!(
            FormatQueryOwnerKind::FromItem.header_depth_floor(Some(3), None),
            Some(3)
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.header_depth_floor(Some(2), None),
            Some(3)
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.header_depth_floor(Some(2), Some(5)),
            Some(5)
        );
        assert_eq!(
            FormatQueryOwnerKind::Clause.auto_format_child_query_owner_base_depth(2, Some(2)),
            2
        );
        assert_eq!(
            FormatQueryOwnerKind::FromItem.auto_format_child_query_owner_base_depth(3, Some(2)),
            3
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.auto_format_child_query_owner_base_depth(2, Some(2)),
            3
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.auto_format_child_query_owner_base_depth(3, Some(2)),
            3
        );
        assert_eq!(
            FormatQueryOwnerKind::Clause.formatter_child_query_head_depth(2, Some(2)),
            4
        );
        assert_eq!(
            FormatQueryOwnerKind::FromItem.formatter_child_query_head_depth(3, Some(2)),
            4
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.formatter_child_query_head_depth(2, Some(2)),
            4
        );
        assert_eq!(
            FormatQueryOwnerKind::Condition.formatter_child_query_head_depth(4, Some(2)),
            5
        );
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
            "CONSTRUCTOR",
            "FINAL",
            "INSTANTIABLE",
            "MAP",
            "OVERRIDING",
            "PACKAGE_BODY",
            "RECOGNIZE",
            "REPEATABLE",
            "SHARE",
            "SQLCODE",
            "SQLERRM",
            "STATIC",
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
    fn plsql_control_keyword_lookup_is_case_insensitive() {
        assert!(is_plsql_control_keyword("IF"));
        assert!(is_plsql_control_keyword("if"));
        assert!(is_plsql_control_keyword("Begin"));
        assert!(!is_plsql_control_keyword("iff"));
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
        assert!(is_with_plsql_declaration_keyword("PACKAGE"));
        assert!(is_with_plsql_declaration_keyword("type"));
        assert!(is_with_non_cte_query_head_keyword("xmlnamespaces"));
        assert!(with_plsql_declaration_starts_routine_body("PACKAGE"));
        assert!(!with_plsql_declaration_starts_routine_body("TYPE"));
        assert!(is_external_language_target_keyword("javascript"));
        assert!(is_external_language_target_keyword("mle"));
        assert!(is_external_language_target_keyword("rust"));
        assert!(is_external_language_clause_keyword("LANGUAGE"));
        assert!(is_external_language_clause_keyword("module"));
        assert!(is_external_language_clause_keyword("SIGNATURE"));
        assert!(is_external_language_clause_keyword("ENV"));
        assert!(is_external_language_clause_keyword("environment"));
        assert!(is_external_language_clause_keyword("AGENT"));
        assert!(is_external_language_clause_keyword("CREDENTIAL"));
        assert!(is_external_language_clause_keyword("IMPORT"));
        assert!(is_external_language_clause_keyword("parameters"));
        assert!(is_external_language_clause_keyword("EXPORT"));
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
