use crate::sql_text;
use crate::ui::theme;
use fltk::{browser::HoldBrowser, prelude::*, window::Window};
use std::any::Any;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::time::{Duration, Instant};

// SQL Keywords for autocomplete
pub const SQL_KEYWORDS: &[&str] = &[
    // Basic DML
    "SELECT",
    "FROM",
    "WHERE",
    "AND",
    "OR",
    "NOT",
    "IN",
    "BETWEEN",
    "LIKE",
    "IS",
    "NULL",
    "ORDER",
    "BY",
    "ASC",
    "DESC",
    "GROUP",
    "HAVING",
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "OUTER",
    "FULL",
    "CROSS",
    "ON",
    "AS",
    "DISTINCT",
    "ALL",
    "TOP",
    "LIMIT",
    "OFFSET",
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "CREATE",
    "REPLACE",
    "TABLE",
    "INDEX",
    "VIEW",
    "DROP",
    "ALTER",
    "ADD",
    "COLUMN",
    "CONSTRAINT",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "UNIQUE",
    "CHECK",
    "DEFAULT",
    "CASCADE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "BEGIN",
    "END",
    "DECLARE",
    "CURSOR",
    "FETCH",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "MINUS",
    "EXISTS",
    "ANY",
    "SOME",
    "WITH",
    "RECURSIVE",
    "OVER",
    "PARTITION",
    "ROW_NUMBER",
    "RANK",
    "DENSE_RANK",
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "COALESCE",
    "NVL",
    "DECODE",
    "TO_CHAR",
    "TO_DATE",
    "TO_NUMBER",
    "SYSDATE",
    "SYSTIMESTAMP",
    "ROWNUM",
    "ROWID",
    "DUAL",
    "SEQUENCE",
    "NEXTVAL",
    "CURRVAL",
    "TRIGGER",
    "PROCEDURE",
    "FUNCTION",
    "PACKAGE",
    "BODY",
    "RETURN",
    "RETURNS",
    "VARCHAR2",
    "NUMBER",
    "INTEGER",
    "DATE",
    "TIMESTAMP",
    "CLOB",
    "BLOB",
    "BOOLEAN",
    // MERGE statement
    "MERGE",
    "USING",
    "MATCHED",
    // PIVOT/UNPIVOT
    "PIVOT",
    "UNPIVOT",
    "FOR",
    // Hierarchical queries
    "CONNECT",
    "START",
    "PRIOR",
    "LEVEL",
    "SIBLINGS",
    "NOCYCLE",
    "CONNECT_BY_ROOT",
    "CONNECT_BY_ISLEAF",
    "CONNECT_BY_ISCYCLE",
    "SYS_CONNECT_BY_PATH",
    // Oracle 12c+ joins
    "LATERAL",
    "APPLY",
    // Analytic function window clauses
    "ROWS",
    "RANGE",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "NULLS",
    "FIRST",
    "LAST",
    "KEEP",
    "RESPECT",
    "IGNORE",
    "WITHIN",
    // Oracle 12c+ FETCH clause
    "NEXT",
    "ONLY",
    "PERCENT",
    "TIES",
    // PL/SQL keywords
    "EXCEPTION",
    "RAISE",
    "PRAGMA",
    "AUTONOMOUS_TRANSACTION",
    "EXCEPTION_INIT",
    "FORALL",
    "BULK",
    "COLLECT",
    "LOOP",
    "WHILE",
    "IF",
    "ELSIF",
    "INSERTING",
    "UPDATING",
    "DELETING",
    "EXIT",
    "CONTINUE",
    "GOTO",
    "SUBTYPE",
    "RECORD",
    "OBJECT",
    "VARRAY",
    "REF",
    "OUT",
    "NOCOPY",
    "DETERMINISTIC",
    "PIPELINED",
    "PARALLEL_ENABLE",
    "RESULT_CACHE",
    "ACCESSIBLE",
    "AUTHID",
    "CURRENT_USER",
    "DEFINER",
    "OPEN",
    "OTHERS",
    "REFCURSOR",
    "EACH",
    "ROW",
    "MEMBER",
    // Additional DML/DDL keywords
    "RETURNING",
    "FLASHBACK",
    "MODEL",
    "SAMPLE",
    "SEED",
    "SKIP",
    "LOCKED",
    "NOWAIT",
    "WAIT",
    "SCN",
    "VERSIONS",
    "PERIOD",
    "INCREMENT",
    "SIZE",
    "UNLIMITED",
    // Data types
    "BINARY_FLOAT",
    "BINARY_DOUBLE",
    "LONG",
    "RAW",
    "NCHAR",
    "NVARCHAR2",
    "NCLOB",
    "BFILE",
    "XMLTYPE",
    "ANYDATA",
    "ANYTYPE",
    "ANYDATASET",
    "PLS_INTEGER",
    "BINARY_INTEGER",
    "SIMPLE_INTEGER",
    // Miscellaneous
    "NATURAL",
    "EDITIONABLE",
    "NONEDITIONABLE",
    "SHARING",
    "METADATA",
    "NONE",
    "COMPILE",
    "DEBUG",
    "REUSE",
    "SETTINGS",
    "SPECIFICATION",
    "INVALIDATE",
    "ENABLE",
    "DISABLE",
    "SESSION",
    "SYSTEM",
    "CURRENT_SCHEMA",
    "CONTAINER",
    "EDITION",
    "TIME_ZONE",
    "TRACEFILE_IDENTIFIER",
    "SQL_TRACE",
    "EVENTS",
    "RESUMABLE",
    "ADVISE",
    "NOTHING",
    "DATABASE",
    "LINK",
    "COMMIT_WAIT",
    "COMMIT_LOGGING",
    "ISOLATION_LEVEL",
    "SERIALIZABLE",
    "READ",
    "WRITE",
    "STATISTICS_LEVEL",
    "OPTIMIZER_MODE",
    "VALIDATE",
    "NOVALIDATE",
    "RELY",
    "NORELY",
    "DEFERRABLE",
    "INITIALLY",
    "DEFERRED",
    "IMMEDIATE",
    "STORAGE",
    "TABLESPACE",
    "LOGGING",
    "NOLOGGING",
    "PARALLEL",
    "NOPARALLEL",
    "COMPRESS",
    "NOCOMPRESS",
    "CACHE",
    "NOCACHE",
    "MATERIALIZED",
    "REFRESH",
    "COMPLETE",
    "FAST",
    "FORCE",
    "NEVER",
    "SYNONYM",
    "PUBLIC",
    "PRIVATE",
    "ROLE",
    "PROFILE",
    "USER",
    "SCHEMA",
    "DIRECTORY",
    "LIBRARY",
    "TYPE",
    "CLUSTER",
    "CONTEXT",
    "DIMENSION",
    "JAVA",
    "SOURCE",
    "RESOURCE",
    "CLASS",
    // INTERVAL and datetime parts
    "INTERVAL",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "TO",
    "ZONE",
    "LOCAL",
    "TIME",
    "AT",
    // EXECUTE IMMEDIATE
    "EXECUTE",
    "EXEC",
    // MODEL clause keywords
    "MEASURES",
    "RULES",
    "AUTOMATIC",
    "SEQUENTIAL",
    "ITERATE",
    "UNTIL",
    "NAV",
    "SINGLE",
    "REFERENCE",
    "MAIN",
    "UPSERT",
    // XMLTABLE/JSON_TABLE keywords
    "COLUMNS",
    "PATH",
    "PASSING",
    "CONTENT",
    "DOCUMENT",
    "WELLFORMED",
    "ERROR",
    "EMPTY",
    "FORMAT",
    "WRAPPER",
    "CONDITIONAL",
    "UNCONDITIONAL",
    "ABSENT",
    "NESTED",
    // FLASHBACK keywords
    "RESTORE",
    "POINT",
    "BEFORE",
    "AFTER",
    "TRANSACTION",
    // FOR UPDATE keywords
    "OF",
    // Analytic keywords
    "NTILE",
    "LEAD",
    "LAG",
    "FIRST_VALUE",
    "LAST_VALUE",
    "NTH_VALUE",
    "LISTAGG",
    "CUME_DIST",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    // Additional Oracle keywords
    "PURGE",
    "RECYCLEBIN",
    "ARCHIVE",
    "NOARCHIVE",
    "INITRANS",
    "MAXTRANS",
    "PCTFREE",
    "PCTUSED",
    "ORGANIZATION",
    "HEAP",
    "IOT",
    "OVERFLOW",
    "INCLUDING",
    "MAPPING",
    "MONITORING",
    "NOMONITORING",
    "USAGE",
    "GLOBAL",
    "TEMPORARY",
    "PRESERVE",
    "EXTERNAL",
    "BITMAP",
    "NOSORT",
    "REVERSE",
    "INVISIBLE",
    "VISIBLE",
    // Constraint keywords
    "EXCEPTIONS",
    "NOEXCEPTIONS",
    "USING_NLS_COMP",
    // Partitioning
    "SUBPARTITION",
    "HASH",
    "LIST",
    "MAXVALUE",
    "MINVALUE",
    "LESS",
    "THAN",
    "STORE",
    // LOB keywords
    "LOB",
    "SECUREFILE",
    "BASICFILE",
    "DEDUPLICATE",
    "RETENTION",
    "FREEPOOLS",
    "PCTVERSION",
    "CHUNK",
    "ENABLE_STORAGE_IN_ROW",
    // Security
    "AUDIT",
    "NOAUDIT",
    "IDENTIFIED",
    "EXTERNALLY",
    "GLOBALLY",
    "PASSWORD",
    "EXPIRE",
    "ACCOUNT",
    "LOCK",
    "UNLOCK",
    // SQL*Plus commands
    "DEFINE",
    "UNDEFINE",
    "CLEAR",
    "BREAKS",
    "COMPUTES",
    "BREAK",
    "COMPUTE",
    "NEW_VALUE",
    "TRIMSPOOL",
    "COLSEP",
    "SPOOL",
    "APPEND",
    "OFF",
    "SHOW",
    "ERRORS",
    "PRINT",
    "WHENEVER",
    "SQLERROR",
    "OSERROR",
    "VAR",
    "VARIABLE",
    "SYS_REFCURSOR",
    "SERVEROUTPUT",
    "SYS_OUTPUT",
    "FEEDBACK",
    "VERIFY",
    "DESCRIBE",
    "TIMING",
    "COMMENT",
    "PIPE",
    "CLOSE",
    // Conditional compilation (PL/SQL)
    "PLSQL_CCFLAGS",
    "PLSQL_DEBUG",
    "PLSQL_OPTIMIZE_LEVEL",
    "PLSQL_CODE_TYPE",
    "PLSQL_WARNINGS",
    "PLSCOPE_SETTINGS",
    // Common ALTER SESSION parameters
    "NLS_DATE_FORMAT",
    "NLS_TIMESTAMP_FORMAT",
    "NLS_TIMESTAMP_TZ_FORMAT",
    "NLS_NUMERIC_CHARACTERS",
    "NLS_SORT",
    "NLS_COMP",
    "NLS_LANGUAGE",
    "NLS_TERRITORY",
    "NLS_CALENDAR",
    "NLS_CURRENCY",
    "NLS_ISO_CURRENCY",
    "NLS_LENGTH_SEMANTICS",
    "NLS_NCHAR_CONV_EXCP",
    "_ORACLE_SCRIPT",
];

// Oracle built-in functions
pub const ORACLE_FUNCTIONS: &[&str] = &[
    // Numeric functions
    "ABS",
    "ACOS",
    "ASIN",
    "ATAN",
    "ATAN2",
    "CEIL",
    "COS",
    "COSH",
    "EXP",
    "FLOOR",
    "LN",
    "LOG",
    "MOD",
    "NANVL",
    "POWER",
    "REMAINDER",
    "ROUND",
    "SIGN",
    "SIN",
    "SINH",
    "SQRT",
    "TAN",
    "TANH",
    "TRUNC",
    "WIDTH_BUCKET",
    "BITAND",
    // Character functions
    "ASCII",
    "ASCIISTR",
    "CHR",
    "COMPOSE",
    "CONCAT",
    "DECOMPOSE",
    "INITCAP",
    "INSTR",
    "LENGTH",
    "LOWER",
    "LPAD",
    "LTRIM",
    "NLS_INITCAP",
    "NLS_LOWER",
    "NLS_UPPER",
    "NLSSORT",
    "REGEXP_COUNT",
    "REGEXP_INSTR",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REPLACE",
    "REVERSE",
    "RPAD",
    "RTRIM",
    "SOUNDEX",
    "SUBSTR",
    "TRANSLATE",
    "TRIM",
    "UNISTR",
    "UPPER",
    // Date/Time functions
    "ADD_MONTHS",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "DBTIMEZONE",
    "EXTRACT",
    "FROM_TZ",
    "LAST_DAY",
    "LOCALTIMESTAMP",
    "MONTHS_BETWEEN",
    "NEW_TIME",
    "NEXT_DAY",
    "NUMTODSINTERVAL",
    "NUMTOYMINTERVAL",
    "SESSIONTIMEZONE",
    "SYSDATE",
    "SYSTIMESTAMP",
    "TO_CHAR",
    "TO_CLOB",
    "TO_DATE",
    "TO_DSINTERVAL",
    "TO_LOB",
    "TO_MULTI_BYTE",
    "TO_NUMBER",
    "TO_SINGLE_BYTE",
    "TO_TIMESTAMP",
    "TO_TIMESTAMP_TZ",
    "TO_YMINTERVAL",
    "TZ_OFFSET",
    // Conversion functions
    "BIN_TO_NUM",
    "CAST",
    "CHARTOROWID",
    "CONVERT",
    "HEXTORAW",
    "RAWTOHEX",
    "ROWIDTOCHAR",
    "TO_BINARY_DOUBLE",
    "TO_BINARY_FLOAT",
    "TO_BLOB",
    "TO_NCHAR",
    "TO_NCLOB",
    "VALIDATE_CONVERSION",
    // NULL-related functions
    "COALESCE",
    "DECODE",
    "DUMP",
    "EMPTY_BLOB",
    "EMPTY_CLOB",
    "GREATEST",
    "LEAST",
    "LNNVL",
    "NULLIF",
    "NVL",
    "NVL2",
    // Aggregate functions
    "AVG",
    "COLLECT",
    "CORR",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CUME_DIST",
    "DENSE_RANK",
    "FIRST",
    "GROUP_ID",
    "GROUPING",
    "GROUPING_ID",
    "LAST",
    "LISTAGG",
    "MAX",
    "MEDIAN",
    "MIN",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "RANK",
    "REGR_SLOPE",
    "REGR_INTERCEPT",
    "REGR_COUNT",
    "REGR_R2",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_SXX",
    "REGR_SYY",
    "REGR_SXY",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUM",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
    "APPROX_COUNT_DISTINCT",
    "APPROX_PERCENTILE",
    "ANY_VALUE",
    // Analytic functions
    "FIRST_VALUE",
    "LAG",
    "LAST_VALUE",
    "LEAD",
    "NTH_VALUE",
    "NTILE",
    "RATIO_TO_REPORT",
    "ROW_NUMBER",
    // JSON functions (Oracle 12c+)
    "JSON_VALUE",
    "JSON_QUERY",
    "JSON_TABLE",
    "JSON_OBJECT",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "JSON_EXISTS",
    "JSON_SERIALIZE",
    "JSON_MERGEPATCH",
    "JSON_TRANSFORM",
    "JSON_SCALAR",
    "JSON_EQUAL",
    // XML functions
    "XMLAGG",
    "XMLELEMENT",
    "XMLATTRIBUTES",
    "XMLFOREST",
    "XMLROOT",
    "XMLPARSE",
    "XMLSERIALIZE",
    "XMLQUERY",
    "XMLTABLE",
    "XMLEXISTS",
    "XMLSEQUENCE",
    "XMLCONCAT",
    "XMLCOMMENT",
    "XMLPI",
    "XMLCDATA",
    "XMLTRANSFORM",
    "XMLCAST",
    "XMLCOLATTVAL",
    "XPATH",
    "EXISTSNODE",
    "EXTRACTVALUE",
    "UPDATEXML",
    "DELETEXML",
    "INSERTXMLBEFORE",
    "INSERTCHILDXML",
    "INSERTCHILDXMLBEFORE",
    "INSERTCHILDXMLAFTER",
    "APPENDCHILDXML",
    // Object reference functions
    "BFILENAME",
    "CARDINALITY",
    "DEREF",
    "MAKE_REF",
    "REF",
    "REFTOHEX",
    "VALUE",
    "TREAT",
    // Environment and session functions
    "SYS_CONTEXT",
    "SYS_GUID",
    "SYS_TYPEID",
    "UID",
    "USER",
    "USERENV",
    "ORA_HASH",
    "STANDARD_HASH",
    "VSIZE",
    // Hierarchical functions
    "SYS_CONNECT_BY_PATH",
    // Miscellaneous functions
    "ORA_INVOKING_USER",
    "ORA_INVOKING_USERID",
    "PREDICTION",
    "PREDICTION_BOUNDS",
    "PREDICTION_COST",
    "PREDICTION_DETAILS",
    "PREDICTION_PROBABILITY",
    "PREDICTION_SET",
    "CLUSTER_ID",
    "CLUSTER_DETAILS",
    "CLUSTER_DISTANCE",
    "CLUSTER_PROBABILITY",
    "CLUSTER_SET",
    "FEATURE_COMPARE",
    "FEATURE_ID",
    "FEATURE_SET",
    "FEATURE_VALUE",
];

const MAX_SUGGESTIONS: usize = 50;

#[derive(Clone, PartialEq, Eq)]
struct NameEntry {
    name: String,
    upper: String,
}

impl NameEntry {
    fn new(name: String) -> Self {
        let upper = name.to_uppercase();
        Self { name, upper }
    }
}

#[derive(Clone)]
pub struct IntellisenseData {
    pub tables: Vec<String>,
    pub columns: HashMap<String, Vec<String>>, // table_name -> column_names
    pub columns_loading: HashSet<String>,
    column_loading_started_at: HashMap<String, Instant>,
    pub views: Vec<String>,
    pub procedures: Vec<String>,
    pub functions: Vec<String>,
    table_entries: Vec<NameEntry>,
    view_entries: Vec<NameEntry>,
    procedure_entries: Vec<NameEntry>,
    function_entries: Vec<NameEntry>,
    column_entries_by_table: HashMap<String, Vec<NameEntry>>,
    virtual_column_entries_by_table: HashMap<String, Vec<NameEntry>>,
    all_columns_entries: Vec<NameEntry>,
    all_columns_dirty: bool,
    relations_upper: HashSet<String>,
    /// Names of virtual tables (CTEs, subquery aliases) whose columns were
    /// derived from SQL text rather than database metadata.
    virtual_table_keys: HashSet<String>,
}

impl IntellisenseData {
    pub fn new() -> Self {
        Self {
            tables: Vec::new(),
            columns: HashMap::new(),
            columns_loading: HashSet::new(),
            column_loading_started_at: HashMap::new(),
            views: Vec::new(),
            procedures: Vec::new(),
            functions: Vec::new(),
            table_entries: Vec::new(),
            view_entries: Vec::new(),
            procedure_entries: Vec::new(),
            function_entries: Vec::new(),
            column_entries_by_table: HashMap::new(),
            virtual_column_entries_by_table: HashMap::new(),
            all_columns_entries: Vec::new(),
            all_columns_dirty: false,
            relations_upper: HashSet::new(),
            virtual_table_keys: HashSet::new(),
        }
    }

    pub fn get_suggestions(
        &mut self,
        prefix: &str,
        include_columns: bool,
        column_tables: Option<&[String]>,
        prefer_relations: bool,
        prefer_columns: bool,
    ) -> Vec<String> {
        self.ensure_base_indices();

        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();
        let relation_only = prefer_relations && prefix_upper.is_empty();
        let column_only = prefer_columns && prefix_upper.is_empty();

        let push_suggestion =
            |value: String, suggestions: &mut Vec<String>, seen: &mut HashSet<String>| {
                if !prefix_upper.is_empty() && value.to_uppercase() == prefix_upper {
                    return suggestions.len() >= MAX_SUGGESTIONS;
                }
                if suggestions.len() >= MAX_SUGGESTIONS {
                    return true;
                }
                if seen.insert(value.to_uppercase()) {
                    suggestions.push(value);
                }
                suggestions.len() >= MAX_SUGGESTIONS
            };

        if prefer_columns && include_columns {
            match column_tables {
                Some(tables) if !tables.is_empty() => {
                    for table in tables {
                        if let Some(cols) = self.column_entries_for_scope_table(table) {
                            if Self::push_entries(cols, &prefix_upper, &mut suggestions, &mut seen)
                            {
                                break;
                            }
                        }
                    }
                }
                _ => {
                    if !prefix_upper.is_empty() {
                        self.ensure_all_columns_entries();
                        let _ = Self::push_entries(
                            &self.all_columns_entries,
                            &prefix_upper,
                            &mut suggestions,
                            &mut seen,
                        );
                    }
                }
            }
            if column_only && !suggestions.is_empty() {
                Self::dedup_suggestions_case_insensitive(&mut suggestions);
                suggestions.truncate(MAX_SUGGESTIONS);
                return suggestions;
            }
        }

        // In table context, prioritize real relation names first.
        if prefer_relations {
            if Self::push_entries(
                &self.table_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                Self::dedup_suggestions_case_insensitive(&mut suggestions);
                return suggestions;
            }
            if Self::push_entries(
                &self.view_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                Self::dedup_suggestions_case_insensitive(&mut suggestions);
                return suggestions;
            }
            if relation_only && !suggestions.is_empty() {
                Self::dedup_suggestions_case_insensitive(&mut suggestions);
                suggestions.truncate(MAX_SUGGESTIONS);
                return suggestions;
            }
        }

        // Add SQL keywords
        for keyword in SQL_KEYWORDS {
            if keyword.starts_with(&prefix_upper) {
                if push_suggestion(keyword.to_string(), &mut suggestions, &mut seen) {
                    break;
                }
            }
        }

        // Add Oracle functions
        for func in ORACLE_FUNCTIONS {
            if func.starts_with(&prefix_upper) {
                if push_suggestion(format!("{}()", func), &mut suggestions, &mut seen) {
                    break;
                }
            }
        }

        // Add tables/views in non-table context after language items.
        if !prefer_relations {
        if Self::push_entries(
            &self.table_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            Self::dedup_suggestions_case_insensitive(&mut suggestions);
            return suggestions;
        }

        if Self::push_entries(
            &self.view_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            Self::dedup_suggestions_case_insensitive(&mut suggestions);
            return suggestions;
        }
        }

        // Add procedures
        if Self::push_entries(
            &self.procedure_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            Self::dedup_suggestions_case_insensitive(&mut suggestions);
            return suggestions;
        }

        // Add functions
        if Self::push_entries(
            &self.function_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            Self::dedup_suggestions_case_insensitive(&mut suggestions);
            return suggestions;
        }

        if include_columns && !prefer_columns {
            match column_tables {
                Some(tables) if !tables.is_empty() => {
                    for table in tables {
                        if let Some(cols) = self.column_entries_for_scope_table(table) {
                            if Self::push_entries(cols, &prefix_upper, &mut suggestions, &mut seen)
                            {
                                break;
                            }
                        }
                    }
                }
                _ => {
                    if !prefix_upper.is_empty() {
                        self.ensure_all_columns_entries();
                        let _ = Self::push_entries(
                            &self.all_columns_entries,
                            &prefix_upper,
                            &mut suggestions,
                            &mut seen,
                        );
                    }
                }
            }
        }

        Self::dedup_suggestions_case_insensitive(&mut suggestions);
        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
    }

    pub fn get_column_suggestions(
        &mut self,
        prefix: &str,
        column_tables: Option<&[String]>,
    ) -> Vec<String> {
        self.ensure_base_indices();

        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();

        match column_tables {
                Some(tables) if !tables.is_empty() => {
                    for table in tables {
                        if let Some(cols) = self.column_entries_for_scope_table(table) {
                            if Self::push_entries(cols, &prefix_upper, &mut suggestions, &mut seen) {
                                break;
                            }
                        }
                }
            }
            _ => {
                self.ensure_all_columns_entries();
                let _ = Self::push_entries(
                    &self.all_columns_entries,
                    &prefix_upper,
                    &mut suggestions,
                    &mut seen,
                );
            }
        }

        Self::dedup_suggestions_case_insensitive(&mut suggestions);
        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
    }

    fn column_entries_for_scope_table(&self, table: &str) -> Option<&[NameEntry]> {
        let key = table.to_uppercase();
        if let Some(entries) = self.column_entries_for_exact_key(&key) {
            return Some(entries);
        }
        if let Some(short) = key.rsplit('.').next() {
            if short != key {
                return self.column_entries_for_exact_key(short);
            }
        }
        None
    }

    fn column_entries_for_exact_key(&self, key: &str) -> Option<&[NameEntry]> {
        self.virtual_column_entries_by_table
            .get(key)
            .map(Vec::as_slice)
            .or_else(|| self.column_entries_by_table.get(key).map(Vec::as_slice))
    }

    #[allow(dead_code)]
    pub fn get_columns_for_table(&self, table_name: &str) -> Vec<String> {
        let key = table_name.to_uppercase();
        if let Some(columns) = self.virtual_column_entries_by_table.get(&key) {
            return columns.iter().map(|entry| entry.name.clone()).collect();
        }
        if let Some(columns) = self.columns.get(&key) {
            return columns.clone();
        }
        if let Some(short) = key.rsplit('.').next() {
            if short != key {
                if let Some(columns) = self.virtual_column_entries_by_table.get(short) {
                    return columns.iter().map(|entry| entry.name.clone()).collect();
                }
                if let Some(columns) = self.columns.get(short) {
                    return columns.clone();
                }
            }
        }
        Vec::new()
    }

    pub fn get_all_columns_for_highlighting(&self) -> Vec<String> {
        let mut seen: HashSet<String> = HashSet::new();
        let mut columns = Vec::new();

        for names in self.columns.values() {
            for name in names {
                let upper = name.to_uppercase();
                if seen.insert(upper) {
                    columns.push(name.clone());
                }
            }
        }

        for names in self.virtual_column_entries_by_table.values() {
            for entry in names {
                let upper = entry.upper.clone();
                if seen.insert(upper) {
                    columns.push(entry.name.clone());
                }
            }
        }

        columns
    }

    pub fn set_columns_for_table(&mut self, table_name: &str, columns: Vec<String>) {
        let key = table_name.to_uppercase();
        self.columns_loading.remove(&key);
        self.column_loading_started_at.remove(&key);
        self.columns.insert(key.clone(), columns.clone());
        self.column_entries_by_table
            .insert(key, Self::build_entries(&columns));
        self.all_columns_dirty = true;
    }

    pub fn mark_columns_loading(&mut self, table_name: &str) -> bool {
        let key = table_name.to_uppercase();
        if self.columns.contains_key(&key) || self.columns_loading.contains(&key) {
            return false;
        }
        self.columns_loading.insert(key.clone());
        self.column_loading_started_at.insert(key, Instant::now());
        true
    }

    pub fn clear_columns_loading(&mut self, table_name: &str) {
        let key = table_name.to_uppercase();
        self.columns_loading.remove(&key);
        self.column_loading_started_at.remove(&key);
    }

    pub fn clear_stale_columns_loading(&mut self, stale_after: Duration) -> usize {
        let now = Instant::now();
        let stale_keys: Vec<String> = self
            .columns_loading
            .iter()
            .filter(|key| {
                self.column_loading_started_at
                    .get(*key)
                    .map_or(true, |started| now.duration_since(*started) >= stale_after)
            })
            .cloned()
            .collect();

        let stale_count = stale_keys.len();
        for key in stale_keys {
            self.columns_loading.remove(&key);
            self.column_loading_started_at.remove(&key);
        }
        stale_count
    }

    pub fn is_known_relation(&self, name: &str) -> bool {
        let upper = name.to_uppercase();
        if !self.relations_upper.is_empty() {
            return self.relations_upper.contains(&upper);
        }
        self.tables.iter().any(|t| t.to_uppercase() == upper)
            || self.views.iter().any(|v| v.to_uppercase() == upper)
    }

    pub fn rebuild_indices(&mut self) {
        self.table_entries = Self::build_entries(&self.tables);
        self.view_entries = Self::build_entries(&self.views);
        self.procedure_entries = Self::build_entries(&self.procedures);
        self.function_entries = Self::build_entries(&self.functions);
        self.relations_upper = self
            .tables
            .iter()
            .chain(self.views.iter())
            .map(|name| name.to_uppercase())
            .collect();
        self.column_entries_by_table.clear();
        self.columns_loading.clear();
        self.column_loading_started_at.clear();
        for (table, columns) in &self.columns {
            self.column_entries_by_table
                .insert(table.clone(), Self::build_entries(columns));
        }
        self.virtual_column_entries_by_table.clear();
        self.all_columns_entries.clear();
        self.all_columns_dirty = true;
        self.virtual_table_keys.clear();
    }

    /// Clear previously inferred virtual table columns (CTEs, subquery aliases).
    /// These may be stale because the user edited the SQL text.
    #[allow(dead_code)]
    pub fn clear_virtual_tables(&mut self) {
        for key in self.virtual_table_keys.drain() {
            self.virtual_column_entries_by_table.remove(&key);
        }
        self.all_columns_dirty = true;
    }

    /// Register columns for a virtual table (CTE or subquery alias).
    /// These are text-derived columns, not loaded from the database.
    #[allow(dead_code)]
    pub fn set_virtual_table_columns(&mut self, name: &str, columns: Vec<String>) {
        let key = name.to_uppercase();
        self.virtual_column_entries_by_table
            .insert(key.clone(), Self::build_entries(&columns));
        self.virtual_table_keys.insert(key);
        self.all_columns_dirty = true;
    }

    /// Replace all inferred virtual table columns with the provided set.
    /// Only marks derived indices dirty when an actual change is detected.
    pub fn replace_virtual_table_columns(
        &mut self,
        virtual_columns: HashMap<String, Vec<String>>,
    ) {
        let mut changed = false;
        let next_keys: HashSet<String> = virtual_columns
            .keys()
            .map(|name| name.to_uppercase())
            .collect();

        let stale_keys: Vec<String> = self
            .virtual_table_keys
            .iter()
            .filter(|key| !next_keys.contains(*key))
            .cloned()
            .collect();
        for key in stale_keys {
            self.virtual_table_keys.remove(&key);
            if self.virtual_column_entries_by_table.remove(&key).is_some() {
                changed = true;
            }
        }

        for (name, columns) in virtual_columns {
            let key = name.to_uppercase();
            let entries = Self::build_entries(&columns);
            let is_same = self
                .virtual_column_entries_by_table
                .get(&key)
                .is_some_and(|existing| existing == &entries);
            if !is_same {
                self.virtual_column_entries_by_table.insert(key.clone(), entries);
                changed = true;
            }
            self.virtual_table_keys.insert(key);
        }

        if changed {
            self.all_columns_dirty = true;
        }
    }

    fn ensure_base_indices(&mut self) {
        if self.table_entries.len() != self.tables.len()
            || self.view_entries.len() != self.views.len()
            || self.procedure_entries.len() != self.procedures.len()
            || self.function_entries.len() != self.functions.len()
        {
            self.rebuild_indices();
        }
    }

    fn ensure_all_columns_entries(&mut self) {
        if !self.all_columns_dirty {
            return;
        }
        let mut all = Vec::new();
        for (table, entries) in &self.column_entries_by_table {
            if !self.virtual_column_entries_by_table.contains_key(table) {
                all.extend(entries.iter().cloned());
            }
        }
        for entries in self.virtual_column_entries_by_table.values() {
            all.extend(entries.iter().cloned());
        }
        all.sort_by(|a, b| a.upper.cmp(&b.upper).then_with(|| a.name.cmp(&b.name)));
        all.dedup_by(|a, b| a.upper == b.upper && a.name == b.name);
        self.all_columns_entries = all;
        self.all_columns_dirty = false;
    }

    fn build_entries(names: &[String]) -> Vec<NameEntry> {
        let mut entries: Vec<NameEntry> = names.iter().cloned().map(NameEntry::new).collect();
        entries.sort_by(|a, b| a.upper.cmp(&b.upper).then_with(|| a.name.cmp(&b.name)));
        entries
    }

    fn push_entries(
        entries: &[NameEntry],
        prefix_upper: &str,
        suggestions: &mut Vec<String>,
        seen: &mut HashSet<String>,
    ) -> bool {
        if suggestions.len() >= MAX_SUGGESTIONS || entries.is_empty() {
            return suggestions.len() >= MAX_SUGGESTIONS;
        }
        let start = entries.partition_point(|entry| entry.upper.as_str() < prefix_upper);
        for entry in entries.iter().skip(start) {
            if !entry.upper.starts_with(prefix_upper) {
                break;
            }
            if !prefix_upper.is_empty() && entry.upper == prefix_upper {
                continue;
            }
            if seen.insert(entry.upper.clone()) {
                suggestions.push(entry.name.clone());
                if suggestions.len() >= MAX_SUGGESTIONS {
                    return true;
                }
            }
        }
        suggestions.len() >= MAX_SUGGESTIONS
    }

    fn dedup_suggestions_case_insensitive(suggestions: &mut Vec<String>) {
        suggestions.sort_unstable();
        suggestions.dedup_by(|a, b| a.eq_ignore_ascii_case(b));
    }
}

impl Default for IntellisenseData {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct IntellisensePopup {
    window: Window,
    browser: HoldBrowser,
    suggestions: Rc<RefCell<Vec<String>>>,
    selected_callback: Rc<RefCell<Option<Box<dyn FnMut(String)>>>>,
    visible: Rc<RefCell<bool>>,
}

impl IntellisensePopup {
    fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
        if let Some(msg) = payload.downcast_ref::<&str>() {
            (*msg).to_string()
        } else if let Some(msg) = payload.downcast_ref::<String>() {
            msg.clone()
        } else {
            "unknown panic payload".to_string()
        }
    }

    fn log_callback_panic(context: &str, payload: &(dyn Any + Send)) {
        let panic_payload = Self::panic_payload_to_string(payload);
        crate::utils::logging::log_error(
            "intellisense_popup::callback",
            &format!("{context} panicked: {panic_payload}"),
        );
        eprintln!("{context} panicked: {panic_payload}");
    }

    pub fn new() -> Self {
        // Temporarily suspend current group to prevent popup window from being
        // added to the parent container (which causes layout issues)
        let current_group = fltk::group::Group::try_current();

        fltk::group::Group::set_current(None::<&fltk::group::Group>);

        let mut window = Window::default().with_size(320, 200);
        window.set_border(false);
        window.set_color(theme::panel_raised());
        window.make_modal(false);
        // Keep typing focus on the SQL editor even when popup is shown.
        // Override windows are not managed as focus-stealing toplevels.
        window.set_override();

        let mut browser = HoldBrowser::default().with_size(320, 200).with_pos(0, 0);
        browser.set_color(theme::panel_alt());
        browser.set_selection_color(theme::selection_strong());

        window.end();

        // Restore current group
        if let Some(ref group) = current_group {
            fltk::group::Group::set_current(Some(group));
        }

        let suggestions = Rc::new(RefCell::new(Vec::new()));
        let selected_callback: Rc<RefCell<Option<Box<dyn FnMut(String)>>>> =
            Rc::new(RefCell::new(None));
        let visible = Rc::new(RefCell::new(false));

        window.hide();

        let mut popup = Self {
            window,
            browser,
            suggestions,
            selected_callback,
            visible,
        };

        popup.setup_callbacks();
        popup
    }

    fn setup_callbacks(&mut self) {
        // Browser click callback - handle mouse selection
        let suggestions = self.suggestions.clone();
        let callback = self.selected_callback.clone();
        let mut window = self.window.clone();
        let visible = self.visible.clone();

        self.browser.set_callback(move |b| {
            let selected = b.value();
            if selected > 0 {
                // First, get the text with suggestions borrow, then release it
                let text = {
                    let suggestions = suggestions.borrow();
                    suggestions.get((selected - 1) as usize).cloned()
                };
                if let Some(text) = text {
                    // Take the callback out, call it, then put it back
                    // This ensures the RefCell is not borrowed during callback execution
                    let cb_opt = callback.borrow_mut().take();
                    if let Some(mut cb) = cb_opt {
                        let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(text)));
                        *callback.borrow_mut() = Some(cb);
                        if let Err(payload) = call_result {
                            Self::log_callback_panic(
                                "intellisense selected callback",
                                payload.as_ref(),
                            );
                        }
                    }
                    window.hide();
                    *visible.borrow_mut() = false;
                }
            }
        });

        // Note: Keyboard events are handled by the editor, not by this popup window.
        // This is because the editor retains focus while the popup is visible,
        // so key events go to the editor's handle(), not the popup's handle().
    }

    pub fn show_suggestions(&mut self, suggestions: Vec<String>, x: i32, y: i32) {
        if suggestions.is_empty() {
            self.hide();
            return;
        }

        let suggestion_count = suggestions.len();
        let browser_lines: Vec<String> = suggestions
            .iter()
            .map(|suggestion| format!("@C255 {}", suggestion))
            .collect();
        self.browser.clear();
        *self.suggestions.borrow_mut() = suggestions;
        for line in &browser_lines {
            self.browser.add(line);
        }

        // Select first item
        if suggestion_count > 0 {
            self.browser.select(1);
        }

        // Calculate popup size
        let height = (suggestion_count.min(10) * 20 + 10) as i32;
        self.window.set_size(320, height);
        self.browser.set_size(320, height);

        self.window.set_pos(x, y);
        if !self.window.shown() {
            self.window.show();
        }
        *self.visible.borrow_mut() = true;
    }

    pub fn hide(&mut self) {
        self.window.hide();
        self.window.resize(0, 0, 0, 0);
        *self.visible.borrow_mut() = false;
    }

    pub fn clear_for_close(&mut self) {
        self.hide();
        self.browser.clear();
        self.suggestions.borrow_mut().clear();
        *self.selected_callback.borrow_mut() = None;
    }

    pub fn is_visible(&self) -> bool {
        *self.visible.borrow()
    }

    pub fn contains_point(&self, x: i32, y: i32) -> bool {
        let left = self.window.x();
        let top = self.window.y();
        let right = left + self.window.w();
        let bottom = top + self.window.h();
        x >= left && x < right && y >= top && y < bottom
    }

    pub fn set_selected_callback<F>(&mut self, callback: F)
    where
        F: FnMut(String) + 'static,
    {
        *self.selected_callback.borrow_mut() = Some(Box::new(callback));
    }

    pub fn select_next(&mut self) {
        let current = self.browser.value();
        let count = self.browser.size();
        if current < count {
            self.browser.select(current + 1);
        }
    }

    pub fn select_prev(&mut self) {
        let current = self.browser.value();
        if current > 1 {
            self.browser.select(current - 1);
        }
    }

    pub fn get_selected(&self) -> Option<String> {
        let selected = self.browser.value();
        if selected > 0 {
            self.suggestions
                .borrow()
                .get((selected - 1) as usize)
                .cloned()
        } else {
            None
        }
    }
}

impl Default for IntellisensePopup {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function to extract the current word at cursor position (Unicode-aware).
// cursor_pos is a byte offset from FLTK TextBuffer.
pub fn get_word_at_cursor(text: &str, cursor_pos: usize) -> (String, usize, usize) {
    if text.is_empty() || cursor_pos == 0 {
        return (String::new(), 0, 0);
    }

    let mut pos = cursor_pos.min(text.len());
    while pos > 0 && !text.is_char_boundary(pos) {
        pos -= 1;
    }

    // Find word start by scanning backwards over identifier characters.
    let mut start = pos;
    while start > 0 {
        let Some((prev_start, ch)) = text[..start].char_indices().next_back() else {
            break;
        };
        if sql_text::is_identifier_char(ch) {
            start = prev_start;
        } else {
            break;
        }
    }

    // Find word end by scanning forwards over identifier characters.
    let mut end = pos;
    while end < text.len() {
        let Some(ch) = text[end..].chars().next() else {
            break;
        };
        if sql_text::is_identifier_char(ch) {
            end += ch.len_utf8();
        } else {
            break;
        }
    }

    let word = text.get(start..pos).unwrap_or("").to_string();
    (word, start, end)
}

// Detect context for smarter suggestions (after FROM, after SELECT, etc.)
// Uses the deep context analyzer for accurate depth-aware detection.
pub fn detect_sql_context(text: &str, cursor_pos: usize) -> SqlContext {
    use crate::ui::intellisense_context::{self, SqlPhase};
    use crate::ui::sql_editor::SqlEditorWidget;

    let mut end = cursor_pos.min(text.len());
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    let before = &text[..end];
    let tokens = SqlEditorWidget::tokenize_sql(before);
    let full_tokens = SqlEditorWidget::tokenize_sql(text);
    let ctx = intellisense_context::analyze_cursor_context(&tokens, &full_tokens);

    match ctx.phase {
        SqlPhase::FromClause
        | SqlPhase::IntoClause
        | SqlPhase::UpdateTarget
        | SqlPhase::DeleteTarget
        | SqlPhase::MergeTarget => SqlContext::TableName,
        SqlPhase::SelectList => SqlContext::ColumnOrAll,
        SqlPhase::WhereClause
        | SqlPhase::JoinCondition
        | SqlPhase::GroupByClause
        | SqlPhase::HavingClause
        | SqlPhase::OrderByClause
        | SqlPhase::SetClause
        | SqlPhase::ConnectByClause
        | SqlPhase::StartWithClause => SqlContext::ColumnName,
        _ => SqlContext::General,
    }
}


#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum SqlContext {
    General,
    TableName,
    ColumnName,
    ColumnOrAll,
}

#[cfg(test)]
mod intellisense_tests {
    use super::*;

    #[test]
    fn get_suggestions_prefers_relations_in_table_context_with_empty_prefix() {
        let mut data = IntellisenseData::new();
        data.tables = (0..80).map(|i| format!("TBL_{:02}", i)).collect();
        data.rebuild_indices();

        let suggestions = data.get_suggestions("", false, None, true, false);

        assert_eq!(suggestions.len(), MAX_SUGGESTIONS);
        assert!(suggestions.iter().all(|s| s.starts_with("TBL_")));
    }

    #[test]
    fn get_suggestions_keeps_to_underscore_matches() {
        let mut data = IntellisenseData::new();
        let suggestions = data.get_suggestions("TO_", false, None, false, false);

        assert!(suggestions.iter().any(|s| s == "TO_CHAR"));
        assert!(suggestions.iter().any(|s| s == "TO_CHAR()"));
    }

    #[test]
    fn get_suggestions_prefers_columns_in_column_context_with_empty_prefix() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["EMP".to_string()];
        data.rebuild_indices();
        data.set_columns_for_table("EMP", vec!["EMPNO".to_string(), "ENAME".to_string()]);
        let column_scope = vec!["EMP".to_string()];

        let suggestions = data.get_suggestions("", true, Some(&column_scope), false, true);

        assert!(suggestions.contains(&"EMPNO".to_string()));
        assert!(suggestions.contains(&"ENAME".to_string()));
        assert!(!suggestions.contains(&"SELECT".to_string()));
    }

    #[test]
    fn get_suggestions_table_context_empty_prefix_falls_back_to_keywords_when_no_relations() {
        let mut data = IntellisenseData::new();

        let suggestions = data.get_suggestions("", false, None, true, false);

        assert!(suggestions.contains(&"SELECT".to_string()));
    }

    #[test]
    fn get_suggestions_column_context_empty_prefix_falls_back_when_columns_missing() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["EMP".to_string()];
        data.rebuild_indices();
        let column_scope = vec!["EMP".to_string()];

        let suggestions = data.get_suggestions("", true, Some(&column_scope), false, true);

        assert!(suggestions.contains(&"SELECT".to_string()));
    }

    #[test]
    fn get_word_at_cursor_supports_unicode_identifier() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let cursor = sql.find(" FROM").unwrap_or(sql.len());
        let (word, _, _) = get_word_at_cursor(sql, cursor);
        assert_eq!(word, "한글컬럼");
    }

    #[test]
    fn detect_sql_context_clamps_non_char_boundary_cursor() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let cursor = sql.find("한").unwrap_or(0) + 1;
        let result = std::panic::catch_unwind(|| detect_sql_context(sql, cursor));
        assert!(result.is_ok());
    }

    #[test]
    fn get_suggestions_excludes_exact_prefix_match() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["AB".to_string(), "ABC_TABLE".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_suggestions("ab", false, None, false, false);

        assert!(!suggestions.iter().any(|s| s.eq_ignore_ascii_case("ab")));
        assert!(suggestions
            .iter()
            .any(|s| s.eq_ignore_ascii_case("abc_table")));
    }

    #[test]
    fn get_suggestions_excludes_exact_prefix_match_for_wrapped_entries() {
        let mut data = IntellisenseData::new();

        let suggestions = data.get_suggestions("sum()", false, None, false, false);

        assert!(!suggestions.iter().any(|s| s.eq_ignore_ascii_case("sum()")));
    }

    #[test]
    fn sql_keywords_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for keyword in SQL_KEYWORDS {
            assert!(
                seen.insert(*keyword),
                "Duplicate SQL keyword found: {}",
                keyword
            );
        }
    }

    #[test]
    fn oracle_functions_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for func in ORACLE_FUNCTIONS {
            assert!(
                seen.insert(*func),
                "Duplicate Oracle function found: {}",
                func
            );
        }
    }

    #[test]
    fn get_suggestions_deduplicates_case_insensitive_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["EmpNo".to_string(), "EMPNO".to_string()]);
        let column_scope = vec!["emp".to_string()];

        let suggestions = data.get_suggestions("", true, Some(&column_scope), false, true);

        let empno_count = suggestions.iter().filter(|value| value.eq_ignore_ascii_case("EMPNO")).count();
        assert_eq!(empno_count, 1);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions.iter().any(|value| value.eq_ignore_ascii_case("EMPNO")));
    }

    #[test]
    fn get_suggestions_deduplicates_case_insensitive_relations() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["Emp".to_string(), "EMP".to_string(), "emp2".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_suggestions("", false, None, true, false);
        let emp_count = suggestions.iter().filter(|value| value.eq_ignore_ascii_case("EMP")).count();
        assert_eq!(emp_count, 1);
        assert!(suggestions.iter().any(|value| value.eq_ignore_ascii_case("EMP")));
    }

    #[test]
    fn virtual_table_columns_do_not_remove_real_table_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["REAL_COL".to_string()]);
        data.set_virtual_table_columns("EMP", vec!["VIRTUAL_COL".to_string()]);
        data.clear_virtual_tables();

        let columns = data.get_column_suggestions("", Some(&vec!["EMP".to_string()]));
        assert!(
            columns.contains(&"REAL_COL".to_string()),
            "real table columns should remain cached after virtual cache clear"
        );
        assert!(
            !columns.contains(&"VIRTUAL_COL".to_string()),
            "virtual table columns should be cleared when clear_virtual_tables is called"
        );
    }

    #[test]
    fn virtual_table_columns_take_precedence_before_real_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["REAL_COL".to_string()]);
        data.set_virtual_table_columns("EMP", vec!["VIRTUAL_COL".to_string()]);

        let columns = data.get_column_suggestions("", Some(&vec!["EMP".to_string()]));
        assert!(
            columns.contains(&"VIRTUAL_COL".to_string()),
            "virtual table columns should be used while virtual entries exist"
        );
        assert!(
            !columns.contains(&"REAL_COL".to_string()),
            "real table columns should not be included while virtual override exists"
        );
    }

    #[test]
    fn get_columns_for_table_uses_virtual_cache_when_available() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["REAL_COL".to_string()]);
        data.set_virtual_table_columns("EMP", vec!["VIRTUAL_COL".to_string()]);

        let columns = data.get_columns_for_table("EMP");
        assert_eq!(columns, vec!["VIRTUAL_COL".to_string()]);
    }

    #[test]
    fn get_columns_for_table_falls_back_to_unqualified_cache_key() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["EMPNO".to_string()]);

        let columns = data.get_columns_for_table("SCOTT.EMP");
        assert_eq!(columns, vec!["EMPNO".to_string()]);
    }

    #[test]
    fn get_all_columns_for_highlighting_includes_virtual_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["REAL_COL".to_string()]);
        data.set_virtual_table_columns("VIRTUAL", vec!["VIRTUAL_COL".to_string()]);

        let columns = data.get_all_columns_for_highlighting();
        assert!(columns.contains(&"REAL_COL".to_string()));
        assert!(columns.contains(&"VIRTUAL_COL".to_string()));
    }

    #[test]
    fn get_column_suggestions_scope_falls_back_to_unqualified_table_cache_key() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["HELP".to_string()];
        data.rebuild_indices();
        data.set_columns_for_table("HELP", vec!["TOPIC".to_string(), "TEXT".to_string()]);

        let scope = vec!["SCOTT.HELP".to_string()];
        let suggestions = data.get_column_suggestions("", Some(scope.as_slice()));

        assert!(
            suggestions
                .iter()
                .any(|name| name.eq_ignore_ascii_case("TOPIC")),
            "expected schema-qualified scope to reuse unqualified cached columns, got: {:?}",
            suggestions
        );
    }
}
