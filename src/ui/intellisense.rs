use crate::sql_text;
use crate::ui::theme;
use fltk::{browser::HoldBrowser, prelude::*, window::Window};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// Shared Oracle SQL keywords
pub const SQL_KEYWORDS: &[&str] = sql_text::ORACLE_SQL_KEYWORDS;

// Oracle built-in functions
pub const ORACLE_FUNCTIONS: &[&str] = &[
    "ABS",
    "ACOS",
    "ADD_MONTHS",
    "ANY_VALUE",
    "APPENDCHILDXML",
    "APPROX_COUNT_DISTINCT",
    "APPROX_PERCENTILE",
    "ASCII",
    "ASCIISTR",
    "ASIN",
    "ATAN",
    "ATAN2",
    "AVG",
    "BFILENAME",
    "BIN_TO_NUM",
    "BITAND",
    "CARDINALITY",
    "CAST",
    "CEIL",
    "CHARTOROWID",
    "CHR",
    "CLUSTER_DETAILS",
    "CLUSTER_DISTANCE",
    "CLUSTER_ID",
    "CLUSTER_PROBABILITY",
    "CLUSTER_SET",
    "COALESCE",
    "COLLECT",
    "COMPOSE",
    "CONCAT",
    "CONVERT",
    "CORR",
    "COS",
    "COSH",
    "COUNT",
    "COVAR_POP",
    "COVAR_SAMP",
    "CUME_DIST",
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "DBTIMEZONE",
    "DECODE",
    "DECOMPOSE",
    "DELETEXML",
    "DENSE_RANK",
    "DEREF",
    "DUMP",
    "EMPTY_BLOB",
    "EMPTY_CLOB",
    "EXISTSNODE",
    "EXP",
    "EXTRACT",
    "EXTRACTVALUE",
    "FEATURE_COMPARE",
    "FEATURE_ID",
    "FEATURE_SET",
    "FEATURE_VALUE",
    "FIRST",
    "FIRST_VALUE",
    "FLOOR",
    "FROM_TZ",
    "GREATEST",
    "GROUPING",
    "GROUPING_ID",
    "GROUP_ID",
    "HEXTORAW",
    "INITCAP",
    "INSERTCHILDXML",
    "INSERTCHILDXMLAFTER",
    "INSERTCHILDXMLBEFORE",
    "INSERTXMLBEFORE",
    "INSTR",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_EQUAL",
    "JSON_EXISTS",
    "JSON_MERGEPATCH",
    "JSON_OBJECT",
    "JSON_OBJECTAGG",
    "JSON_QUERY",
    "JSON_SCALAR",
    "JSON_SERIALIZE",
    "JSON_TABLE",
    "JSON_TRANSFORM",
    "JSON_VALUE",
    "LAG",
    "LAST",
    "LAST_DAY",
    "LAST_VALUE",
    "LEAD",
    "LEAST",
    "LENGTH",
    "LISTAGG",
    "LN",
    "LNNVL",
    "LOCALTIMESTAMP",
    "LOG",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MAKE_REF",
    "MAX",
    "MEDIAN",
    "MIN",
    "MOD",
    "MONTHS_BETWEEN",
    "NANVL",
    "NEW_TIME",
    "NEXT_DAY",
    "NLSSORT",
    "NLS_INITCAP",
    "NLS_LOWER",
    "NLS_UPPER",
    "NTH_VALUE",
    "NTILE",
    "NULLIF",
    "NUMTODSINTERVAL",
    "NUMTOYMINTERVAL",
    "NVL",
    "NVL2",
    "ORA_HASH",
    "ORA_INVOKING_USER",
    "ORA_INVOKING_USERID",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "PERCENT_RANK",
    "POWER",
    "PREDICTION",
    "PREDICTION_BOUNDS",
    "PREDICTION_COST",
    "PREDICTION_DETAILS",
    "PREDICTION_PROBABILITY",
    "PREDICTION_SET",
    "RANK",
    "RATIO_TO_REPORT",
    "RAWTOHEX",
    "REF",
    "REFTOHEX",
    "REGEXP_COUNT",
    "REGEXP_INSTR",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_COUNT",
    "REGR_INTERCEPT",
    "REGR_R2",
    "REGR_SLOPE",
    "REGR_SXX",
    "REGR_SXY",
    "REGR_SYY",
    "REMAINDER",
    "REPLACE",
    "REVERSE",
    "ROUND",
    "ROWIDTOCHAR",
    "ROW_NUMBER",
    "RPAD",
    "RTRIM",
    "SESSIONTIMEZONE",
    "SIGN",
    "SIN",
    "SINH",
    "SOUNDEX",
    "SQRT",
    "STANDARD_HASH",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "SUBSTR",
    "SUM",
    "SYSDATE",
    "SYSTIMESTAMP",
    "SYS_CONNECT_BY_PATH",
    "SYS_CONTEXT",
    "SYS_GUID",
    "SYS_TYPEID",
    "TAN",
    "TANH",
    "TO_BINARY_DOUBLE",
    "TO_BINARY_FLOAT",
    "TO_BLOB",
    "TO_CHAR",
    "TO_CLOB",
    "TO_DATE",
    "TO_DSINTERVAL",
    "TO_LOB",
    "TO_MULTI_BYTE",
    "TO_NCHAR",
    "TO_NCLOB",
    "TO_NUMBER",
    "TO_SINGLE_BYTE",
    "TO_TIMESTAMP",
    "TO_TIMESTAMP_TZ",
    "TO_YMINTERVAL",
    "TRANSLATE",
    "TREAT",
    "TRIM",
    "TRUNC",
    "TZ_OFFSET",
    "UID",
    "UNISTR",
    "UPDATEXML",
    "UPPER",
    "USER",
    "USERENV",
    "VALIDATE_CONVERSION",
    "VALUE",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "VSIZE",
    "WIDTH_BUCKET",
    "XMLAGG",
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
    "XPATH",
];

// ---------------------------------------------------------------------------
// MySQL / MariaDB built-in functions (sorted, uppercase)
// ---------------------------------------------------------------------------
pub const MYSQL_FUNCTIONS: &[&str] = &[
    "ABS",
    "ACOS",
    "ADDDATE",
    "ADDTIME",
    "AES_DECRYPT",
    "AES_ENCRYPT",
    "ANY_VALUE",
    "ASCII",
    "ASIN",
    "ATAN",
    "ATAN2",
    "AVG",
    "BENCHMARK",
    "BIN",
    "BIN_TO_UUID",
    "BIT_AND",
    "BIT_COUNT",
    "BIT_LENGTH",
    "BIT_OR",
    "BIT_XOR",
    "CAST",
    "CEIL",
    "CEILING",
    "CHAR",
    "CHARACTER_LENGTH",
    "CHARSET",
    "CHAR_LENGTH",
    "COALESCE",
    "COERCIBILITY",
    "COLLATION",
    "COMPRESS",
    "CONCAT",
    "CONCAT_WS",
    "CONNECTION_ID",
    "CONV",
    "CONVERT",
    "CONVERT_TZ",
    "COS",
    "COT",
    "COUNT",
    "CRC32",
    "CUME_DIST",
    "CURDATE",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURTIME",
    "DATABASE",
    "DATE",
    "DATEDIFF",
    "DATE_ADD",
    "DATE_FORMAT",
    "DATE_SUB",
    "DAY",
    "DAYNAME",
    "DAYOFMONTH",
    "DAYOFWEEK",
    "DAYOFYEAR",
    "DECODE",
    "DEFAULT",
    "DEGREES",
    "DENSE_RANK",
    "DES_DECRYPT",
    "DES_ENCRYPT",
    "ELT",
    "ENCODE",
    "ENCRYPT",
    "EXP",
    "EXPORT_SET",
    "EXTRACT",
    "FIELD",
    "FIND_IN_SET",
    "FIRST_VALUE",
    "FLOOR",
    "FORMAT",
    "FOUND_ROWS",
    "FROM_BASE64",
    "FROM_DAYS",
    "FROM_UNIXTIME",
    "GET_FORMAT",
    "GET_LOCK",
    "GREATEST",
    "GROUP_CONCAT",
    "HEX",
    "HOUR",
    "IF",
    "IFNULL",
    "INET6_ATON",
    "INET6_NTOA",
    "INET_ATON",
    "INET_NTOA",
    "INSERT",
    "INSTR",
    "ISNULL",
    "IS_FREE_LOCK",
    "IS_IPV4",
    "IS_IPV4_COMPAT",
    "IS_IPV4_MAPPED",
    "IS_IPV6",
    "IS_USED_LOCK",
    "JSON_ARRAY",
    "JSON_ARRAYAGG",
    "JSON_ARRAY_APPEND",
    "JSON_ARRAY_INSERT",
    "JSON_CONTAINS",
    "JSON_CONTAINS_PATH",
    "JSON_DEPTH",
    "JSON_EXTRACT",
    "JSON_INSERT",
    "JSON_KEYS",
    "JSON_LENGTH",
    "JSON_MERGE",
    "JSON_MERGE_PATCH",
    "JSON_MERGE_PRESERVE",
    "JSON_OBJECT",
    "JSON_OBJECTAGG",
    "JSON_OVERLAPS",
    "JSON_PRETTY",
    "JSON_QUOTE",
    "JSON_REMOVE",
    "JSON_REPLACE",
    "JSON_SCHEMA_VALID",
    "JSON_SCHEMA_VALIDATION_REPORT",
    "JSON_SEARCH",
    "JSON_SET",
    "JSON_STORAGE_FREE",
    "JSON_STORAGE_SIZE",
    "JSON_TABLE",
    "JSON_TYPE",
    "JSON_UNQUOTE",
    "JSON_VALID",
    "JSON_VALUE",
    "LAG",
    "LAST_DAY",
    "LAST_INSERT_ID",
    "LAST_VALUE",
    "LCASE",
    "LEAD",
    "LEAST",
    "LEFT",
    "LENGTH",
    "LN",
    "LOAD_FILE",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCATE",
    "LOG",
    "LOG10",
    "LOG2",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MAKEDATE",
    "MAKETIME",
    "MAKE_SET",
    "MASTER_POS_WAIT",
    "MAX",
    "MD5",
    "MICROSECOND",
    "MID",
    "MIN",
    "MINUTE",
    "MOD",
    "MONTH",
    "MONTHNAME",
    "NAME_CONST",
    "NOW",
    "NTH_VALUE",
    "NTILE",
    "NULLIF",
    "OCT",
    "OCTET_LENGTH",
    "OLD_PASSWORD",
    "ORD",
    "PASSWORD",
    "PERCENT_RANK",
    "PERIOD_ADD",
    "PERIOD_DIFF",
    "PI",
    "POINT",
    "POLYGON",
    "POSITION",
    "POW",
    "POWER",
    "QUARTER",
    "QUOTE",
    "RADIANS",
    "RAND",
    "RANDOM_BYTES",
    "RANK",
    "REGEXP_INSTR",
    "REGEXP_LIKE",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "RELEASE_ALL_LOCKS",
    "RELEASE_LOCK",
    "REPEAT",
    "REPLACE",
    "REVERSE",
    "RIGHT",
    "RLIKE",
    "ROUND",
    "ROW_COUNT",
    "ROW_NUMBER",
    "RPAD",
    "RTRIM",
    "SCHEMA",
    "SECOND",
    "SEC_TO_TIME",
    "SESSION_USER",
    "SHA1",
    "SHA2",
    "SIGN",
    "SIN",
    "SLEEP",
    "SOUNDEX",
    "SPACE",
    "SQRT",
    "STD",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "STRCMP",
    "STR_TO_DATE",
    "ST_AREA",
    "ST_ASBINARY",
    "ST_ASTEXT",
    "ST_ASWKB",
    "ST_ASWKT",
    "ST_BUFFER",
    "ST_CENTROID",
    "ST_CONTAINS",
    "ST_CONVEXHULL",
    "ST_CROSSES",
    "ST_DIFFERENCE",
    "ST_DIMENSION",
    "ST_DISJOINT",
    "ST_DISTANCE",
    "ST_DISTANCE_SPHERE",
    "ST_ENDPOINT",
    "ST_ENVELOPE",
    "ST_EQUALS",
    "ST_EXTERIORRING",
    "ST_GEOMCOLLFROMTEXT",
    "ST_GEOMCOLLFROMWKB",
    "ST_GEOMETRYCOLLECTIONFROMTEXT",
    "ST_GEOMETRYCOLLECTIONFROMWKB",
    "ST_GEOMETRYFROMTEXT",
    "ST_GEOMETRYFROMWKB",
    "ST_GEOMETRYN",
    "ST_GEOMETRYTYPE",
    "ST_GEOMFROMGEOJSON",
    "ST_GEOMFROMTEXT",
    "ST_GEOMFROMWKB",
    "ST_INTERIORRINGN",
    "ST_INTERSECTION",
    "ST_INTERSECTS",
    "ST_ISCLOSED",
    "ST_ISEMPTY",
    "ST_ISSIMPLE",
    "ST_ISVALID",
    "ST_LATFROMGEOHASH",
    "ST_LATITUDE",
    "ST_LENGTH",
    "ST_LINEFROMTEXT",
    "ST_LINEFROMWKB",
    "ST_LINESTRINGFROMTEXT",
    "ST_LINESTRINGFROMWKB",
    "ST_LONGFROMGEOHASH",
    "ST_LONGITUDE",
    "ST_MAKEENVELOPE",
    "ST_MLINEFROMTEXT",
    "ST_MLINEFROMWKB",
    "ST_MPOINTFROMTEXT",
    "ST_MPOINTFROMWKB",
    "ST_MPOLYFROMTEXT",
    "ST_MPOLYFROMWKB",
    "ST_MULTILINESTRINGFROMTEXT",
    "ST_MULTILINESTRINGFROMWKB",
    "ST_MULTIPOINTFROMTEXT",
    "ST_MULTIPOINTFROMWKB",
    "ST_MULTIPOLYGONFROMTEXT",
    "ST_MULTIPOLYGONFROMWKB",
    "ST_NUMGEOMETRIES",
    "ST_NUMINTERIORRING",
    "ST_NUMINTERIORRINGS",
    "ST_NUMPOINTS",
    "ST_OVERLAPS",
    "ST_POINTATDISTANCE",
    "ST_POINTFROMGEOHASH",
    "ST_POINTFROMTEXT",
    "ST_POINTFROMWKB",
    "ST_POINTN",
    "ST_POLYFROMTEXT",
    "ST_POLYFROMWKB",
    "ST_POLYGONFROMTEXT",
    "ST_POLYGONFROMWKB",
    "ST_SIMPLIFY",
    "ST_SRID",
    "ST_STARTPOINT",
    "ST_SWAPXY",
    "ST_SYMDIFFERENCE",
    "ST_TOUCHES",
    "ST_TRANSFORM",
    "ST_UNION",
    "ST_VALIDATE",
    "ST_WITHIN",
    "ST_X",
    "ST_Y",
    "SUBDATE",
    "SUBSTR",
    "SUBSTRING",
    "SUBSTRING_INDEX",
    "SUBTIME",
    "SUM",
    "SYSDATE",
    "SYSTEM_USER",
    "TAN",
    "TIME",
    "TIMEDIFF",
    "TIMESTAMP",
    "TIMESTAMPADD",
    "TIMESTAMPDIFF",
    "TIME_FORMAT",
    "TIME_TO_SEC",
    "TO_BASE64",
    "TO_DAYS",
    "TO_SECONDS",
    "TRIM",
    "TRUNCATE",
    "UCASE",
    "UNCOMPRESS",
    "UNCOMPRESSED_LENGTH",
    "UNHEX",
    "UNIX_TIMESTAMP",
    "UPPER",
    "USER",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "UUID",
    "UUID_SHORT",
    "UUID_TO_BIN",
    "VALIDATE_PASSWORD_STRENGTH",
    "VALUES",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "VERSION",
    "WAIT_FOR_EXECUTED_GTID_SET",
    "WEEK",
    "WEEKDAY",
    "WEEKOFYEAR",
    "WEIGHT_STRING",
    "YEAR",
    "YEARWEEK",
];

pub static MYSQL_FUNCTIONS_SET: once_cell::sync::Lazy<std::collections::HashSet<&'static str>> =
    once_cell::sync::Lazy::new(|| MYSQL_FUNCTIONS.iter().copied().collect());

const FUNCTION_SUFFIX: &str = "()";

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QualifiedMemberKind {
    Table,
    View,
    Procedure,
    Function,
    Package,
    Sequence,
    Synonym,
    PublicSynonym,
    User,
}

impl QualifiedMemberKind {
    pub fn from_object_type_name(object_type: &str) -> Option<Self> {
        match object_type.trim().to_ascii_uppercase().as_str() {
            "TABLE" | "BASE TABLE" => Some(Self::Table),
            "VIEW" => Some(Self::View),
            "PROCEDURE" => Some(Self::Procedure),
            "FUNCTION" => Some(Self::Function),
            "PACKAGE" => Some(Self::Package),
            "SEQUENCE" => Some(Self::Sequence),
            "SYNONYM" => Some(Self::Synonym),
            "PUBLIC SYNONYM" => Some(Self::PublicSynonym),
            "USER" | "SCHEMA" => Some(Self::User),
            _ => None,
        }
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
    pub packages: Vec<String>,
    pub sequences: Vec<String>,
    pub synonyms: Vec<String>,
    pub public_synonyms: Vec<String>,
    pub users: Vec<String>,
    table_entries: Vec<NameEntry>,
    view_entries: Vec<NameEntry>,
    procedure_entries: Vec<NameEntry>,
    function_entries: Vec<NameEntry>,
    package_entries: Vec<NameEntry>,
    sequence_entries: Vec<NameEntry>,
    synonym_entries: Vec<NameEntry>,
    public_synonym_entries: Vec<NameEntry>,
    user_entries: Vec<NameEntry>,
    column_entries_by_table: HashMap<String, Vec<NameEntry>>,
    virtual_column_entries_by_table: HashMap<String, Vec<NameEntry>>,
    member_entries_by_qualifier: HashMap<String, Vec<NameEntry>>,
    member_kinds_by_qualifier: HashMap<String, HashMap<String, HashSet<QualifiedMemberKind>>>,
    relation_member_entries_by_qualifier: HashMap<String, Vec<NameEntry>>,
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
            packages: Vec::new(),
            sequences: Vec::new(),
            synonyms: Vec::new(),
            public_synonyms: Vec::new(),
            users: Vec::new(),
            table_entries: Vec::new(),
            view_entries: Vec::new(),
            procedure_entries: Vec::new(),
            function_entries: Vec::new(),
            package_entries: Vec::new(),
            sequence_entries: Vec::new(),
            synonym_entries: Vec::new(),
            public_synonym_entries: Vec::new(),
            user_entries: Vec::new(),
            column_entries_by_table: HashMap::new(),
            virtual_column_entries_by_table: HashMap::new(),
            member_entries_by_qualifier: HashMap::new(),
            member_kinds_by_qualifier: HashMap::new(),
            relation_member_entries_by_qualifier: HashMap::new(),
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
        self.get_suggestions_for_db(
            prefix,
            include_columns,
            column_tables,
            prefer_relations,
            prefer_columns,
            None,
        )
    }

    pub fn get_suggestions_for_db(
        &mut self,
        prefix: &str,
        include_columns: bool,
        column_tables: Option<&[String]>,
        prefer_relations: bool,
        prefer_columns: bool,
        db_type: Option<crate::db::DatabaseType>,
    ) -> Vec<String> {
        self.ensure_base_indices();

        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();
        let relation_only = prefer_relations && prefix_upper.is_empty();
        let column_only = prefer_columns && prefix_upper.is_empty();

        if prefer_columns && include_columns {
            self.append_column_suggestions(
                &prefix_upper,
                column_tables,
                false,
                &mut suggestions,
                &mut seen,
            );
            if column_only && !suggestions.is_empty() {
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
                return suggestions;
            }
            if Self::push_entries(
                &self.view_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                return suggestions;
            }
            if Self::push_entries(
                &self.synonym_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                return suggestions;
            }
            if Self::push_entries(
                &self.public_synonym_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                return suggestions;
            }
            if relation_only && !suggestions.is_empty() {
                suggestions.truncate(MAX_SUGGESTIONS);
                return suggestions;
            }
        }

        // Add SQL keywords and built-in functions only when a non-empty prefix
        // is available.  With an empty prefix we would iterate the entire
        // sorted array and hit MAX_SUGGESTIONS before useful entries appear,
        // so skip them – the caller already has context-specific entries
        // (tables, views, columns) for empty-prefix completions.
        let is_mysql = db_type.is_some_and(crate::db::DatabaseType::uses_mysql_sql_dialect);
        if !prefix_upper.is_empty() {
            // SQL keywords – choose list based on connected database type
            let keywords: &[&str] = if is_mysql {
                sql_text::MYSQL_SQL_KEYWORDS
            } else {
                SQL_KEYWORDS
            };
            {
                let start = keywords.partition_point(|kw| *kw < prefix_upper.as_str());
                for keyword in &keywords[start..] {
                    if !keyword.starts_with(prefix_upper.as_str()) {
                        break;
                    }
                    if seen.insert((*keyword).to_string()) {
                        suggestions.push((*keyword).to_string());
                    }
                    if suggestions.len() >= MAX_SUGGESTIONS {
                        break;
                    }
                }
            }

            // Built-in functions – choose list based on connected database type
            let functions: &[&str] = if is_mysql {
                MYSQL_FUNCTIONS
            } else {
                ORACLE_FUNCTIONS
            };
            {
                let start = functions.partition_point(|f| *f < prefix_upper.as_str());
                for func in &functions[start..] {
                    if !func.starts_with(prefix_upper.as_str()) {
                        break;
                    }
                    let rendered = format!("{func}{FUNCTION_SUFFIX}");
                    if seen.insert(rendered.to_uppercase()) {
                        suggestions.push(rendered);
                    }
                    if suggestions.len() >= MAX_SUGGESTIONS {
                        break;
                    }
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
                return suggestions;
            }

            if Self::push_entries(
                &self.view_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                return suggestions;
            }

            if Self::push_entries(
                &self.synonym_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
                return suggestions;
            }

            if Self::push_entries(
                &self.public_synonym_entries,
                &prefix_upper,
                &mut suggestions,
                &mut seen,
            ) {
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
            return suggestions;
        }

        // Add packages
        if Self::push_entries(
            &self.package_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            return suggestions;
        }

        // Add functions
        if Self::push_entries(
            &self.function_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            return suggestions;
        }

        if Self::push_entries(
            &self.sequence_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        ) {
            return suggestions;
        }

        let _ = Self::push_entries(
            &self.user_entries,
            &prefix_upper,
            &mut suggestions,
            &mut seen,
        );

        if include_columns && !prefer_columns {
            self.append_column_suggestions(
                &prefix_upper,
                column_tables,
                false,
                &mut suggestions,
                &mut seen,
            );
        }

        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
    }

    pub fn get_relation_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(
            prefix,
            &[
                &self.table_entries,
                &self.view_entries,
                &self.synonym_entries,
                &self.public_synonym_entries,
                &self.user_entries,
            ],
        )
    }

    pub fn get_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(
            prefix,
            &[
                &self.table_entries,
                &self.view_entries,
                &self.synonym_entries,
                &self.public_synonym_entries,
                &self.procedure_entries,
                &self.package_entries,
                &self.function_entries,
                &self.sequence_entries,
                &self.user_entries,
            ],
        )
    }

    pub fn get_routine_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(
            prefix,
            &[
                &self.procedure_entries,
                &self.package_entries,
                &self.function_entries,
            ],
        )
    }

    pub fn get_table_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.table_entries])
    }

    pub fn get_view_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.view_entries])
    }

    pub fn get_procedure_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.procedure_entries])
    }

    pub fn get_function_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.function_entries])
    }

    pub fn get_package_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.package_entries])
    }

    pub fn get_sequence_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.sequence_entries])
    }

    pub fn get_synonym_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.synonym_entries])
    }

    pub fn get_public_synonym_object_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.public_synonym_entries])
    }

    pub fn get_user_suggestions(&mut self, prefix: &str) -> Vec<String> {
        self.ensure_base_indices();
        Self::suggestions_from_entry_groups(prefix, &[&self.user_entries])
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

        self.append_column_suggestions(
            &prefix_upper,
            column_tables,
            true,
            &mut suggestions,
            &mut seen,
        );

        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
    }

    pub fn set_members_for_qualifier(&mut self, qualifier: &str, members: Vec<String>) {
        let key = Self::normalize_qualifier_lookup_key(qualifier);
        if key.is_empty() {
            return;
        }
        self.member_entries_by_qualifier
            .insert(key, Self::build_entries(&members));
        self.member_kinds_by_qualifier
            .remove(&Self::normalize_qualifier_lookup_key(qualifier));
    }

    pub fn set_members_for_qualifier_with_kinds(
        &mut self,
        qualifier: &str,
        members: Vec<(String, Option<QualifiedMemberKind>)>,
    ) {
        let key = Self::normalize_qualifier_lookup_key(qualifier);
        if key.is_empty() {
            return;
        }

        let mut names = Vec::with_capacity(members.len());
        let mut member_kinds: HashMap<String, HashSet<QualifiedMemberKind>> = HashMap::new();
        for (name, kind) in members {
            names.push(name.clone());
            if let Some(kind) = kind {
                member_kinds
                    .entry(name.to_uppercase())
                    .or_default()
                    .insert(kind);
            }
        }

        self.member_entries_by_qualifier
            .insert(key.clone(), Self::build_entries(&names));
        if member_kinds.is_empty() {
            self.member_kinds_by_qualifier.remove(&key);
        } else {
            self.member_kinds_by_qualifier.insert(key, member_kinds);
        }
    }

    pub fn set_relation_members_for_qualifier(&mut self, qualifier: &str, members: Vec<String>) {
        let key = Self::normalize_qualifier_lookup_key(qualifier);
        if key.is_empty() {
            return;
        }
        self.relation_member_entries_by_qualifier
            .insert(key, Self::build_entries(&members));
    }

    pub fn has_members_for_qualifier(&self, qualifier: &str, relation_only: bool) -> bool {
        self.member_entries_for_qualifier(qualifier, relation_only)
            .is_some_and(|entries| !entries.is_empty())
    }

    pub fn get_member_suggestions(
        &mut self,
        qualifier: &str,
        prefix: &str,
        relation_only: bool,
    ) -> Vec<String> {
        self.ensure_base_indices();

        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();

        if let Some(entries) = self.member_entries_for_qualifier(qualifier, relation_only) {
            let _ = Self::push_entries(entries, &prefix_upper, &mut suggestions, &mut seen);
        }

        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
    }

    pub fn qualifier_member_matches_kinds(
        &self,
        qualifier: &str,
        candidate: &str,
        expected_kinds: &[QualifiedMemberKind],
    ) -> Option<bool> {
        if expected_kinds.is_empty() {
            return Some(true);
        }

        let candidate_upper = candidate.to_uppercase();
        for key in Self::qualifier_lookup_keys(qualifier) {
            let Some(member_kinds) = self.member_kinds_by_qualifier.get(&key) else {
                continue;
            };
            let matches = member_kinds
                .get(&candidate_upper)
                .is_some_and(|kinds| expected_kinds.iter().any(|kind| kinds.contains(kind)));
            return Some(matches);
        }

        None
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

    fn append_column_suggestions(
        &mut self,
        prefix_upper: &str,
        column_tables: Option<&[String]>,
        allow_empty_prefix_global: bool,
        suggestions: &mut Vec<String>,
        seen: &mut HashSet<String>,
    ) {
        match column_tables {
            Some(tables) if !tables.is_empty() => {
                for table in tables {
                    if let Some(cols) = self.column_entries_for_scope_table(table) {
                        if Self::push_entries(cols, prefix_upper, suggestions, seen) {
                            break;
                        }
                    }
                }
            }
            _ => {
                if allow_empty_prefix_global || !prefix_upper.is_empty() {
                    self.ensure_all_columns_entries();
                    let _ = Self::push_entries(
                        &self.all_columns_entries,
                        prefix_upper,
                        suggestions,
                        seen,
                    );
                }
            }
        }
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
        let mut seen: HashSet<&str> = HashSet::new();
        let mut columns = Vec::new();

        for (table, entries) in &self.column_entries_by_table {
            if self.virtual_column_entries_by_table.contains_key(table) {
                continue;
            }
            for entry in entries {
                if seen.insert(entry.upper.as_str()) {
                    columns.push(entry.name.clone());
                }
            }
        }

        for names in self.virtual_column_entries_by_table.values() {
            for entry in names {
                if seen.insert(entry.upper.as_str()) {
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
        let entries = Self::build_entries(&columns);
        self.columns.insert(key.clone(), columns);
        self.column_entries_by_table.insert(key, entries);
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
                    .is_none_or(|started| now.duration_since(*started) >= stale_after)
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
        self.tables.iter().any(|t| t.eq_ignore_ascii_case(&upper))
            || self.views.iter().any(|v| v.eq_ignore_ascii_case(&upper))
            || self.synonyms.iter().any(|v| v.eq_ignore_ascii_case(&upper))
            || self
                .public_synonyms
                .iter()
                .any(|v| v.eq_ignore_ascii_case(&upper))
    }

    pub fn rebuild_indices(&mut self) {
        self.table_entries = Self::build_entries(&self.tables);
        self.view_entries = Self::build_entries(&self.views);
        self.procedure_entries = Self::build_entries(&self.procedures);
        self.function_entries = Self::build_entries(&self.functions);
        self.package_entries = Self::build_entries(&self.packages);
        self.sequence_entries = Self::build_entries(&self.sequences);
        self.synonym_entries = Self::build_entries(&self.synonyms);
        self.public_synonym_entries = Self::build_entries(&self.public_synonyms);
        self.user_entries = Self::build_entries(&self.users);
        self.relations_upper = self
            .tables
            .iter()
            .chain(self.views.iter())
            .chain(self.synonyms.iter())
            .chain(self.public_synonyms.iter())
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
    pub fn replace_virtual_table_columns(&mut self, virtual_columns: HashMap<String, Vec<String>>) {
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
                self.virtual_column_entries_by_table
                    .insert(key.clone(), entries);
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
            || self.package_entries.len() != self.packages.len()
            || self.sequence_entries.len() != self.sequences.len()
            || self.synonym_entries.len() != self.synonyms.len()
            || self.public_synonym_entries.len() != self.public_synonyms.len()
            || self.user_entries.len() != self.users.len()
        {
            self.rebuild_indices();
        }
    }

    fn member_entries_for_qualifier(
        &self,
        qualifier: &str,
        relation_only: bool,
    ) -> Option<&[NameEntry]> {
        let keys = Self::qualifier_lookup_keys(qualifier);
        let source = if relation_only {
            &self.relation_member_entries_by_qualifier
        } else {
            &self.member_entries_by_qualifier
        };

        for key in &keys {
            if let Some(entries) = source.get(key) {
                return Some(entries.as_slice());
            }
        }

        if relation_only {
            for key in &keys {
                if let Some(entries) = self.member_entries_by_qualifier.get(key) {
                    return Some(entries.as_slice());
                }
            }
        }

        None
    }

    fn suggestions_from_entry_groups(prefix: &str, groups: &[&[NameEntry]]) -> Vec<String> {
        let prefix_upper = prefix.to_uppercase();
        let mut suggestions = Vec::new();
        let mut seen = HashSet::new();

        for group in groups {
            if Self::push_entries(group, &prefix_upper, &mut suggestions, &mut seen) {
                break;
            }
        }

        suggestions.truncate(MAX_SUGGESTIONS);
        suggestions
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

    fn normalize_qualifier_lookup_key(qualifier: &str) -> String {
        qualifier
            .split('.')
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join(".")
            .to_ascii_uppercase()
    }

    fn qualifier_lookup_keys(qualifier: &str) -> Vec<String> {
        let normalized = Self::normalize_qualifier_lookup_key(qualifier);
        if normalized.is_empty() {
            return Vec::new();
        }

        let mut keys = vec![normalized.clone()];
        if let Some(last) = normalized.rsplit('.').next() {
            if last != normalized {
                keys.push(last.to_string());
            }
        }
        keys
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
            if seen.insert(entry.upper.clone()) {
                suggestions.push(entry.name.clone());
                if suggestions.len() >= MAX_SUGGESTIONS {
                    return true;
                }
            }
        }
        suggestions.len() >= MAX_SUGGESTIONS
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
    suggestions: Arc<Mutex<Vec<String>>>,
    all_suggestions: Arc<Mutex<Vec<String>>>,
    selected_callback: Arc<Mutex<Option<Box<dyn FnMut(String)>>>>,
    state: Arc<Mutex<PopupState>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PopupState {
    Hidden,
    Visible,
}

impl PopupState {
    fn is_visible(self) -> bool {
        matches!(self, Self::Visible)
    }
}

impl IntellisensePopup {
    const POPUP_PAGE_STEP: i32 = 10;

    fn is_deleted(&self) -> bool {
        self.window.was_deleted() || self.browser.was_deleted()
    }

    fn next_page_selection(current: i32, count: i32) -> Option<i32> {
        if count <= 0 {
            return None;
        }

        let normalized = current.max(1);
        Some((normalized + Self::POPUP_PAGE_STEP).min(count))
    }

    fn prev_page_selection(current: i32, count: i32) -> Option<i32> {
        if count <= 0 {
            return None;
        }

        let normalized = current.max(1);
        Some((normalized - Self::POPUP_PAGE_STEP).max(1))
    }

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

    fn invoke_selected_callback(
        callback_slot: &Arc<Mutex<Option<Box<dyn FnMut(String)>>>>,
        selected_text: String,
    ) {
        let callback = {
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slot.take()
        };

        if let Some(mut cb) = callback {
            let call_result = panic::catch_unwind(AssertUnwindSafe(|| cb(selected_text)));
            let mut slot = callback_slot
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if slot.is_none() {
                *slot = Some(cb);
            }
            if let Err(payload) = call_result {
                Self::log_callback_panic("intellisense selected callback", payload.as_ref());
            }
        }
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

        let suggestions = Arc::new(Mutex::new(Vec::new()));
        let all_suggestions = Arc::new(Mutex::new(Vec::new()));
        let selected_callback: Arc<Mutex<Option<Box<dyn FnMut(String)>>>> =
            Arc::new(Mutex::new(None));
        let state = Arc::new(Mutex::new(PopupState::Hidden));

        window.hide();

        let mut popup = Self {
            window,
            browser,
            suggestions,
            all_suggestions,
            selected_callback,
            state,
        };

        popup.setup_callbacks();
        popup
    }

    fn setup_callbacks(&mut self) {
        // Browser click callback - handle mouse selection
        let suggestions = self.suggestions.clone();
        let callback = self.selected_callback.clone();
        let mut window = self.window.clone();
        let state = self.state.clone();

        self.browser.set_callback(move |b| {
            let selected = b.value();
            if selected > 0 {
                // First, get the text with suggestions borrow, then release it
                let text = {
                    let suggestions = suggestions
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    suggestions.get((selected - 1) as usize).cloned()
                };
                if let Some(text) = text {
                    // Take the callback out, call it, then put it back if needed.
                    // This ensures the callback slot mutex is not held during callback execution
                    // while preserving callbacks that were replaced during invocation.
                    Self::invoke_selected_callback(&callback, text);
                    window.hide();
                    *state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
                }
            }
        });

        // Note: Keyboard events are handled by the editor, not by this popup window.
        // This is because the editor retains focus while the popup is visible,
        // so key events go to the editor's handle(), not the popup's handle().
    }

    pub fn show_suggestions(&mut self, suggestions: Vec<String>, x: i32, y: i32) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
            return;
        }

        if suggestions.is_empty() {
            self.hide();
            return;
        }

        *self
            .all_suggestions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = suggestions.clone();
        self.set_suggestions(suggestions, None);

        self.window.set_pos(x, y);
        if !self.window.shown() {
            self.window.show();
        }
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Visible;
    }

    fn set_suggestions(&mut self, suggestions: Vec<String>, selected_text: Option<&str>) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
            return;
        }

        let suggestion_count = suggestions.len();
        if suggestion_count == 0 {
            self.hide();
            return;
        }

        // Preserve selection when possible.
        let selected_idx = selected_text
            .and_then(|selected| suggestions.iter().position(|item| item == selected))
            .unwrap_or(0);
        self.browser.clear();
        for suggestion in &suggestions {
            self.browser.add(&format!("@C255 {}", suggestion));
        }
        *self
            .suggestions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = suggestions;

        if suggestion_count > 0 {
            self.browser.select((selected_idx + 1) as i32);
        }

        // Calculate popup size
        let height = (suggestion_count.min(10) * 20 + 10) as i32;
        self.window.set_size(320, height);
        self.browser.set_size(320, height);
    }

    pub fn filter_visible_suggestions_by_prefix(&mut self, prefix: &str) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
            return;
        }

        if !self.is_visible() {
            return;
        }

        let selected = self.get_selected();
        let filtered = {
            let all = self
                .all_suggestions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            filter_suggestions_by_prefix(all.as_slice(), prefix)
        };

        if filtered.is_empty() {
            self.hide();
            self.browser.clear();
            self.suggestions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
            return;
        }

        self.set_suggestions(filtered, selected.as_deref());
    }

    pub fn hide(&mut self) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
            return;
        }

        self.window.hide();
        self.window.resize(0, 0, 0, 0);
        *self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
    }

    pub fn clear_for_close(&mut self) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
        } else {
            self.hide();
            self.browser.set_callback(|_| {});
            self.browser.clear();
        }
        self.suggestions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        self.all_suggestions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        *self
            .selected_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    pub fn delete_for_close(&mut self) {
        if self.is_deleted() {
            *self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = PopupState::Hidden;
            return;
        }

        self.clear_for_close();
        if !self.window.was_deleted() {
            Window::delete(self.window.clone());
        }
    }

    pub fn is_visible(&self) -> bool {
        if self.is_deleted() {
            return false;
        }

        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_visible()
    }

    pub fn popup_dimensions(&self) -> (i32, i32) {
        if self.is_deleted() {
            return (0, 0);
        }

        (self.window.w(), self.window.h())
    }

    pub fn set_position(&mut self, x: i32, y: i32) {
        if self.is_deleted() {
            return;
        }

        self.window.set_pos(x, y);
    }

    pub fn contains_point(&self, x: i32, y: i32) -> bool {
        if self.is_deleted() {
            return false;
        }

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
        *self
            .selected_callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Box::new(callback));
    }

    pub fn select_next(&mut self) {
        if self.is_deleted() {
            return;
        }

        let current = self.browser.value();
        let count = self.browser.size();
        if current < count {
            self.browser.select(current + 1);
        }
    }

    pub fn select_prev(&mut self) {
        if self.is_deleted() {
            return;
        }

        let current = self.browser.value();
        if current > 1 {
            self.browser.select(current - 1);
        }
    }

    pub fn select_next_page(&mut self) {
        if self.is_deleted() {
            return;
        }

        let count = self.browser.size();
        let current = self.browser.value();
        if let Some(next) = Self::next_page_selection(current, count) {
            self.browser.select(next);
        }
    }

    pub fn select_prev_page(&mut self) {
        if self.is_deleted() {
            return;
        }

        let count = self.browser.size();
        let current = self.browser.value();
        if let Some(prev) = Self::prev_page_selection(current, count) {
            self.browser.select(prev);
        }
    }

    pub fn get_selected(&self) -> Option<String> {
        if self.is_deleted() {
            return None;
        }

        let selected = self.browser.value();
        if selected > 0 {
            self.suggestions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get((selected - 1) as usize)
                .cloned()
        } else {
            None
        }
    }
}

pub fn filter_suggestions_by_prefix(suggestions: &[String], prefix: &str) -> Vec<String> {
    if prefix.is_empty() {
        return suggestions.to_vec();
    }

    suggestions
        .iter()
        .filter(|candidate| suggestion_matches_completion_prefix(candidate, prefix))
        .cloned()
        .collect()
}

fn suggestion_matches_completion_prefix(candidate: &str, prefix: &str) -> bool {
    starts_with_ignore_ascii_case(candidate, prefix)
        || comparison_lhs_identifier_prefix(candidate)
            .is_some_and(|identifier| starts_with_ignore_ascii_case(identifier, prefix))
}

fn comparison_lhs_identifier_prefix(candidate: &str) -> Option<&str> {
    let lhs = candidate.split_once('=')?.0.trim_end();
    let identifier = lhs.rsplit('.').next()?.trim();
    Some(strip_matching_identifier_quotes(identifier))
}

fn strip_matching_identifier_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'`' && last == b'`') {
            return &value[1..value.len() - 1];
        }
    }
    value
}

fn starts_with_ignore_ascii_case(value: &str, prefix: &str) -> bool {
    let value_bytes = value.as_bytes();
    let prefix_bytes = prefix.as_bytes();
    value_bytes.len() >= prefix_bytes.len()
        && value_bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
}

impl Default for IntellisensePopup {
    fn default() -> Self {
        Self::new()
    }
}

// Helper function to extract the current word at cursor position (Unicode-aware).
// cursor_pos is a byte offset from FLTK TextBuffer.
fn normalize_cursor_pos(text: &str, cursor_pos: usize) -> usize {
    if text.is_empty() {
        return 0;
    }

    let idx = cursor_pos.min(text.len());
    if text.is_char_boundary(idx) {
        return idx;
    }

    // Clamp invalid UTF-8 byte offsets to the previous valid boundary.
    let mut clamped = idx;
    while clamped > 0 && !text.is_char_boundary(clamped) {
        clamped -= 1;
    }
    clamped
}

pub fn get_word_at_cursor(text: &str, cursor_pos: usize) -> (String, usize, usize) {
    if text.is_empty() || cursor_pos == 0 {
        return (String::new(), 0, 0);
    }

    let raw_pos = cursor_pos.min(text.len());
    let pos = normalize_cursor_pos(text, raw_pos);
    let cursor_was_non_boundary = raw_pos < text.len() && raw_pos != pos;
    let effective_pos = if cursor_was_non_boundary {
        // If FLTK gives an invalid byte offset in the middle of a UTF-8 character,
        // advance to the end of the current identifier so prefix extraction remains stable.
        let mut p = pos;
        while p < text.len() {
            let Some(ch) = text[p..].chars().next() else {
                break;
            };
            if sql_text::is_identifier_char(ch) {
                p += ch.len_utf8();
            } else {
                break;
            }
        }
        p
    } else {
        pos
    };

    // Find word start by scanning backwards over identifier characters.
    let mut start = effective_pos;
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
    let mut end = effective_pos;
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

    let word = text.get(start..effective_pos).unwrap_or("").to_string();
    (word, start, end)
}

// Detect context for smarter suggestions (after FROM, after SELECT, etc.)
// Uses the deep context analyzer for accurate depth-aware detection.
pub fn detect_sql_context(text: &str, cursor_pos: usize) -> SqlContext {
    use crate::ui::intellisense_context;
    use crate::ui::sql_editor::query_text::tokenize_sql_spanned;

    let end = normalize_cursor_pos(text, cursor_pos);
    let token_spans = tokenize_sql_spanned(text);
    let split_idx = token_spans.partition_point(|span| span.end <= end);
    let full_tokens = token_spans
        .into_iter()
        .map(|span| span.token)
        .collect::<Vec<_>>();
    let ctx = intellisense_context::analyze_cursor_context_owned(full_tokens, split_idx);

    sql_context_for_phase(ctx.phase)
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum SqlContext {
    General,
    TableName,
    ColumnName,
    ColumnOrAll,
    VariableName,
    BindValue,
    GeneratedName,
}

pub(crate) fn sql_context_for_phase(
    phase: crate::ui::intellisense_context::SqlPhase,
) -> SqlContext {
    use crate::ui::intellisense_context::SqlPhase;

    match phase {
        SqlPhase::FromClause
        | SqlPhase::IntoClause
        | SqlPhase::UpdateTarget
        | SqlPhase::DeleteTarget
        | SqlPhase::MergeTarget => SqlContext::TableName,
        SqlPhase::SelectIntoTarget
        | SqlPhase::FetchIntoTarget
        | SqlPhase::ExecuteIntoTarget
        | SqlPhase::ReturningIntoTarget => SqlContext::VariableName,
        SqlPhase::UsingBindList => SqlContext::BindValue,
        SqlPhase::SelectList => SqlContext::ColumnOrAll,
        SqlPhase::CteColumnList
        | SqlPhase::ConflictTargetList
        | SqlPhase::JoinUsingColumnList
        | SqlPhase::RecursiveCteColumnList
        | SqlPhase::DmlSetTargetList
        | SqlPhase::InsertColumnList
        | SqlPhase::MergeInsertColumnList
        | SqlPhase::DmlReturningList
        | SqlPhase::LockingColumnList
        | SqlPhase::WhereClause
        | SqlPhase::JoinCondition
        | SqlPhase::GroupByClause
        | SqlPhase::HavingClause
        | SqlPhase::OrderByClause
        | SqlPhase::SetClause
        | SqlPhase::ValuesClause
        | SqlPhase::ConnectByClause
        | SqlPhase::StartWithClause
        | SqlPhase::PivotClause
        | SqlPhase::MatchRecognizeClause
        | SqlPhase::ModelClause => SqlContext::ColumnName,
        SqlPhase::RecursiveCteGeneratedColumnName | SqlPhase::HierarchicalGeneratedColumnName => {
            SqlContext::GeneratedName
        }
        _ => SqlContext::General,
    }
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
    fn get_suggestions_include_char_keyword() {
        let mut data = IntellisenseData::new();
        let suggestions = data.get_suggestions("ch", false, None, false, false);

        assert!(suggestions.iter().any(|s| s == "CHAR"));
        assert!(!suggestions.iter().any(|s| s == "CHAR()"));
    }

    #[test]
    fn get_suggestions_include_plsql_diagnostics_as_bare_keywords() {
        let mut data = IntellisenseData::new();
        let suggestions = data.get_suggestions("sqlc", false, None, false, false);

        assert!(suggestions.iter().any(|s| s == "SQLCODE"));
        assert!(!suggestions.iter().any(|s| s == "SQLCODE()"));
    }

    #[test]
    fn get_suggestions_include_mysql_control_and_cast_keywords() {
        let mut data = IntellisenseData::new();

        let do_suggestions = data.get_suggestions_for_db(
            "do",
            false,
            None,
            false,
            false,
            Some(crate::db::DatabaseType::MySQL),
        );
        assert!(do_suggestions.iter().any(|s| s == "DO"));

        let close_suggestions = data.get_suggestions_for_db(
            "clo",
            false,
            None,
            false,
            false,
            Some(crate::db::DatabaseType::MySQL),
        );
        assert!(close_suggestions.iter().any(|s| s == "CLOSE"));

        let signed_suggestions = data.get_suggestions_for_db(
            "sig",
            false,
            None,
            false,
            false,
            Some(crate::db::DatabaseType::MySQL),
        );
        assert!(signed_suggestions.iter().any(|s| s == "SIGNED"));

        let found_suggestions = data.get_suggestions_for_db(
            "fou",
            false,
            None,
            false,
            false,
            Some(crate::db::DatabaseType::MySQL),
        );
        assert!(found_suggestions.iter().any(|s| s == "FOUND"));
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
    fn get_suggestions_table_context_empty_prefix_returns_empty_when_no_relations() {
        let mut data = IntellisenseData::new();

        let suggestions = data.get_suggestions("", false, None, true, false);

        // Keywords are not added for empty prefix – only context-specific
        // entries (tables/views/columns) are shown.
        assert!(suggestions.is_empty());
    }

    #[test]
    fn get_suggestions_column_context_empty_prefix_returns_tables_when_columns_missing() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["EMP".to_string()];
        data.rebuild_indices();
        let column_scope = vec!["EMP".to_string()];

        let suggestions = data.get_suggestions("", true, Some(&column_scope), false, true);

        // No columns loaded for EMP and keywords are not added for empty
        // prefix, but table names are still returned.
        assert!(suggestions.contains(&"EMP".to_string()));
        assert!(!suggestions.contains(&"SELECT".to_string()));
    }

    #[test]
    fn get_relation_suggestions_non_empty_prefix_stays_relation_only() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["CONFIG".to_string()];
        data.views = vec!["COUNTS_VIEW".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_relation_suggestions("co");

        assert!(suggestions.iter().any(|s| s == "CONFIG"));
        assert!(suggestions.iter().any(|s| s == "COUNTS_VIEW"));
        assert!(!suggestions.iter().any(|s| s == "COLUMN"));
        assert!(!suggestions.iter().any(|s| s == "COALESCE()"));
        assert!(!suggestions.iter().any(|s| s == "COUNT()"));
    }

    #[test]
    fn get_word_at_cursor_supports_unicode_identifier() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let cursor = sql.find(" FROM").unwrap_or(sql.len());
        let (word, _, _) = get_word_at_cursor(sql, cursor);
        assert_eq!(word, "한글컬럼");
    }

    #[test]
    fn get_word_at_cursor_clamps_non_boundary_utf8_offset() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let cursor = sql.find('한').expect("expected utf-8 anchor") + 1;
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
    fn normalize_cursor_pos_clamps_non_boundary_utf8_offset() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let utf8_start = sql.find('한').expect("expected utf-8 anchor");
        let mid_char = utf8_start + 1;
        assert!(!sql.is_char_boundary(mid_char));
        assert_eq!(normalize_cursor_pos(sql, mid_char), utf8_start);
    }

    #[test]
    fn detect_sql_context_clamps_non_boundary_utf8_offset() {
        let sql = "SELECT 한글컬럼 FROM dual";
        let utf8_start = sql.find('한').expect("expected utf-8 anchor");
        let mid_char = utf8_start + 1;
        assert!(!sql.is_char_boundary(mid_char));
        assert_eq!(
            detect_sql_context(sql, mid_char),
            detect_sql_context(sql, utf8_start)
        );
    }

    #[test]
    fn detect_sql_context_qualify_clause_is_column_name() {
        let sql_with_cursor = "SELECT a FROM t QUALIFY |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_returning_clause_is_column_name() {
        let sql_with_cursor = "INSERT INTO t (a) VALUES (1) RETURNING | INTO :a";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_update_set_target_is_column_name() {
        let sql_with_cursor = "UPDATE emp SET |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_merge_update_set_target_is_column_name() {
        let sql_with_cursor =
            "MERGE INTO tgt t USING src s ON (t.id = s.id) WHEN MATCHED THEN UPDATE SET |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_select_into_target_is_variable_name() {
        let sql_with_cursor = "BEGIN SELECT empno INTO | FROM emp; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::VariableName);
    }

    #[test]
    fn detect_sql_context_returning_into_target_is_variable_name() {
        let sql_with_cursor = "UPDATE emp SET sal = sal + 1 RETURNING empno INTO |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::VariableName);
    }

    #[test]
    fn detect_sql_context_fetch_into_target_is_variable_name() {
        let sql_with_cursor = "BEGIN FETCH c_emp INTO |; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::VariableName);
    }

    #[test]
    fn detect_sql_context_execute_immediate_using_is_bind_value() {
        let sql_with_cursor =
            "BEGIN EXECUTE IMMEDIATE 'select count(*) from emp where deptno = :1' INTO l_cnt USING |; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::BindValue);
    }

    #[test]
    fn detect_sql_context_open_for_using_is_bind_value() {
        let sql_with_cursor = "BEGIN OPEN c FOR l_sql USING |; END;";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::BindValue);
    }

    #[test]
    fn detect_sql_context_join_using_clause_is_column_name() {
        let sql_with_cursor = "SELECT * FROM employees e JOIN departments d USING (|)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_on_conflict_target_list_is_column_name() {
        let sql_with_cursor =
            "INSERT INTO t (id, val) VALUES (1, 2) ON CONFLICT (|) DO UPDATE SET val = EXCLUDED.val";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_recursive_cte_search_by_is_column_name() {
        let sql_with_cursor =
            "WITH t(n) AS (SELECT 1 FROM dual UNION ALL SELECT n + 1 FROM t WHERE n < 3) SEARCH DEPTH FIRST BY | SET ord SELECT * FROM t";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_recursive_cte_cycle_set_is_generated_name() {
        let sql_with_cursor =
            "WITH t(n) AS (SELECT 1 FROM dual UNION ALL SELECT n + 1 FROM t WHERE n < 3) CYCLE n SET | TO 1 DEFAULT 0 SELECT * FROM t";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::GeneratedName);
    }

    #[test]
    fn detect_sql_context_hierarchical_search_set_is_generated_name() {
        let sql_with_cursor =
            "SELECT * FROM emp CONNECT BY PRIOR empno = mgr SEARCH DEPTH FIRST BY empno SET |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::GeneratedName);
    }

    #[test]
    fn detect_sql_context_hierarchical_cycle_set_is_generated_name() {
        let sql_with_cursor =
            "SELECT * FROM emp CONNECT BY PRIOR empno = mgr CYCLE empno SET | TO 'Y' DEFAULT 'N'";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::GeneratedName);
    }

    #[test]
    fn detect_sql_context_for_update_of_is_column_name() {
        let sql_with_cursor = "SELECT * FROM emp FOR UPDATE OF |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_pivot_sum_argument_is_column_name() {
        let sql_with_cursor = "WITH s AS (SELECT DEPTNO, job, sal FROM oqt_t_emp) SELECT * FROM s PIVOT (SUM(|) AS sum_sal FOR DEPTNO IN (10 AS D10))";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_pivot_for_expression_is_column_name() {
        let sql_with_cursor = "WITH s AS (SELECT DEPTNO, job, sal FROM oqt_t_emp) SELECT * FROM s PIVOT (SUM(sal) AS sum_sal FOR | IN (10 AS D10))";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_match_recognize_define_is_column_name() {
        let sql_with_cursor =
            "SELECT * FROM sales MATCH_RECOGNIZE (PARTITION BY dept ORDER BY ts DEFINE A AS |)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_model_measures_is_column_name() {
        let sql_with_cursor =
            "SELECT * FROM sales MODEL DIMENSION BY (deptno) MEASURES (|) RULES ()";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_insert_values_clause_is_column_name() {
        let sql_with_cursor = "INSERT INTO t (id, val) VALUES (|)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_merge_insert_values_clause_is_column_name() {
        let sql_with_cursor = "MERGE INTO tgt t USING src s ON (t.id = s.id) WHEN NOT MATCHED THEN INSERT (id) VALUES (s.|)";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_insert_all_into_clause_is_table_name() {
        let sql_with_cursor = "INSERT ALL INTO | (id) VALUES (1) SELECT 1 FROM dual";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::TableName);
    }

    #[test]
    fn detect_sql_context_insert_all_after_first_values_into_clause_is_table_name() {
        let sql_with_cursor =
            "INSERT ALL INTO t1 (id) VALUES (1) INTO | (id) VALUES (2) SELECT 1 FROM dual";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::TableName);
    }

    #[test]
    fn detect_sql_context_insert_first_else_into_clause_is_table_name() {
        let sql_with_cursor =
            "INSERT FIRST WHEN score >= 90 THEN INTO top_rank (id) VALUES (1) ELSE INTO | (id) VALUES (2) SELECT 1 score FROM dual";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::TableName);
    }

    #[test]
    fn detect_sql_context_outer_apply_rhs_is_table_name() {
        let sql_with_cursor = "SELECT * FROM t1 OUTER APPLY |";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::TableName);
    }

    #[test]
    fn detect_sql_context_with_cte_explicit_column_list_is_column_name() {
        let sql_with_cursor = "WITH cte(id, |) AS (SELECT 1, 2 FROM dual) SELECT * FROM cte";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn detect_sql_context_second_cte_explicit_column_list_is_column_name() {
        let sql_with_cursor =
            "WITH c1(a) AS (SELECT 1 FROM dual), c2(x, |) AS (SELECT 1, 2 FROM dual) SELECT * FROM c2";
        let cursor = sql_with_cursor
            .find('|')
            .expect("expected cursor marker in SQL");
        let sql = format!(
            "{}{}",
            &sql_with_cursor[..cursor],
            &sql_with_cursor[cursor + 1..]
        );
        assert_eq!(detect_sql_context(&sql, cursor), SqlContext::ColumnName);
    }

    #[test]
    fn get_suggestions_includes_exact_prefix_match() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["AB".to_string(), "ABC_TABLE".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_suggestions("ab", false, None, false, false);

        assert!(suggestions.iter().any(|s| s.eq_ignore_ascii_case("ab")));
        assert!(suggestions
            .iter()
            .any(|s| s.eq_ignore_ascii_case("abc_table")));
    }

    #[test]
    fn get_suggestions_includes_exact_keyword_match() {
        let mut data = IntellisenseData::new();

        let suggestions = data.get_suggestions("as", false, None, false, false);

        assert!(suggestions.iter().any(|s| s.eq_ignore_ascii_case("as")));
    }

    #[test]
    fn get_suggestions_includes_exact_function_prefix_match() {
        let mut data = IntellisenseData::new();

        let suggestions = data.get_suggestions("sum", false, None, false, false);

        assert!(suggestions.iter().any(|s| s.eq_ignore_ascii_case("sum()")));
    }

    #[test]
    fn filter_suggestions_by_prefix_empty_prefix_keeps_all() {
        let suggestions = vec!["SELECT".to_string(), "FROM".to_string()];
        let filtered = filter_suggestions_by_prefix(&suggestions, "");
        assert_eq!(filtered, suggestions);
    }

    #[test]
    fn filter_suggestions_by_prefix_case_insensitive_and_underscore() {
        let suggestions = vec![
            "TO_CHAR".to_string(),
            "to_date".to_string(),
            "TABLE".to_string(),
        ];
        let filtered = filter_suggestions_by_prefix(&suggestions, "to_");
        assert_eq!(filtered, vec!["TO_CHAR".to_string(), "to_date".to_string()]);
    }

    #[test]
    fn filter_suggestions_by_prefix_no_match_returns_empty() {
        let suggestions = vec!["SELECT".to_string(), "FROM".to_string()];
        let filtered = filter_suggestions_by_prefix(&suggestions, "zz");
        assert!(filtered.is_empty());
    }

    #[test]
    fn filter_suggestions_by_prefix_matches_condition_comparison_left_column() {
        let suggestions = vec![
            "a.TOTAL = b.TOTAL".to_string(),
            "a.NAME = b.NAME".to_string(),
        ];
        let filtered = filter_suggestions_by_prefix(&suggestions, "to");
        assert_eq!(filtered, vec!["a.TOTAL = b.TOTAL".to_string()]);
    }

    #[test]
    fn filter_suggestions_by_prefix_matches_quoted_condition_comparison_left_column() {
        let suggestions = vec![
            "a.\"Order Id\" = b.\"Order Id\"".to_string(),
            "a.\"Dept No\" = b.\"Dept No\"".to_string(),
        ];
        let filtered = filter_suggestions_by_prefix(&suggestions, "or");
        assert_eq!(
            filtered,
            vec!["a.\"Order Id\" = b.\"Order Id\"".to_string()]
        );
    }

    #[test]
    fn popup_page_selection_advances_by_page_size_and_clamps_to_end() {
        assert_eq!(IntellisensePopup::next_page_selection(1, 25), Some(11));
        assert_eq!(IntellisensePopup::next_page_selection(20, 25), Some(25));
        assert_eq!(IntellisensePopup::next_page_selection(0, 7), Some(7));
        assert_eq!(IntellisensePopup::next_page_selection(1, 0), None);
    }

    #[test]
    fn popup_page_selection_moves_up_by_page_size_and_clamps_to_start() {
        assert_eq!(IntellisensePopup::prev_page_selection(21, 30), Some(11));
        assert_eq!(IntellisensePopup::prev_page_selection(5, 30), Some(1));
        assert_eq!(IntellisensePopup::prev_page_selection(0, 8), Some(1));
        assert_eq!(IntellisensePopup::prev_page_selection(3, 0), None);
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
    fn sql_keywords_is_sorted() {
        for pair in SQL_KEYWORDS.windows(2) {
            assert!(
                pair[0] <= pair[1],
                "SQL_KEYWORDS not sorted: {:?} > {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn oracle_functions_is_sorted() {
        for pair in ORACLE_FUNCTIONS.windows(2) {
            assert!(
                pair[0] <= pair[1],
                "ORACLE_FUNCTIONS not sorted: {:?} > {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn get_suggestions_deduplicates_case_insensitive_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["EmpNo".to_string(), "EMPNO".to_string()]);
        let column_scope = vec!["emp".to_string()];

        let suggestions = data.get_suggestions("", true, Some(&column_scope), false, true);

        let empno_count = suggestions
            .iter()
            .filter(|value| value.eq_ignore_ascii_case("EMPNO"))
            .count();
        assert_eq!(empno_count, 1);
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions
            .iter()
            .any(|value| value.eq_ignore_ascii_case("EMPNO")));
    }

    #[test]
    fn get_suggestions_deduplicates_case_insensitive_relations() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["Emp".to_string(), "EMP".to_string(), "emp2".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_suggestions("", false, None, true, false);
        let emp_count = suggestions
            .iter()
            .filter(|value| value.eq_ignore_ascii_case("EMP"))
            .count();
        assert_eq!(emp_count, 1);
        assert!(suggestions
            .iter()
            .any(|value| value.eq_ignore_ascii_case("EMP")));
    }

    #[test]
    fn virtual_table_columns_do_not_remove_real_table_columns() {
        let mut data = IntellisenseData::new();
        data.set_columns_for_table("EMP", vec!["REAL_COL".to_string()]);
        data.set_virtual_table_columns("EMP", vec!["VIRTUAL_COL".to_string()]);
        data.clear_virtual_tables();

        let columns = data.get_column_suggestions("", Some(&["EMP".to_string()]));
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

        let columns = data.get_column_suggestions("", Some(&["EMP".to_string()]));
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

    #[test]
    fn get_relation_suggestions_include_synonyms() {
        let mut data = IntellisenseData::new();
        data.tables = vec!["EMP".to_string()];
        data.synonyms = vec!["EMP_SYN".to_string()];
        data.public_synonyms = vec!["PUBLIC_EMP".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_relation_suggestions("P");

        assert!(suggestions.iter().any(|name| name == "PUBLIC_EMP"));
        assert!(!suggestions.iter().any(|name| name == "PACKAGE"));
    }

    #[test]
    fn get_relation_suggestions_include_users_for_schema_qualification() {
        let mut data = IntellisenseData::new();
        data.users = vec!["SCOTT".to_string(), "SYS".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_relation_suggestions("SC");

        assert_eq!(suggestions, vec!["SCOTT".to_string()]);
    }

    #[test]
    fn get_object_suggestions_include_packages_sequences_and_synonyms() {
        let mut data = IntellisenseData::new();
        data.procedures = vec!["RUN_JOB".to_string()];
        data.packages = vec!["UTIL_PKG".to_string()];
        data.sequences = vec!["SEQ_ORDER".to_string()];
        data.synonyms = vec!["JOB_SYN".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_object_suggestions("");

        assert!(suggestions.iter().any(|name| name == "RUN_JOB"));
        assert!(suggestions.iter().any(|name| name == "UTIL_PKG"));
        assert!(suggestions.iter().any(|name| name == "SEQ_ORDER"));
        assert!(suggestions.iter().any(|name| name == "JOB_SYN"));
    }

    #[test]
    fn get_object_suggestions_include_users_for_schema_qualification() {
        let mut data = IntellisenseData::new();
        data.users = vec!["SCOTT".to_string()];
        data.rebuild_indices();

        let suggestions = data.get_object_suggestions("SC");

        assert_eq!(suggestions, vec!["SCOTT".to_string()]);
    }

    #[test]
    fn get_member_suggestions_use_package_and_schema_qualifiers() {
        let mut data = IntellisenseData::new();
        data.set_members_for_qualifier(
            "DEMO_PKG",
            vec!["RUN_JOB".to_string(), "CALC_BONUS".to_string()],
        );
        data.set_members_for_qualifier(
            "SCOTT",
            vec![
                "EMP".to_string(),
                "EMP_API".to_string(),
                "SEQ_EMP".to_string(),
            ],
        );
        data.set_relation_members_for_qualifier(
            "SCOTT",
            vec!["EMP".to_string(), "EMP_VIEW".to_string()],
        );

        let package_members = data.get_member_suggestions("demo_pkg", "R", false);
        let schema_members = data.get_member_suggestions("scott", "EMP", false);
        let schema_relations = data.get_member_suggestions("scott", "EMP", true);

        assert_eq!(package_members, vec!["RUN_JOB".to_string()]);
        assert!(schema_members.iter().any(|name| name == "EMP_API"));
        assert!(schema_relations.iter().any(|name| name == "EMP_VIEW"));
        assert!(!schema_relations.iter().any(|name| name == "EMP_API"));
    }

    #[test]
    fn invoke_selected_callback_preserves_replaced_callback() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(String)>>>> = Arc::new(Mutex::new(None));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let callback_slot_for_first = callback_slot.clone();
        let calls_for_first = calls.clone();
        *callback_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Box::new(move |value: String| {
                calls_for_first
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(format!("first:{value}"));
                let calls_for_second = calls_for_first.clone();
                *callback_slot_for_first
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some(Box::new(move |next: String| {
                        calls_for_second
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner())
                            .push(format!("second:{next}"));
                    }));
            }));

        IntellisensePopup::invoke_selected_callback(&callback_slot, "alpha".to_string());
        IntellisensePopup::invoke_selected_callback(&callback_slot, "beta".to_string());

        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            ["first:alpha".to_string(), "second:beta".to_string()]
        );
    }

    #[test]
    fn invoke_selected_callback_restores_original_after_panic() {
        let callback_slot: Arc<Mutex<Option<Box<dyn FnMut(String)>>>> = Arc::new(Mutex::new(None));
        let calls = Arc::new(Mutex::new(Vec::new()));

        let calls_for_cb = calls.clone();
        *callback_slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Some(Box::new(move |value: String| {
                calls_for_cb
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(value.clone());
                if value == "panic" {
                    panic!("expected test panic");
                }
            }));

        IntellisensePopup::invoke_selected_callback(&callback_slot, "panic".to_string());
        IntellisensePopup::invoke_selected_callback(&callback_slot, "ok".to_string());

        assert_eq!(
            calls
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            ["panic".to_string(), "ok".to_string()]
        );
    }
}
