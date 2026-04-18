use space_query::db::connection::DatabaseType;
use space_query::db::oracle_thin::{
    close as close_oracle_thin, connect as connect_oracle_thin, execute_select_all_with_binds,
};
use space_query::db::ConnectionInfo;
use std::env;

fn main() {
    let info = ConnectionInfo {
        name: "oracle-session-probe".to_string(),
        username: env::var("ORACLE_TEST_USER").unwrap_or_else(|_| "system".to_string()),
        password: env::var("ORACLE_TEST_PASSWORD").unwrap_or_else(|_| "password".to_string()),
        host: env::var("ORACLE_TEST_HOST").unwrap_or_else(|_| "localhost".to_string()),
        port: env::var("ORACLE_TEST_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1521),
        service_name: env::var("ORACLE_TEST_SERVICE").unwrap_or_else(|_| "FREE".to_string()),
        db_type: DatabaseType::OracleThin,
    };

    let conn = match connect_oracle_thin(&info) {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("failed to connect Oracle thin probe session: {err}");
            std::process::exit(1);
        }
    };
    let result = match execute_select_all_with_binds(
        conn.as_ref(),
        r#"
        SELECT s.sid,
               s.serial#,
               s.status,
               s.state,
               s.event,
               NVL(TO_CHAR(s.blocking_session), '-') AS blocking_sid,
               NVL(s.module, '-') AS module_name,
               NVL(s.action, '-') AS action_name,
               NVL(s.sql_id, '-') AS sql_id,
               NVL(s.prev_sql_id, '-') AS prev_sql_id,
               SUBSTR(NVL(q.sql_text, '-'), 1, 200) AS sql_text
        FROM sys.v_$session s
        LEFT JOIN sys.v_$sql q
          ON q.sql_id = s.sql_id
        WHERE s.type <> 'BACKGROUND'
          AND s.sid <> TO_NUMBER(SYS_CONTEXT('USERENV', 'SID'))
        ORDER BY s.logon_time
        "#,
        &[],
    ) {
        Ok(result) => result,
        Err(err) => {
            eprintln!("failed to query Oracle thin probe session: {err}");
            let _ = close_oracle_thin(conn.as_ref());
            std::process::exit(1);
        }
    };

    for row in result.rows {
        println!("{}", row.join(" | "));
    }

    if let Err(err) = close_oracle_thin(conn.as_ref()) {
        eprintln!("failed to close Oracle thin probe session: {err}");
        std::process::exit(1);
    }
}
