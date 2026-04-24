# Local MySQL 8 Test Database

- Container: `space-query-mysql80`
- Image: `mysql:8.0`
- Server version: `8.0.46`
- Host: `127.0.0.1`
- Port: `3307`
- Database: `query_tool_mysql8`
- Username: `root`
- Password: `spacequery`

```bash
mysql --protocol=TCP -h 127.0.0.1 -P 3307 -uroot -pspacequery query_tool_mysql8
```

Rust test environment:

```bash
SPACE_QUERY_TEST_MYSQL_HOST=127.0.0.1
SPACE_QUERY_TEST_MYSQL_PORT=3307
SPACE_QUERY_TEST_MYSQL_DATABASE=query_tool_mysql8
SPACE_QUERY_TEST_MYSQL_USER=root
SPACE_QUERY_TEST_MYSQL_PASSWORD=spacequery
```

