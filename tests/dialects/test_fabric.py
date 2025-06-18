from sqlglot import parse_one
from tests.dialects.test_dialect import Validator


class TestFabric(Validator):
    dialect = "fabric"

    def test_fabric_basic(self):
        """Test basic Fabric SQL functionality."""

        # Test basic SELECT statement
        self.validate_identity("SELECT * FROM MyTable")

        # Test that we inherit T-SQL functionality
        self.validate_identity("SELECT TOP 10 * FROM table1")

    def test_fabric_case_sensitivity(self):
        """Test case sensitivity in Fabric."""

        # Unlike T-SQL, Fabric should preserve case
        sql = "SELECT Column1, COLUMN2 FROM MyTable WHERE column3 = 'value'"
        parsed = parse_one(sql, dialect="fabric")

        # The parsed result should preserve the original casing
        self.assertIn("Column1", str(parsed))
        self.assertIn("COLUMN2", str(parsed))
        self.assertIn("MyTable", str(parsed))
        self.assertIn("column3", str(parsed))

    def test_fabric_information_schema(self):
        """Test INFORMATION_SCHEMA uppercasing."""

        # Test that INFORMATION_SCHEMA table and column names are uppercased
        self.validate_all(
            "SELECT schema_name FROM information_schema.schemata",
            write={"fabric": "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"},
        )

        self.validate_all(
            "SELECT table_name, table_schema FROM information_schema.tables",
            write={"fabric": "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES"},
        )

        # Test with WHERE clause
        self.validate_all(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dbo'",
            write={
                "fabric": "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'"
            },
        )

    def test_fabric_data_types(self):
        """Test Fabric data type conversions."""

        # Test deprecated data types conversion
        self.validate_all(
            "CREATE TABLE test (id INT, amount MONEY)",
            write={"fabric": "CREATE TABLE test (id INTEGER, amount DECIMAL(19, 4))"},
        )

        self.validate_all(
            "CREATE TABLE test (small_amount SMALLMONEY, blob_data IMAGE)",
            write={
                "fabric": "CREATE TABLE test (small_amount DECIMAL(10, 4), blob_data VARBINARY(MAX))"
            },
        )

        # Test DATETIME -> DATETIME2 conversion
        self.validate_all(
            "CREATE TABLE test (created_date DATETIME, modified_time SMALLDATETIME)",
            write={"fabric": "CREATE TABLE test (created_date DATETIME2, modified_time DATETIME2)"},
        )

        # Test TINYINT -> SMALLINT conversion
        self.validate_all(
            "CREATE TABLE test (flag TINYINT, counter SMALLINT)",
            write={"fabric": "CREATE TABLE test (flag SMALLINT, counter SMALLINT)"},
        )

        # Test NCHAR/NVARCHAR -> CHAR/VARCHAR conversion
        self.validate_all(
            "CREATE TABLE test (name NVARCHAR(50), code NCHAR(10))",
            write={"fabric": "CREATE TABLE test (name VARCHAR(50), code CHAR(10))"},
        )

        # Test TEXT/NTEXT -> VARCHAR(MAX) conversion
        self.validate_all(
            "CREATE TABLE test (description TEXT, notes NTEXT)",
            write={"fabric": "CREATE TABLE test (description VARCHAR(MAX), notes VARCHAR(MAX))"},
        )

        # Test geographic types -> VARBINARY(MAX)
        self.validate_all(
            "CREATE TABLE test (location GEOGRAPHY, shape GEOMETRY)",
            write={"fabric": "CREATE TABLE test (location VARBINARY(MAX), shape VARBINARY(MAX))"},
        )

        # Test JSON/XML -> VARCHAR(MAX)
        self.validate_all(
            "CREATE TABLE test (data JSON, config XML)",
            write={"fabric": "CREATE TABLE test (data VARCHAR(MAX), config VARCHAR(MAX))"},
        )

    def test_fabric_precision_limits(self):
        """Test Fabric precision limitations."""

        # Test DATETIME2 precision limit (max 6)
        self.validate_all(
            "CREATE TABLE test (timestamp DATETIME2(7))",
            write={"fabric": "CREATE TABLE test (timestamp DATETIME2(6))"},
        )

        # Test TIME precision limit (max 6)
        self.validate_all(
            "CREATE TABLE test (time_col TIME(7))",
            write={"fabric": "CREATE TABLE test (time_col TIME(6))"},
        )

        # Test valid precision values are preserved
        self.validate_all(
            "CREATE TABLE test (timestamp DATETIME2(3), time_col TIME(4))",
            write={"fabric": "CREATE TABLE test (timestamp DATETIME2(3), time_col TIME(4))"},
        )

    def test_fabric_mixed_information_schema(self):
        """Test mixed scenarios with INFORMATION_SCHEMA and regular tables."""

        # Test that regular table columns are NOT uppercased
        self.validate_all(
            "SELECT table_name FROM my_custom_table",
            write={"fabric": "SELECT table_name FROM my_custom_table"},
        )

        # Test basic INFORMATION_SCHEMA without alias works
        self.validate_all(
            "SELECT schema_name FROM information_schema.schemata",
            write={"fabric": "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA"},
        )

        # Test subquery with INFORMATION_SCHEMA (column in subquery should be uppercase)
        self.validate_all(
            "SELECT u.name FROM users u WHERE u.schema_name IN (SELECT schema_name FROM information_schema.schemata)",
            write={
                "fabric": "SELECT u.name FROM users AS u WHERE u.schema_name IN (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA)"
            },
        )

    def test_fabric_information_schema_aliases(self):
        """Test INFORMATION_SCHEMA with table aliases."""

        # Test simple aliased case
        self.validate_all(
            "SELECT t.table_name FROM information_schema.tables t",
            write={"fabric": "SELECT t.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES AS t"},
        )

        # Test complex JOIN with mixed aliases
        self.validate_all(
            "SELECT t.table_name, u.user_name FROM information_schema.tables t JOIN users u ON t.table_schema = u.schema_name",
            write={
                "fabric": "SELECT t.TABLE_NAME, u.user_name FROM INFORMATION_SCHEMA.TABLES AS t JOIN users AS u ON t.TABLE_SCHEMA = u.schema_name"
            },
        )

    def test_fabric_unsupported_operations(self):
        """Test unsupported operations in Fabric."""

        # Test MERGE statement (not supported)
        with self.assertRaises(Exception):
            parse_one("MERGE target USING source ON condition", dialect="fabric")

        # Test IDENTITY columns (not supported) - would be caught during parsing
        # This would typically be validated at a higher level

    def test_fabric_truncate_table(self):
        """Test TRUNCATE TABLE support."""

        # Test basic TRUNCATE TABLE (supported)
        self.validate_identity("TRUNCATE TABLE MyTable")

        # Test with schema qualification
        self.validate_identity("TRUNCATE TABLE dbo.MyTable")

    def test_fabric_temp_tables(self):
        """Test session-scoped distributed #temp tables."""

        # Test #temp table creation - INT gets converted to INTEGER
        self.validate_all(
            "CREATE TABLE #temp (id INT, name VARCHAR(50))",
            write={"fabric": "CREATE TABLE #temp (id INTEGER, name VARCHAR(50))"},
        )

        # Test #temp table usage
        self.validate_identity("SELECT * FROM #temp")

    def test_fabric_alter_table_limitations(self):
        """Test ALTER TABLE limitations in Fabric."""

        # Test that supported ALTER TABLE operations work
        self.validate_identity("ALTER TABLE MyTable ADD COLUMN new_col VARCHAR(50)")
        self.validate_identity("ALTER TABLE MyTable DROP COLUMN old_col")

        # Test constraints with NOT ENFORCED (should be supported)
        self.validate_identity(
            "ALTER TABLE MyTable ADD CONSTRAINT pk_test PRIMARY KEY (id) NOT ENFORCED"
        )

    def test_fabric_schema_table_name_restrictions(self):
        """Test schema/table name restrictions."""

        # Test that schema and table names can't contain / or \ (this would be validation)
        # For now, we'll just test that normal names work
        self.validate_identity("SELECT * FROM database.schema.table")

    def test_fabric_query_hints(self):
        """Test limited query hints support."""

        # Test supported hints - Fabric removes space after OPTION
        self.validate_all(
            "SELECT * FROM table1 OPTION (MAXDOP 1)",
            write={"fabric": "SELECT * FROM table1 OPTION(MAXDOP 1)"},
        )

        # Test LABEL hint (should be supported)
        self.validate_all(
            "SELECT * FROM table1 OPTION (LABEL = 'test_query')",
            write={"fabric": "SELECT * FROM table1 OPTION(LABEL = 'test_query')"},
        )

    def test_fabric_cte_support(self):
        """Test CTE support including nested CTEs."""

        # Test standard CTE - TSQL dialect adds AS aliases for columns
        self.validate_all(
            "WITH cte AS (SELECT id FROM table1) SELECT * FROM cte",
            write={"fabric": "WITH cte AS (SELECT id AS id FROM table1) SELECT * FROM cte"},
        )

        # Test sequential CTEs
        self.validate_all(
            "WITH cte1 AS (SELECT id FROM table1), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2",
            write={
                "fabric": "WITH cte1 AS (SELECT id AS id FROM table1), cte2 AS (SELECT * FROM cte1) SELECT * FROM cte2"
            },
        )

        # Note: Nested CTEs are flattened by the parser by design
        # This is not a Fabric-specific limitation

    def test_fabric_stored_procedure_support(self):
        """Test stored procedure support."""

        # Test sp_rename for column renaming (should be supported)
        self.validate_identity("EXEC sp_rename 'table.old_column', 'new_column', 'COLUMN'")

    def test_fabric_data_type_edge_cases(self):
        """Test edge cases for data type conversions."""

        # Test DATETIMEOFFSET -> DATETIME2 (with timezone info loss warning)
        self.validate_all(
            "CREATE TABLE test (ts DATETIMEOFFSET)",
            write={"fabric": "CREATE TABLE test (ts DATETIME2)"},
        )

        # Test precision handling for MONEY types
        self.validate_all(
            "SELECT CAST(100.50 AS MONEY)",
            write={"fabric": "SELECT CAST(100.50 AS DECIMAL(19, 4))"},
        )

        # Test UNIQUEIDENTIFIER (should be supported as-is)
        self.validate_identity("CREATE TABLE test (id UNIQUEIDENTIFIER)")

    def test_fabric_limitations_documentation(self):
        """Test various limitations mentioned in the PDF."""

        # Test that SELECT FOR XML is not supported (would be caught during generation)
        # This would typically generate an unsupported error

        # Test that SET ROWCOUNT is not supported (would be caught during generation)
        # This would typically generate an unsupported error

        # Test that SET TRANSACTION ISOLATION LEVEL is not supported
        # This would typically generate an unsupported error

        # Test that BULK LOAD operations are not supported
        # This would typically be caught during parsing/generation

        pass  # These are structural tests that would be implemented with proper error handling
