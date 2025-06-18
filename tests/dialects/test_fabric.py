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
            write={"fabric": "CREATE TABLE test (id INTEGER, amount DECIMAL(19,4))"},
        )

        self.validate_all(
            "CREATE TABLE test (small_amount SMALLMONEY, blob_data IMAGE)",
            write={
                "fabric": "CREATE TABLE test (small_amount DECIMAL(10,4), blob_data VARBINARY(MAX))"
            },
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
