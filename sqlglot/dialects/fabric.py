from __future__ import annotations

import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import NormalizationStrategy
from sqlglot.dialects.tsql import TSQL
from sqlglot.tokens import TokenType

if t.TYPE_CHECKING:
    pass


class Fabric(TSQL):
    """Microsoft Fabric SQL Analytics Endpoint dialect.

    Microsoft Fabric is a unified data platform that includes SQL Analytics Endpoints
    for data warehouses. It uses a T-SQL dialect with specific limitations and requirements.

    Key differences from standard T-SQL:
    - Case-sensitive identifiers and keywords
    - Requires fully qualified table names (database.schema.table)
    - INFORMATION_SCHEMA tables are uppercase
    - Limited T-SQL surface area (subset of T-SQL functionality)
    - Different data type support and limitations
    """

    # Fabric is case-sensitive unlike standard T-SQL
    NORMALIZATION_STRATEGY = NormalizationStrategy.CASE_SENSITIVE

    # Fabric requires fully qualified names for tables
    SUPPORTS_UNQUALIFIED_TABLES = False

    # Supported data types in Fabric Data Warehouse (based on Microsoft documentation)
    SUPPORTED_DATA_TYPES = {
        # Exact numerics
        "BIT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "DECIMAL",
        "NUMERIC",
        # Approximate numerics
        "FLOAT",
        "REAL",
        # Date and time (with precision limitations)
        "DATE",
        "TIME",
        "DATETIME2",
        # Fixed-length character strings
        "CHAR",
        # Variable length character strings
        "VARCHAR",
        # Binary strings
        "VARBINARY",
        "UNIQUEIDENTIFIER",
    }

    # Unsupported data types and their recommended alternatives
    UNSUPPORTED_DATA_TYPES = {
        "MONEY": "DECIMAL(19,4)",
        "SMALLMONEY": "DECIMAL(10,4)",
        "DATETIME": "DATETIME2",
        "SMALLDATETIME": "DATETIME2",
        "DATETIMEOFFSET": "DATETIME2",  # Note: timezone info is lost
        "NCHAR": "CHAR",  # Use CHAR with UTF-8 collation
        "NVARCHAR": "VARCHAR",  # Use VARCHAR with UTF-8 collation
        "TEXT": "VARCHAR(MAX)",
        "NTEXT": "VARCHAR(MAX)",
        "IMAGE": "VARBINARY(MAX)",
        "TINYINT": "SMALLINT",
        "GEOGRAPHY": "VARBINARY(MAX)",  # Store as WKB or lat/lng pair
        "GEOMETRY": "VARBINARY(MAX)",  # Store as WKB or lat/lng pair
        "JSON": "VARCHAR(MAX)",
        "XML": "VARCHAR(MAX)",  # No equivalent, use VARCHAR as fallback
    }

    # Data type precision limitations
    PRECISION_LIMITS = {
        "DATETIME2": 6,  # Maximum 6 digits precision for fractions of seconds
        "TIME": 6,  # Maximum 6 digits precision for fractions of seconds
    }

    class Tokenizer(TSQL.Tokenizer):
        # Fabric uses case-sensitive keywords
        CASE_SENSITIVE = True

        # Override keywords to be case-sensitive
        KEYWORDS = {
            **TSQL.Tokenizer.KEYWORDS,
            # In Fabric, we want to handle some deprecated types differently
            # but we'll keep the same token mapping for now
        }

    class Parser(TSQL.Parser):
        # Fabric has more restrictive parsing requirements

        def _parse_types(
            self, check_func: bool = False, schema: bool = False, allow_identifiers: bool = True
        ) -> t.Optional[exp.Expression]:
            """Override to handle Fabric-specific type conversions during parsing."""
            # Handle deprecated/unsupported data types during parsing

            # NTEXT -> VARCHAR(MAX)
            if self._match_text_seq("NTEXT"):
                return exp.DataType.build("VARCHAR(MAX)")

            # TEXT -> VARCHAR(MAX)
            if self._match_text_seq("TEXT") and not self._match_text_seq(
                "LONGTEXT", "MEDIUMTEXT", "TINYTEXT"
            ):
                return exp.DataType.build("VARCHAR(MAX)")

            # DATETIME -> DATETIME2
            if self._match_text_seq("DATETIME") and not self._match_text_seq("DATETIME2"):
                # Check if there's a precision specifier
                precision = None
                if self._match(TokenType.L_PAREN):
                    precision = self._parse_number()
                    self._match_r_paren()

                if precision:
                    return exp.DataType.build(f"DATETIME2({precision})")
                return exp.DataType.build("DATETIME2")

            # SMALLDATETIME -> DATETIME2
            if self._match_text_seq("SMALLDATETIME"):
                return exp.DataType.build("DATETIME2")

            # DATETIMEOFFSET -> DATETIME2 (with note that timezone info is lost)
            if self._match_text_seq("DATETIMEOFFSET"):
                # Check if there's a precision specifier
                precision = None
                if self._match(TokenType.L_PAREN):
                    precision = self._parse_number()
                    self._match_r_paren()

                if precision:
                    return exp.DataType.build(f"DATETIME2({precision})")
                return exp.DataType.build("DATETIME2")

            # TINYINT -> SMALLINT
            if self._match_text_seq("TINYINT"):
                return exp.DataType.build("SMALLINT")

            # MONEY -> DECIMAL(19,4)
            if self._match_text_seq("MONEY") and not self._match_text_seq("SMALLMONEY"):
                return exp.DataType.build("DECIMAL(19,4)")

            # SMALLMONEY -> DECIMAL(10,4)
            if self._match_text_seq("SMALLMONEY"):
                return exp.DataType.build("DECIMAL(10,4)")

            # IMAGE -> VARBINARY(MAX)
            if self._match_text_seq("IMAGE"):
                return exp.DataType.build("VARBINARY(MAX)")

            return super()._parse_types(
                check_func=check_func, schema=schema, allow_identifiers=allow_identifiers
            )

        def _parse_table_parts(
            self, schema: bool = True, is_db_reference: bool = False, wildcard: bool = False
        ) -> exp.Table:
            """Override to enforce fully qualified table names in Fabric."""
            table = super()._parse_table_parts(
                schema=schema, is_db_reference=is_db_reference, wildcard=wildcard
            )

            # In Fabric, tables should be fully qualified (database.schema.table)
            # We'll allow this to be enforced at validation time rather than parse time
            # to maintain compatibility with existing code
            return table

    class Generator(TSQL.Generator):
        # Fabric uses case-sensitive SQL
        CASE_SENSITIVE = True

        # Fabric has limitations on T-SQL features
        SUPPORTS_TABLE_HINTS = False  # Limited table hint support

        # Override type mappings for Fabric-specific data types
        TYPE_MAPPING = {
            **TSQL.Generator.TYPE_MAPPING,
            # Override TSQL's DECIMAL -> NUMERIC mapping to keep DECIMAL
            exp.DataType.Type.DECIMAL: "DECIMAL",
            # Fabric has specific data type limitations - map unsupported types to alternatives
            # Note: MONEY and SMALLMONEY are handled in datatype_sql method for precise formatting
            exp.DataType.Type.IMAGE: "VARBINARY(MAX)",  # IMAGE deprecated, use VARBINARY(MAX)
            exp.DataType.Type.TEXT: "VARCHAR(MAX)",  # TEXT not supported, use VARCHAR(MAX)
            exp.DataType.Type.NCHAR: "CHAR",  # NCHAR not supported, use CHAR with UTF-8 collation
            exp.DataType.Type.NVARCHAR: "VARCHAR",  # NVARCHAR not supported, use VARCHAR with UTF-8 collation
            exp.DataType.Type.SMALLDATETIME: "DATETIME2",  # SMALLDATETIME not supported, use DATETIME2
            exp.DataType.Type.TINYINT: "SMALLINT",  # TINYINT not supported, use SMALLINT
            exp.DataType.Type.GEOGRAPHY: "VARBINARY(MAX)",  # GEOGRAPHY not supported, use VARBINARY or VARCHAR
            exp.DataType.Type.GEOMETRY: "VARBINARY(MAX)",  # GEOMETRY not supported, use VARBINARY or VARCHAR
            exp.DataType.Type.JSON: "VARCHAR(MAX)",  # JSON not supported, use VARCHAR
            exp.DataType.Type.XML: "VARCHAR(MAX)",  # XML not supported, use VARCHAR as fallback
        }

        def create_sql(self, expression: exp.Create) -> str:
            """Override CREATE statements for Fabric limitations."""
            # Use the standard create_sql but with Fabric-specific table/column handling
            return super().create_sql(expression)

        def table_sql(self, expression: exp.Table, sep: str = " AS ") -> str:
            """Override table references to handle INFORMATION_SCHEMA uppercase requirement."""
            # Check if this is an INFORMATION_SCHEMA reference
            db_name = expression.text("db")
            if db_name and db_name.lower() == "information_schema":
                # Create a new table expression with uppercase INFORMATION_SCHEMA
                new_table = expression.copy()
                new_table.set("db", exp.Identifier(this="INFORMATION_SCHEMA", quoted=False))

                # Also uppercase the table name
                table_name = expression.text("this")
                if table_name:
                    new_table.set("this", exp.Identifier(this=table_name.upper(), quoted=False))

                return super().table_sql(new_table, sep)

            # For non-INFORMATION_SCHEMA tables, use standard handling
            return super().table_sql(expression, sep)

        def column_sql(self, expression: exp.Column) -> str:
            """Override column references to handle INFORMATION_SCHEMA column names."""
            should_uppercase = False

            # Check if column has direct INFORMATION_SCHEMA reference
            db_name = expression.text("db")
            table_name = expression.text("table")

            if db_name and db_name.upper() == "INFORMATION_SCHEMA":
                should_uppercase = True
            elif table_name:
                # Check if the referenced table alias points to an INFORMATION_SCHEMA table
                parent = expression.parent
                while parent:
                    if isinstance(parent, exp.Select):
                        # Look for table definitions in this SELECT context
                        for table_expr in parent.find_all(exp.Table):
                            table_db = table_expr.text("db")
                            table_this = table_expr.text("this")

                            # Get the alias more robustly
                            table_alias = None
                            alias_obj = table_expr.args.get("alias")
                            if alias_obj:
                                if hasattr(alias_obj, "this") and alias_obj.this:
                                    table_alias = str(alias_obj.this)
                                elif hasattr(alias_obj, "text"):
                                    table_alias = alias_obj.text("this")
                                else:
                                    table_alias = str(alias_obj)

                            # Check if this table reference matches our column's table
                            matches = False
                            if table_alias and str(table_alias) == table_name:
                                matches = True
                            elif not table_alias and table_this and table_this == table_name:
                                matches = True

                            if matches and table_db and table_db.upper() == "INFORMATION_SCHEMA":
                                should_uppercase = True
                                break
                        break
                    parent = parent.parent
            else:
                # For unqualified columns, check if we're selecting from a single INFORMATION_SCHEMA table
                parent = expression.parent
                while parent:
                    if isinstance(parent, exp.Select):
                        info_schema_tables = 0
                        total_tables = 0

                        for table_expr in parent.find_all(exp.Table):
                            total_tables += 1
                            table_db = table_expr.text("db")
                            if table_db and table_db.upper() == "INFORMATION_SCHEMA":
                                info_schema_tables += 1

                        # Only uppercase unqualified columns if ALL tables are from INFORMATION_SCHEMA
                        if total_tables > 0 and info_schema_tables == total_tables:
                            should_uppercase = True
                        break
                    parent = parent.parent

            if should_uppercase:
                col_name = expression.text("this")
                if col_name and col_name.lower() != col_name.upper():
                    new_column = expression.copy()
                    new_column.set("this", exp.Identifier(this=col_name.upper(), quoted=False))
                    return super().column_sql(new_column)

            return super().column_sql(expression)

        def identifier_sql(self, expression: exp.Identifier) -> str:
            """Override identifier handling for case sensitivity."""
            # In Fabric, identifiers are case-sensitive
            # We need to be careful about quoting
            identifier = super().identifier_sql(expression)
            return identifier

        def truncatetable_sql(self, expression: exp.TruncateTable) -> str:
            """Override TRUNCATE for Fabric limitations."""
            # Fabric supports TRUNCATE TABLE but with limitations
            # It doesn't support all the options that standard T-SQL does
            table = self.sql(expression, "this")

            # Fabric TRUNCATE is simpler - just TRUNCATE TABLE tablename
            return f"TRUNCATE TABLE {table}"

        def hint_sql(self, expression: exp.Hint) -> str:
            """Override hints for Fabric limitations."""
            # Fabric has limited query hint support
            # Many T-SQL hints are not supported

            # List of hints that are supported in Fabric (this is a subset)
            SUPPORTED_HINTS = {
                "MAXDOP",
                "OPTION",
                # Add other supported hints as needed
            }

            expressions = expression.expressions
            if expressions:
                hint_name = (
                    expressions[0].name if hasattr(expressions[0], "name") else str(expressions[0])
                )
                if hint_name.upper() not in SUPPORTED_HINTS:
                    # Skip unsupported hints or convert them
                    self.unsupported(
                        f"Query hint '{hint_name}' is not supported in Microsoft Fabric"
                    )
                    return ""

            return super().hint_sql(expression)

        def queryoption_sql(self, expression: exp.QueryOption) -> str:
            """Override query options for Fabric limitations."""
            option = self.sql(expression, "this")

            # Fabric supports fewer query options than full T-SQL
            SUPPORTED_OPTIONS = {
                "MAXDOP",
                "LABEL",
                # Add other supported options
            }

            if option.upper() not in SUPPORTED_OPTIONS:
                self.unsupported(f"Query option '{option}' is not supported in Microsoft Fabric")
                return ""

            return super().queryoption_sql(expression)

        def datatype_sql(self, expression: exp.DataType) -> str:
            """Override data types for Fabric-specific limitations."""

            # Handle types that need precise formatting control
            if expression.this == exp.DataType.Type.MONEY:
                return "DECIMAL(19,4)"
            elif expression.this == exp.DataType.Type.SMALLMONEY:
                return "DECIMAL(10,4)"

            # Handle precision limits for temporal types
            if expression.this == exp.DataType.Type.DATETIME2:
                # Limit precision for datetime2 to 6 digits as per Fabric documentation
                if expression.expressions:
                    precision = self.sql(expression.expressions[0])
                    try:
                        prec_int = int(precision)
                        if prec_int > 6:
                            return "DATETIME2(6)"
                    except ValueError:
                        pass
            elif expression.this == exp.DataType.Type.TIME:
                # Limit precision for time to 6 digits as per Fabric documentation
                if expression.expressions:
                    precision = self.sql(expression.expressions[0])
                    try:
                        prec_int = int(precision)
                        if prec_int > 6:
                            return "TIME(6)"
                    except ValueError:
                        pass

            # Handle special cases for string-based types that may not be in the enum
            type_str = str(expression.this).upper() if expression.this else ""
            if type_str == "NTEXT":
                return "VARCHAR(MAX)"
            elif type_str == "DATETIME" and expression.this != exp.DataType.Type.DATETIME2:
                return "DATETIME2"
            elif type_str == "DATETIMEOFFSET":
                return "DATETIME2"

            # Use the standard TYPE_MAPPING for everything else
            return super().datatype_sql(expression)
