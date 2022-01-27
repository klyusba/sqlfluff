"""The Vertica dialect.
https://www.vertica.com/docs/11.0.x/HTML/Content/Authoring/ConceptsGuide/Other/SQLOverview.htm
"""

from sqlfluff.core.parser import (OneOf, BaseSegment, Sequence, Indent, Ref, Bracketed, Dedent,
                                  GreedyUntil, StartsWith, Delimited,
                                  OptionallyBracketed, Conditional, AnyNumberOf)

from sqlfluff.core.dialects import load_raw_dialect
from sqlfluff.dialects.dialect_vertica_keywords import reserved_keywords, unreserved_keywords


ansi_dialect = load_raw_dialect("ansi")
vertica_dialect = ansi_dialect.copy_as("vertica")

vertica_dialect.sets("unreserved_keywords").clear()
vertica_dialect.sets("unreserved_keywords").update(unreserved_keywords.splitlines())
vertica_dialect.sets("reserved_keywords").clear()
vertica_dialect.sets("reserved_keywords").update(reserved_keywords.splitlines())
vertica_dialect.sets("bare_functions").add('SYSDATE')


@vertica_dialect.segment()
class EqualsNullsafeSegment(BaseSegment):
    """Nullsafe Equals to operator."""

    type = "comparison_operator"
    match_grammar = Sequence(
        Ref("RawLessThanSegment"), Ref("RawEqualsSegment"), Ref("RawGreaterThanSegment"),
        allow_gaps=False
    )


vertica_dialect.replace(
    # qualify removed
    FromClauseTerminatorGrammar=OneOf(
        "WHERE",
        "LIMIT",
        Sequence("GROUP", "BY"),
        Sequence("ORDER", "BY"),
        "HAVING",
        "WINDOW",
        Ref("SetOperatorSegment"),
        Ref("WithNoSchemaBindingClauseSegment"),
    ),
    # qualify removed
    WhereClauseTerminatorGrammar=OneOf(
        "LIMIT",
        Sequence("GROUP", "BY"),
        Sequence("ORDER", "BY"),
        "HAVING",
        "WINDOW",
        "OVERLAPS",
    ),
    # <=> added
    ComparisonOperatorGrammar=OneOf(
        Ref("EqualsSegment"),
        Ref("GreaterThanSegment"),
        Ref("LessThanSegment"),
        Ref("GreaterThanOrEqualToSegment"),
        Ref("LessThanOrEqualToSegment"),
        Ref("NotEqualToSegment"),
        Ref("LikeOperatorSegment"),
        Ref("EqualsNullsafeSegment"),
    ),
    # RLIKE removed, LIKEB and ILIKEB added
    LikeGrammar=OneOf("LIKE", "ILIKE", "LIKEB", "ILIKEB"),
)


@vertica_dialect.segment(replace=True)
class StatementSegment(BaseSegment):
    """A generic segment, to any of its child subsegments."""

    type = "statement"
    match_grammar = GreedyUntil(Ref("DelimiterSegment"))

    parse_grammar = OneOf(
        Ref("SelectableGrammar"),
        Ref("InsertStatementSegment"),
        Ref("TransactionStatementSegment"),
        Ref("DropTableStatementSegment"),
        Ref("DropViewStatementSegment"),
        Ref("DropUserStatementSegment"),
        Ref("TruncateStatementSegment"),
        Ref("AccessStatementSegment"),
        Ref("CreateTableStatementSegment"),
        Ref("CreateTypeStatementSegment"),
        Ref("CreateRoleStatementSegment"),
        Ref("AlterTableStatementSegment"),
        Ref("CreateSchemaStatementSegment"),
        Ref("SetSchemaStatementSegment"),
        Ref("DropSchemaStatementSegment"),
        Ref("CreateExtensionStatementSegment"),
        Ref("CreateIndexStatementSegment"),
        Ref("DropIndexStatementSegment"),
        Ref("CreateViewStatementSegment"),
        Ref("DeleteStatementSegment"),
        Ref("UpdateStatementSegment"),
        Ref("CreateFunctionStatementSegment"),
        Ref("CreateModelStatementSegment"),
        Ref("DropModelStatementSegment"),
        Ref("ExplainStatementSegment"),
        Ref("CreateSequenceStatementSegment"),
        Ref("AlterSequenceStatementSegment"),
        Ref("DropSequenceStatementSegment"),
    )

    def get_table_references(self):
        """Use parsed tree to extract table references."""
        table_refs = {
            tbl_ref.raw for tbl_ref in self.recursive_crawl("table_reference")
        }
        cte_refs = {
            cte_def.get_identifier().raw
            for cte_def in self.recursive_crawl("common_table_expression")
        }
        # External references are any table references which aren't
        # also cte aliases.
        return table_refs - cte_refs


@vertica_dialect.segment(replace=True)
class FromExpressionSegment(BaseSegment):
    """A from expression segment."""

    type = "from_expression"
    match_grammar = Sequence(
        Ref("FromExpressionElementSegment"),
        Conditional(Dedent, indented_joins=False),
        AnyNumberOf(
            Ref("JoinClauseSegment"), Ref("JoinLikeClauseGrammar"), optional=True
        ),
        Conditional(Dedent, indented_joins=True),
    )


@vertica_dialect.segment(replace=True)
class OrderByClauseSegment(BaseSegment):
    """A `ORDER BY` clause like in `SELECT`."""

    type = "orderby_clause"
    match_grammar = StartsWith(
        Sequence("ORDER", "BY"),
        terminator=OneOf(
            "LIMIT",
            "HAVING",
            # For window functions
            "WINDOW",
            Ref("FrameClauseUnitGrammar"),
            "SEPARATOR",
        ),
    )
    parse_grammar = Sequence(
        "ORDER",
        "BY",
        Indent,
        Delimited(
            Sequence(
                OneOf(
                    Ref("ColumnReferenceSegment"),
                    # Can `ORDER BY 1`
                    Ref("NumericLiteralSegment"),
                    # Can order by an expression
                    Ref("ExpressionSegment"),
                ),
                OneOf("ASC", "DESC", optional=True),
                # NB: This isn't really ANSI, and isn't supported in Mysql, but
                # is supported in enough other dialects for it to make sense here
                # for now.
                Sequence("NULLS", OneOf("FIRST", "LAST"), optional=True),
            ),
            terminator=OneOf(Ref.keyword("LIMIT"), Ref("FrameClauseUnitGrammar")),
        ),
        Dedent,
    )


@vertica_dialect.segment(replace=True)
class GroupByClauseSegment(BaseSegment):
    """A `GROUP BY` clause like in `SELECT`."""

    type = "groupby_clause"
    match_grammar = StartsWith(
        Sequence("GROUP", "BY"),
        terminator=OneOf("ORDER", "LIMIT", "HAVING", "WINDOW"),
        enforce_whitespace_preceding_terminator=True,
    )
    parse_grammar = Sequence(
        "GROUP",
        "BY",
        Indent,
        Delimited(
            OneOf(
                Ref("ColumnReferenceSegment"),
                # Can `GROUP BY 1`
                Ref("NumericLiteralSegment"),
                # Can `GROUP BY coalesce(col, 1)`
                Ref("ExpressionSegment"),
            ),
            terminator=OneOf("ORDER", "LIMIT", "HAVING", "WINDOW"),
        ),
        Dedent,
    )


@vertica_dialect.segment(replace=True)
class HavingClauseSegment(BaseSegment):
    """A `HAVING` clause like in `SELECT`."""

    type = "having_clause"
    match_grammar = StartsWith(
        "HAVING",
        terminator=OneOf("ORDER", "LIMIT", "WINDOW"),
        enforce_whitespace_preceding_terminator=True,
    )
    parse_grammar = Sequence(
        "HAVING",
        Indent,
        OptionallyBracketed(Ref("ExpressionSegment")),
        Dedent,
    )


@vertica_dialect.segment(replace=True)
class LimitClauseSegment(BaseSegment):
    """A `LIMIT` clause like in `SELECT`."""

    type = "limit_clause"

    match_grammar = Sequence(
        "LIMIT",
        Indent,
        OneOf(
            Ref("NumericLiteralSegment"),
            Sequence(
                Ref("NumericLiteralSegment"), "OFFSET", Ref("NumericLiteralSegment")
            ),
            Sequence(
                Ref("NumericLiteralSegment"),
                Ref("CommaSegment"),
                Ref("NumericLiteralSegment"),
            ),
            Sequence(
                Ref("NumericLiteralSegment"),
                "OVER",
                OneOf(
                    Ref("SingleIdentifierGrammar"),  # Window name
                    Bracketed(
                        Ref("PartitionClauseSegment", optional=True),
                        Ref("OrderByClauseSegment", optional=True),
                    ),
                ),
            ),
        ),
        Dedent,
    )


@vertica_dialect.segment(replace=True)
class InsertStatementSegment(BaseSegment):
    """An `INSERT` statement."""

    type = "insert_statement"
    match_grammar = StartsWith("INSERT")
    parse_grammar = Sequence(
        "INSERT",
        "INTO",
        Ref("TableReferenceSegment"),
        Ref("BracketedColumnReferenceListGrammar", optional=True),
        Ref("SelectableGrammar"),
    )


@vertica_dialect.segment(replace=True)
class WindowSpecificationSegment(BaseSegment):
    """Window specification within OVER(...)."""

    type = "window_specification"
    match_grammar = Sequence(
        OneOf(
            Ref("SingleIdentifierGrammar", optional=True),  # "Base" window name
            Sequence(
                Ref("SingleIdentifierGrammar", optional=True),  # "Base" window name
                Ref("OrderByClauseSegment", optional=True),
            ),
            Sequence(
                Ref("PartitionClauseSegment", optional=True),
                Ref("OrderByClauseSegment", optional=True),
            ),
        ),
        Ref("FrameClauseSegment", optional=True),
        optional=True,
        ephemeral_name="OverClauseContent",
    )


@vertica_dialect.segment(replace=True)
class ArrayLiteralSegment(BaseSegment):
    """An array literal segment."""

    type = "array_literal"
    match_grammar = Sequence(
        "ARRAY",
        Bracketed(
            Delimited(Ref("ExpressionSegment"), optional=True),
            bracket_type="square",
        ),
    )


@vertica_dialect.segment()
class SetLiteralSegment(BaseSegment):
    """An set literal segment."""

    type = "set_literal"
    match_grammar = Sequence(
        "SET",
        Bracketed(
            Delimited(Ref("ExpressionSegment"), optional=True),
            bracket_type="square",
        ),
    )


@vertica_dialect.segment(replace=True)
class DatatypeSegment(BaseSegment):
    """A data type segment.

    Supports timestamp with(out) time zone. Doesn't currently support intervals.
    """

    type = "data_type"
    match_grammar = OneOf(
        # TODO VARCHAR, NUMERIC
        "INT",
        "INTEGER",
        "FLOAT",
        Sequence(
            OneOf("time", "timestamp"),
            Bracketed(Ref("NumericLiteralSegment"), optional=True),
            Sequence(OneOf("WITH", "WITHOUT"), "TIME", "ZONE", optional=True),
        ),
        Sequence(
            "DOUBLE",
            "PRECISION",
        ),
        Sequence(
            OneOf(
                Sequence(
                    OneOf("CHARACTER", "BINARY"),
                    OneOf("VARYING", Sequence("LARGE", "OBJECT")),
                ),
                Sequence(
                    # Some dialects allow optional qualification of data types with schemas
                    Sequence(
                        Ref("SingleIdentifierGrammar"),
                        Ref("DotSegment"),
                        allow_gaps=False,
                        optional=True,
                    ),
                    Ref("DatatypeIdentifierSegment"),
                    allow_gaps=False,
                ),
            ),
            Bracketed(
                OneOf(
                    Delimited(Ref("ExpressionSegment")),
                    # The brackets might be empty for some cases...
                    optional=True,
                ),
                # There may be no brackets for some data types
                optional=True,
            ),
            Ref("CharCharacterSetSegment", optional=True),
        ),
    )


vertica_dialect.get_segment("FromClauseSegment").parse_grammar = Sequence(
        "FROM",
        OptionallyBracketed(
            Delimited(
                Ref("FromExpressionSegment"),
            ),
        )
    )
