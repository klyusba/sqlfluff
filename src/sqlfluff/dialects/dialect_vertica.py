"""The Vertica dialect.
https://www.vertica.com/docs/11.0.x/HTML/Content/Authoring/ConceptsGuide/Other/SQLOverview.htm
"""

from sqlfluff.core.parser import (OneOf, BaseSegment, Sequence, Indent, Ref, Bracketed, Dedent,
                                  GreedyUntil, StartsWith, Delimited,
                                  OptionallyBracketed, Conditional, AnyNumberOf, StringLexer, CodeSegment,
                                  StringParser, SymbolSegment)

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


vertica_dialect.insert_lexer_matchers(
    [StringLexer("safe_casting_operator", "::!", CodeSegment), ],
    before='casting_operator'
)


vertica_dialect.replace(
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
class CastOperatorSegment(BaseSegment):
    type = "casting_operator"
    match_grammar = OneOf(
        StringParser(
            "::", SymbolSegment, name="casting_operator", type="casting_operator"
        ),
        StringParser(
            "::!", SymbolSegment, name="safe_casting_operator", type="safe_casting_operator"
        )
    )


@vertica_dialect.segment(replace=True)
class IntervalExpressionSegment(BaseSegment):
    """An interval expression segment.

    INTERVAL 'interval-literal' [ interval-qualifier ] [ (p) ]
    """

    type = "interval_expression"
    match_grammar = Sequence(
        "INTERVAL",
        Ref("QuotedLiteralSegment"),
        AnyNumberOf(
            Ref("DatetimeUnitSegment"),
            Sequence(
                Ref("DatetimeUnitSegment"),
                "TO",
                Ref("DatetimeUnitSegment"),
                Bracketed(
                    Ref("NumericLiteralSegment"),
                    optional=True
                )
            ),
            max_times=1,
        )
    )


@vertica_dialect.segment(replace=True)
class StatementSegment(BaseSegment):
    """A generic segment, to any of its child subsegments."""

    type = "statement"
    match_grammar = GreedyUntil(Ref("DelimiterSegment"))

    parse_grammar = OneOf(
        Ref("SelectableGrammar"),
        Ref("InsertStatementSegment"),
        Ref("MergeStatementSegment"),
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
class PartitionClauseSegment(BaseSegment):
    """A `PARTITION BY` for window functions."""

    type = "partitionby_clause"
    match_grammar = StartsWith(
        "PARTITION",
        terminator=OneOf("ORDER", Ref("FrameClauseUnitGrammar")),
        enforce_whitespace_preceding_terminator=True,
    )
    parse_grammar = OneOf(
        Sequence(
            "PARTITION",
            "BY",
            Indent,
            # Brackets are optional in a partition by statement
            OptionallyBracketed(Delimited(Ref("ExpressionSegment"))),
            Dedent,
        ),
        Sequence(
            "PARTITION",
            "BEST",
        ),
        Sequence(
            "PARTITION",
            "NODES",
        )
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
        OptionallyBracketed(Ref("SelectableGrammar")),
    )


@vertica_dialect.segment()
class MergeStatementSegment(BaseSegment):
    """An `MERGE` statement."""

    type = "merge_statement"
    match_grammar = StartsWith("MERGE")
    parse_grammar = Sequence(
        "MERGE",
        "INTO",
        Ref("TableReferenceSegment"),
        Ref("AliasExpressionSegment", optional=True),
        "USING",
        OneOf(
            Sequence(
                Ref("TableReferenceSegment"),
                Ref("AliasExpressionSegment", optional=True),
            ),
            Sequence(
                Bracketed(
                    Ref("SelectableGrammar")
                ),
                Ref("SingleIdentifierGrammar")
            )
        ),
        Ref("JoinOnConditionSegment"),
        AnyNumberOf(
            Ref("MergeMatchedClauseSegment"),
            Ref("MergeNotMatchedClauseSegment"),
            min_times=1,
        ),
    )


@vertica_dialect.segment()
class MergeMatchedClauseSegment(BaseSegment):
    """The `WHEN MATCHED` clause within a `MERGE` statement."""

    type = "merge_when_matched_clause"

    match_grammar = Sequence(
        "WHEN",
        "MATCHED",
        Sequence(
            "AND",
            Ref("ExpressionSegment"),
            optional=True,
        ),
        Indent,
        "THEN",
        "UPDATE",
        Ref("SetClauseListSegment"),
        Dedent,
    )
    # TODO Vertica also supports Oracle syntax for specifying update filters


@vertica_dialect.segment()
class MergeNotMatchedClauseSegment(BaseSegment):
    """The `WHEN NOT MATCHED` clause within a `MERGE` statement."""

    type = "merge_when_not_matched_clause"

    match_grammar = Sequence(
        "WHEN",
        "NOT",
        "MATCHED",
        Sequence("AND", Ref("ExpressionSegment"), optional=True),
        Indent,
        "THEN",
        Ref("MergeInsertClauseSegment"),
        Dedent,
    )
    # TODO Vertica also supports Oracle syntax for specifying update filters


@vertica_dialect.segment()
class MergeInsertClauseSegment(BaseSegment):
    """`INSERT` clause within the `MERGE` statement."""

    type = "merge_insert_clause"
    match_grammar = Sequence(
        "INSERT",
        Indent,
        Ref("BracketedColumnReferenceListGrammar", optional=True),
        Dedent,
        "VALUES",
        Indent,
        Bracketed(
            Delimited(
                AnyNumberOf(
                    Ref("ExpressionSegment"),
                ),
            ),
        ),
        Dedent,
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


vertica_dialect.replace(
    FunctionContentsGrammar=ansi_dialect.get_grammar("FunctionContentsGrammar").copy(
        insert=[
            Sequence(
                "USING",
                "PARAMETERS",
                Delimited(
                    Sequence(
                        Ref("SingleIdentifierGrammar"),
                        Ref("RawEqualsSegment"),
                        Ref("LiteralGrammar"),
                    )
                )
            ),
            Sequence(  # TRIM function
                OneOf(
                    "BOTH",
                    "LEADING",
                    "TRAILING",
                ),
                Sequence(
                    Ref("LiteralGrammar"),
                    "FROM",
                    optional=True
                ),
                Ref("SingleIdentifierGrammar")
            ),
            Sequence(  # SUBSTRING function
                "USING",
                OneOf(
                    "CHARACTERS",
                    "OCTETS"
                )
            ),
        ]
    )
)
