"""The Vertica dialect.
"""

from sqlfluff.core.parser import OneOf, BaseSegment, Sequence, Indent, Ref, Bracketed, Dedent

from sqlfluff.core.dialects import load_raw_dialect


postgres_dialect = load_raw_dialect("postgres")
ansi_dialect = load_raw_dialect("ansi")

vertica_dialect = postgres_dialect.copy_as("vertica")


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

