"""A list of all SQL key words.
Gain by "SELECT * FROM keywords"
Vertica Version 11.0.1-1
"""

reserved_keywords = """\
ALL
AND
ANY
ARRAY
AS
ASC
AUTHORIZATION
BETWEEN
BIGINT
BINARY
BIT
BOOLEAN
BOTH
CASE
CAST
CHAR
CHAR_LENGTH
CHARACTER_LENGTH
CHECK
COLLATE
COLUMN
CONSTRAINT
CORRELATION
CREATE
CROSS
CURRENT_DATABASE
CURRENT_DATE
CURRENT_SCHEMA
CURRENT_TIME
CURRENT_TIMESTAMP
CURRENT_USER
DATEDIFF
DATETIME
DECIMAL
DECODE
DEFAULT
DEFERRABLE
DESC
DISTINCT
ELSE
ENCODED
END
EXCEPT
EXISTS
EXTRACT
FALSE
FLOAT
FOR
FOREIGN
FROM
FULL
GRANT
GROUP
HAVING
ILIKE
ILIKEB
IN
INITIALLY
INNER
INOUT
INT
INTEGER
INTERSECT
INTERVAL
INTERVALYM
INTO
IS
ISNULL
JOIN
KSAFE
LEADING
LEFT
LIKE
LIKEB
LIMIT
LOCALTIME
LOCALTIMESTAMP
MATCH
MINUS
MONEY
NATURAL
NCHAR
NEW
NONE
NOT
NOTNULL
NULL
NULLSEQUAL
NUMBER
NUMERIC
OFFSET
OLD
ON
ONLY
OR
ORDER
OUT
OUTER
OVER
OVERLAPS
OVERLAY
PINNED
POSITION
PRECISION
PRIMARY
REAL
REFERENCES
RIGHT
ROW
SCHEMA
SELECT
SESSION_USER
SIMILAR
SMALLDATETIME
SMALLINT
SOME
SUBSTRING
SYSDATE
TABLE
TEXT
THEN
TIME
TIMESERIES
TIMESTAMP
TIMESTAMPADD
TIMESTAMPDIFF
TIMESTAMPTZ
TIMETZ
TIMEZONE
TINYINT
TO
TRAILING
TREAT
TRIM
TRUE
UNBOUNDED
UNION
UNIQUE
USER
USING
UUID
VARBINARY
VARCHAR
VARCHAR2
WHEN
WHERE
WINDOW
WITH
WITHIN"""

unreserved_keywords = """\
ABORT
ABSOLUTE
ACCESS
ACCESSRANK
ACCOUNT
ACTION
ACTIVATE
ACTIVEPARTITIONCOUNT
ADD
ADDRESS
ADMIN
AFTER
AGGREGATE
ALSO
ALTER
ANALYSE
ANALYTIC
ANALYZE
ANNOTATED
ANTI
ASSERTION
ASSIGNMENT
AT
AUTHENTICATION
AUTO
AUTO_INCREMENT
AVAILABLE
BACKWARD
BALANCE
BASENAME
BATCH
BEFORE
BEGIN
BEST
BLOCK
BLOCK_DICT
BLOCKDICT_COMP
BROADCAST
BUNDLE
BY
BYTEA
BYTES
BZIP
BZIP_COMP
CA
CACHE
CALL
CALLED
CASCADE
CATALOGPATH
CERTIFICATE
CERTIFICATES
CHAIN
CHARACTER
CHARACTERISTICS
CHARACTERS
CHECKPOINT
CIPHER
CLASS
CLEAR
CLOSE
CLUSTER
COLLECTIONCLOSE
COLLECTIONDELIMITER
COLLECTIONENCLOSE
COLLECTIONNULLELEMENT
COLLECTIONOPEN
COLSIZES
COLUMNS_COUNT
COMMENT
COMMIT
COMMITTED
COMMONDELTA_COMP
COMMUNAL
COMPLEX
CONFIGURATION
CONNECT
CONSTRAINTS
CONTROL
COPY
CPUAFFINITYMODE
CPUAFFINITYSET
CREATEDB
CREATEUSER
CSV
CUBE
CURRENT
CURSOR
CUSTOM
CUSTOM_PARTITIONS
CYCLE
DATA
DATABASE
DATAPATH
DAY
DEACTIVATE
DEALLOCATE
DEBUG
DEC
DECLARE
DEFAULTS
DEFERRED
DEFINE
DEFINER
DELETE
DELIMITED
DELIMITER
DELIMITERS
DELTARANGE_COMP
DELTARANGE_COMP_SP
DELTAVAL
DEPENDS
DETERMINES
DIRECT
DIRECTCOLS
DIRECTED
DIRECTGROUPED
DIRECTPROJ
DISABLE
DISABLED
DISCONNECT
DISTVALINDEX
DO
DOMAIN
DOUBLE
DROP
DURABLE
EACH
ENABLE
ENABLED
ENCLOSED
ENCODING
ENCRYPTED
ENFORCELENGTH
EPHEMERAL
EPOCH
ERROR
ESCAPE
EVENT
EVENTS
EXCEPTION
EXCEPTIONS
EXCLUDE
EXCLUDING
EXCLUSIVE
EXECUTE
EXECUTIONPARALLELISM
EXPIRE
EXPLAIN
EXPORT
EXTEND
EXTENSIONS
EXTERNAL
FAILED_LOGIN_ATTEMPTS
FAULT
FENCED
FETCH
FILESYSTEM
FILLER
FILTER
FIRST
FIXEDWIDTH
FLEX
FLEXIBLE
FOLLOWING
FORCE
FORMAT
FORWARD
FREEZE
FUNCTION
FUNCTIONS
GCDDELTA
GET
GLOBAL
GRACEPERIOD
GROUPED
GROUPING
GZIP
GZIP_COMP
HANDLER
HCATALOG
HCATALOG_CONNECTION_TIMEOUT
HCATALOG_DB
HCATALOG_SCHEMA
HCATALOG_SLOW_TRANSFER_LIMIT
HCATALOG_SLOW_TRANSFER_TIME
HCATALOG_USER
HIGH
HIVESERVER2_HOSTNAME
HOLD
HOST
HOSTNAME
HOUR
HOURS
IDENTIFIED
IDENTITY
IDLESESSIONTIMEOUT
IF
IGNORE
IMMEDIATE
IMMUTABLE
IMPLICIT
INCLUDE
INCLUDING
INCREMENT
INDEX
INHERITS
INPUT
INSENSITIVE
INSERT
INSTEAD
INTERFACE
INTERPOLATE
INVOKER
ISOLATION
JSON
KEY
LABEL
LANCOMPILER
LANGUAGE
LARGE
LAST
LATEST
LENGTH
LESS
LEVEL
LIBRARY
LISTEN
LOAD
LOCAL
LOCATION
LOCK
LONG
LOW
LZO
MANAGED
MAP
MASK
MATCHED
MATERIALIZE
MAXCONCURRENCY
MAXCONCURRENCYGRACE
MAXCONNECTIONS
MAXMEMORYSIZE
MAXPAYLOAD
MAXQUERYMEMORYSIZE
MAXVALUE
MEDIUM
MEMORYCAP
MEMORYSIZE
MERGE
MERGEOUT
METHOD
MICROSECONDS
MILLISECONDS
MINUTE
MINUTES
MINVALUE
MODE
MODEL
MONTH
MOVE
MOVEOUT
NAME
NATIONAL
NATIVE
NETWORK
NEXT
NO
NOCREATEDB
NOCREATEUSER
NODE
NODES
NOTHING
NOTIFIER
NOTIFY
NOWAIT
NULLAWARE
NULLCOLS
NULLS
OBJECT
OCTETS
OF
OFF
OIDS
OPERATOR
OPT
OPTIMIZER
OPTION
OPTVER
ORC
OTHERS
OWNER
PARAMETER
PARAMETERS
PARQUET
PARSER
PARTIAL
PARTITION
PARTITIONING
PASSWORD
PASSWORD_GRACE_TIME
PASSWORD_LIFE_TIME
PASSWORD_LOCK_TIME
PASSWORD_MAX_LENGTH
PASSWORD_MIN_CHAR_CHANGE
PASSWORD_MIN_DIGITS
PASSWORD_MIN_LENGTH
PASSWORD_MIN_LETTERS
PASSWORD_MIN_LIFE_TIME
PASSWORD_MIN_LOWERCASE_LETTERS
PASSWORD_MIN_SYMBOLS
PASSWORD_MIN_UPPERCASE_LETTERS
PASSWORD_REUSE_MAX
PASSWORD_REUSE_TIME
PATTERN
PERCENT
PERMANENT
PLACING
PLANNEDCONCURRENCY
POLICY
POOL
PORT
PRECEDING
PREFER
PREPARE
PREPASS
PRESERVE
PREVIOUS
PRIOR
PRIORITY
PRIVILEGES
PROCEDURAL
PROCEDURE
PROFILE
PROJECTION
PROJECTIONS
PSDATE
QUERY
QUEUETIMEOUT
QUOTE
RANDOM
RANGE
RAW
READ
RECHECK
RECORD
RECOVER
RECURSIVE
REFRESH
REINDEX
REJECTED
REJECTMAX
RELATIVE
RELEASE
REMOVE
RENAME
REORGANIZE
REPEATABLE
REPLACE
RESET
RESOURCE
RESTART
RESTRICT
RESULTS
RETURN
RETURNREJECTED
REVOKE
RLE
ROLE
ROLES
ROLLBACK
ROLLUP
ROUTE
ROUTING
ROWS
RULE
RUNTIMECAP
RUNTIMEPRIORITY
RUNTIMEPRIORITYTHRESHOLD
RWITH
SALT
SAVE
SAVEPOINT
SCROLL
SEARCH_PATH
SECOND
SECONDARY
SECONDS
SECURITY
SECURITY_ALGORITHM
SEGMENTED
SEMI
SEMIALL
SEQUENCE
SEQUENCES
SERIALIZABLE
SESSION
SET
SETOF
SETS
SHARE
SHARED
SHOW
SIGNED
SIMPLE
SINGLEINITIATOR
SITE
SITES
SKIP
SOURCE
SPLIT
SSL_CONFIG
STABLE
STANDBY
START
STATEMENT
STATISTICS
STDIN
STDOUT
STEMMER
STORAGE
STREAM
STRENGTH
STRICT
SUBCLUSTER
SUBJECT
SUBNET
SUITES
SYSID
SYSTEM
TABLES
TABLESAMPLE
TABLESPACE
TEMP
TEMPLATE
TEMPORARY
TEMPSPACECAP
TERMINATOR
THAN
TIES
TLS
TLSMODE
TOAST
TOKENIZER
TOLERANCE
TRANSACTION
TRANSFORM
TRICKLE
TRIGGER
TRUNCATE
TRUSTED
TUNING
TYPE
UDPARAMETER
UNCOMMITTED
UNCOMPRESSED
UNI
UNINDEXED
UNKNOWN
UNLIMITED
UNLISTEN
UNLOCK
UNSEGMENTED
UPDATE
USAGE
VACUUM
VALID
VALIDATE
VALIDATOR
VALINDEX
VALUE
VALUES
VARYING
VERBOSE
VERTICA
VIEW
VOLATILE
WAIT
WEBHDFS_ADDRESS
WEBSERVICE_HOSTNAME
WEBSERVICE_PORT
WITHOUT
WORK
WRITE
YEAR
ZONE
ZSTD
ZSTD_COMP
ZSTD_FAST_COMP
ZSTD_HIGH_COMP"""

# nondocumented keywords
unreserved_keywords += """
SEPARATOR
DATE
DATEADD"""
