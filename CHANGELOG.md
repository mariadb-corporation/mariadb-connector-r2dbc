# Change Log

## [1.2.1](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.2.1) (19 Jun 2024)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.2.0...1.2.1)

Notable Changes:
* R2DBC-96 Missing UUID support

Bugs Fixed:
* R2DBC-97 NPE if No HaMode provided
* R2DBC-98 correcting possible bug connecting if project contain a project.properties file
* R2DBC-99 No encoding set for ByteBuffer parameter
* R2DBC-101 Wrong default return type for MySQL JSON fields
* R2DBC-102 avoid netty unneeded dependencies

## [1.2.0](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.2.0) (08 Feb 2024)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.1.4...1.2.0)

Notable Changes:

* R2DBC-93 new timezone option
* R2DBC-66 add support for connection redirection

Bugs Fixed:

* R2DBC-92 Properly end connection (in place of RST TCP packet)
* R2DBC-86 Failover High availability mode "r2dbc:mariadb:[sequential|loadbalancing]://..." wrongly parsed
* R2DBC-87 Compatibility with mariadb 11.1.1
* R2DBC-88 java 8 compatibility regression
* R2DBC-91 ensure respecting server collation
* R2DBC-94 session tracking wrong implementation when multiple system variable changes


## [1.1.4](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.1.4) (16 Mar 2023)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.1.3...1.1.4)

Bugs Fixed:

* [R2DBC-76] Wrong MEDIUM field binary decoding
* [R2DBC-77] Metadata is null when using returnGeneratedValues on server before 10.5
* [R2DBC-79] Wrong client side parsing for named parameter when using user variable
* [R2DBC-80] add option to disable hostname verification for SslMode.TUNNEL. thanks to @shubha-rajan
* [R2DBC-81] missing parsing/builder variables for option restrictedAuth,rsaPublicKey,cachingRsaPublicKey and
	allowPublicKeyRetrievalString
* [R2DBC-82] wrong transactionIsolation level set/get with server without session tracking
* [R2DBC-85] adding hint in order to execute text command when useServerPrepStmts option is set
* [R2DBC-83] support xpand 0000-00-00 timestamp/date encoding

## [1.1.3](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.1.3) (22 Dec 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.1.2...1.1.3)

Notable Changes:

* [R2DBC-67] set SPEC version support to 1.0.0-release version
* [R2DBC-69] Add SSL tunnel mode
		* New `sslMode` option “tunnel” to permit to use pre-existing SSL tunnel
		* New option `sslContextBuilderCustomizer` to permit customizing SSL context Builder
* [R2DBC-74] Use default netty hostname verifier in place of custom one

Bugs Fixed:

* [R2DBC-68] subscriber cancellation before response end might stall connection #45
* [R2DBC-65] IllegalReferenceCountException exception in TextRowDecoder #24
* [R2DBC-70] SSL host name verification don't properly close socket when failing
* [R2DBC-71] ensuring proper closing of socket, when error occurs
* [R2DBC-73] pipelining PREPARE + EXECUTE might result in buffer leak when prepare fails
* [R2DBC-72] Encoded statement parameter buffer might not be released until garbage
* [R2DBC-75] failover redo buffer race condition not releasing before garbage

## [1.1.2](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.1.2) (12 Mai 2022)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.1.1...1.1.2)

* [R2DBC-54] Support r2dbc spec 0.9.1 version
* [R2DBC-42] Specification precision on Statement::add
* [R2DBC-44] simplify client side prepared statement
* [R2DBC-45] Implement SPI TestKit to validate driver with spec tests
* [R2DBC-46] Add sql to R2DBC exception hierarchy
* [R2DBC-47] ensure driver follow spec precision about Row.get returning error.
* [R2DBC-48] after spec batch clarification trailing batch should fail
* [R2DBC-49] Support for failover and load balancing modes
* [R2DBC-50] TIME data without indication default to return Duration in place of LocalTime
* [R2DBC-56] Transaction isolation spec precision
* [R2DBC-57] varbinary data default must return byte[]
* [R2DBC-63] backpressure handling
* [R2DBC-64] Support batch cancellation
* [R2DBC-53] correct RowMetadata case-sensitivity lookup
* [R2DBC-62] Prepared statement wrong column type on prepare meta not skipped

## [1.1.1-rc](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.1.1) (13 Sept 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.1.0...1.1.1)

Changes:

* [R2DBC-37] Full java 9 JPMS module
* [R2DBC-38] Permit sharing channels with option loopResources

Corrections:

* [R2DBC-40] netty buffer leaks when not consuming results
* [R2DBC-39] MariadbResult.getRowsUpdated() fails with ClassCastException for RETURNING command

## [1.0.3](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.0.3) (13 Sept 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.0.2...1.0.3)

Corrections:

* [R2DBC-40] netty buffer leaks when not consuming results
* [R2DBC-39] MariadbResult.getRowsUpdated() fails with ClassCastException for RETURNING command

## [1.1.0-beta](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.1.0-beta) (15 Jul 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.0.2...1.1.0-beta)

Changes:

* [R2DBC-10] - support 10.6 new feature metadata skip
* [R2DBC-21] - Failover capabilities for Connector/R2DBC
* [R2DBC-23] - Restrict authentication plugin list by option  (new option `restrictedAuth`)
* support SPI 0.9 M2
		* [R2DBC-32] - Add support for improved bind parameter declarations
		* [R2DBC-33] - Add Connection.beginTransaction(TransactionDefinition)
		* [R2DBC-34] - implement NoSuchOptionException
		* [R2DBC-35] - Refinement of RowMetadata
		* [R2DBC-36] - Implement statement timeout

## [1.0.2](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.0.2) (02 Jul 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.0.1...1.0.2)

Corrections:

* [R2DBC-24] columns of type Bit(1)/TINYINT(1) now convert as Boolean (new option `tinyInt1isBit`)
* [R2DBC-25] Statement::add correction after specification precision
* [R2DBC-26] handle error like 'too many connection" on socket creation
* [R2DBC-27] Options not parsed from connection string
* [R2DBC-28] mutual authentication not done when using ssl TRUST option
* [R2DBC-29] improve coverage to reaching 90%
* [R2DBC-30] Native Password plugin error

## [1.0.1](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.0.1) (09 Mar 2021)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/1.0.0...1.0.1)

Changes:

* [R2DBC-16] Ensure connection autocommit initialisation and new option autocommit

Corrections:

* [R2DBC-17] Transactions in query flux might not be persisted
* [R2DBC-19] Data bigger than 16Mb correction.

## [1.0.0](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/1.0.0) (08 Dec 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/0.8.4...1.0.0)

First GA release.

Corrections:

* [R2DBC-14] correcting backpressure handling #7
* [R2DBC-12] Ensure retaining blob buffer #6
* [R2DBC-11] Batching on statement use parameters not added #8
* Rely on 0.8.3 specification (was 0.8.2)
* bump dependencies
* Ensuring row buffer reading position

## [0.8.4](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/0.8.4) (29 Sep 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/0.8.3...0.8.4)

First Release Candidate release.

Changes compared to 0.8.3.beta1:
[R2DBC-9] Non pipelining prepare is close 2 time when race condition cache
[R2DBC-8] synchronous close
[R2DBC-7] authentication error when using multiple classloader
[R2DBC-6] socket option configuration addition for socket Timeout, keep alive and force RST

## [0.8.3](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/0.8.3) (23 Jul 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/0.8.2...0.8.3)

First Beta release.

This version had a strong focus on testing coverage (40%->85%).

Changes compared to 0.8.2.alpha1:

* Corrections and optimisations
* new option `pamOtherPwd` to permit PAM authentication with multiple steps.
* Rely on 0.8.2 specification (was 0.8.1)

## [0.8.2](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/0.8.2) (08 May 2020)

[Full Changelog](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/compare/0.8.1...0.8.2)
second Alpha release

## [0.8.1](https://github.com/mariadb-corporation/mariadb-connector-r2dbc/tree/0.8.1) (23 Mar. 2020)

First Alpha release
