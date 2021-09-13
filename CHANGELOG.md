# Change Log


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