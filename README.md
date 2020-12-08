<p align="center">
  <a href="http://mariadb.com/">
    <img src="https://mariadb.com/kb/static/images/logo-2018-black.png">
  </a>
</p>

# MariaDB R2DBC connector

[![Maven Central][maven-image]][maven-url]
[![Linux Build][travis-image]][travis-url]
[![Build status][appveyor-image]][appveyor-url]
[![License][license-image]][license-url]


**Non-blocking MariaDB and MySQL client.**

MariaDB and MySQL client, 100% Java, compatible with Java8+, apache 2.0 licensed.
- Driver permits ed25519, PAM authentication that comes with MariaDB.
- use MariaDB 10.5 returning fonction to permit Statement.returnGeneratedValues 

Driver follow [R2DBC 0.8.3 specifications](https://r2dbc.io/spec/0.8.3.RELEASE/spec/html/)

## Documentation

See [documentation](https://mariadb.com/docs/appdev/connector-r2dbc/) for native or with Spring Data R2DBC use. 


## Quick Start

The MariaDB Connector is available through maven using :

```
    <dependency>
        <groupId>org.mariadb</groupId>
        <artifactId>r2dbc-mariadb</artifactId>
        <version>1.0.0</version>
    </dependency>
```

Factory can be created using ConnectionFactory or using connection URL.

Using builder                                                     
```java

MariadbConnectionConfiguration conf = MariadbConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .username("myUser")
            .password("MySuperPassword")
            .database("db")
            .build();
MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

//OR

ConnectionFactory factory = ConnectionFactories.get("r2dbc:mariadb://user:password@host:3306/myDB?option1=value");
```

Basic example: 
```java
    MariadbConnectionConfiguration conf = MariadbConnectionConfiguration.builder()
            .host("localhost")
            .port(3306)
            .username("myUser")
            .password("MySuperPassword")
            .database("db")
            .build();
    MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

    MariadbConnection connection = factory.create().block();
    connection.createStatement("SELECT * FROM myTable")
            .execute()
            .flatMap(r -> r.map((row, metadata) -> {
              return "value=" + row.get(0, String.class);
            }));
    connection.close().subscribe();
```

### Connection options

|option|description|type|default| 
|---:|---|:---:|:---:| 
| **`username`** | User to access database. |*string* | 
| **`password`** | User password. |*string* | 
| **`host`** | IP address or DNS of the database server. *Not used when using option `socketPath`*. |*string*| "localhost"|
| **`port`** | Database server port number. *Not used when using option `socketPath`*|*integer*| 3306|
| **`database`** | Default database to use when establishing the connection. | *string* | 
| **`connectTimeout`** | Sets the connection timeout |  *Duration* | 10s|
| **`socketTimeout`** | Sets the socket timeout |  *Duration* | |
| **`tcpKeepAlive`** | Sets socket keep alive  |  *Boolean* | false|
| **`tcpAbortiveClose`** | 	This option can be used in environments where connections are created and closed in rapid succession. Often, it is not possible to create a socket in such an environment after a while, since all local “ephemeral” ports are used up by TCP connections in TCP_WAIT state. Using tcpAbortiveClose works around this problem by resetting TCP connections (abortive or hard close) rather than doing an orderly close. |  *boolean* | false|
| **`socket`** | Permits connections to the database through the Unix domain socket for faster connection whe server is local. |  *string* | 
| **`allowMultiQueries`** | Allows you to issue several SQL statements in a single call. (That is, `INSERT INTO a VALUES('b'); INSERT INTO c VALUES('d');`).  <br/><br/>This may be a **security risk** as it allows for SQL Injection attacks.|  *boolean* | false| 
| **`connectionAttributes`** | When performance_schema is active, permit to send server some client information. <br>Those information can be retrieved on server within tables performance_schema.session_connect_attrs and performance_schema.session_account_connect_attrs. This can permit from server an identification of client/application per connection|*Map<String,String>* | 
| **`sessionVariables`** | Permits to set session variables upon successful connection |  *Map<String,String>* |
| **`tlsProtocol`** |Force TLS/SSL protocol to a specific set of TLS versions (example "TLSv1.2", "TLSv1.3").|*List<String>*| <i>java default</i>|
| **`serverSslCert`** | Permits providing server's certificate in DER form, or server's CA certificate. <br/>This permits a self-signed certificate to be trusted. Can be used in one of 3 forms : <ul><li> serverSslCert=/path/to/cert.pem (full path to certificate)</li><li> serverSslCert=classpath:relative/cert.pem (relative to current classpath)</li><li> as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----"</li></ul> |*String*| |
| **`clientSslCert`** | Permits providing client's certificate in DER form (use only for mutual authentication). Can be used in one of 3 forms : <ul><li>clientSslCert=/path/to/cert.pem (full path to certificate)</li><li> clientSslCert=classpath:relative/cert.pem (relative to current classpath)</li><li> as verbatim DER-encoded certificate string "------BEGIN CERTIFICATE-----"</li></ul> |*String*| |
| **`clientSslKey`** | client private key path(for mutual authentication) |*String* | |
| **`clientSslPassword`** | client private key password |*charsequence* | |
| **`sslMode`** | ssl requirement. Possible value are <ul><li>DISABLED, // NO SSL</li><li>ENABLE_TRUST, // Encryption, but no certificate and hostname validation  (DEVELOPMENT ONLY)</li><li>ENABLE_WITHOUT_HOSTNAME_VERIFICATION, // Encryption, certificates validation, BUT no hostname validation</li><li>ENABLE, // Standard SSL use: Encryption, certificate validation and hostname validation</li></ul> | SslMode |DISABLED|
| **`rsaPublicKey`** | <i>only for MySQL server</i><br/> Server RSA public key, for SHA256 authentication |*String* | |
| **`cachingRsaPublicKey`** | <i>only for MySQL server</i><br/> Server caching RSA public key, for cachingSHA256 authentication |*String* | |
| **`allowPublicKeyRetrieval`** | <i>only for MySQL server</i><br/> Permit retrieved Server RSA public key from server. This can create a security issue |*boolean* | true | 
| **`allowPipelining`** | Permit to send queries to server without waiting for previous query to finish |*boolean* | true | 
| **`useServerPrepStmts`** | Permit to indicate to use text or binary protocol for query with parameter |*boolean* | false | 
| **`prepareCacheSize`** | if useServerPrepStmts = true, cache the prepared informations in a LRU cache to avoid re-preparation of command. Next use of that command, only prepared identifier and parameters (if any) will be sent to server. This mainly permit for server to avoid reparsing query. |*int* |256 |
| **`pamOtherPwd`** | Permit to provide additional password for PAM authentication with multiple authentication step. If multiple passwords, value must be URL encoded.|*string* | |  

## Roadmap

* Performance !
* Fast batch using mariadb bulk
* GeoJSON datatype
* Pluggable types for MariaDB 10.5 (JSON, INET4, INET6, BOOLEAN, ...)


## Tracker 

To file an issue or follow the development, see [JIRA](https://jira.mariadb.org/projects/R2DBC/issues/).


[travis-image]:https://travis-ci.com/mariadb-corporation/mariadb-connector-r2dbc.svg?branch=master
[travis-url]:https://travis-ci.com/mariadb-corporation/mariadb-connector-r2dbc
[maven-image]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb/badge.svg
[maven-url]:https://maven-badges.herokuapp.com/maven-central/org.mariadb/r2dbc-mariadb
[appveyor-image]:https://ci.appveyor.com/api/projects/status/hbfar9vg8w0rw15f/branch/master?svg=true
[appveyor-url]:https://ci.appveyor.com/project/rusher/mariadb-connector-r2dbc-57na3/branch/master
[license-image]:https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license-url]:https://opensource.org/licenses/Apache-2.0
