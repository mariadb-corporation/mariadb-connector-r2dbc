// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.api.MariadbConnectionMetadata;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ErrorTest extends BaseConnectionTest {

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE IF EXISTS deadlock").execute().blockLast();
  }

  @Test
  @Disabled
  void queryTimeout() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 2, 0));
    sharedConn
        .createStatement(
            "SET STATEMENT max_statement_time=0.01 FOR "
                + "SELECT * FROM information_schema.tables, information_schema.tables as t2")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcTimeoutException
                    && throwable.getMessage().contains("Query execution was interrupted"))
        .verify();
  }

  @Test
  void permissionDenied() throws Exception {
    sharedConn.createStatement("CREATE USER IF NOT EXISTS userWithoutRight"+getHostSuffix()).execute().blockLast();
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder
            .clone()
            .allowPublicKeyRetrieval(true)
            .username("userWithoutRight")
            .password("")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable.getMessage().contains("Access denied for user 'userWithoutRight'")
                        || throwable.getMessage().contains("Insufficient user permissions")))
        .verify();

    conf =
        TestConfiguration.defaultBuilder
            .clone()
            .allowPublicKeyRetrieval(true)
            .username("userWithoutRight")
            .password("wrongpassword")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && throwable.getMessage().contains("Fail to establish connection to")
                    && (throwable
                            .getCause()
                            .getMessage()
                            .contains("Access denied for user 'userWithoutRight'")
                        || throwable.getMessage().contains("Access denied")))
        .verify();
  }

  @Test
  void dataIntegrity() throws Exception {
    sharedConn
        .createStatement("CREATE TEMPORARY TABLE dataIntegrity(t1 VARCHAR(5))")
        .execute()
        .blockLast();
    sharedConn
        .createStatement("INSERT INTO dataIntegrity VALUE ('DATATOOOBIG')")
        .execute()
        .flatMap(r -> r.getRowsUpdated())
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("Data too long"))
        .verify();
  }

  @Test
  void rollbackException() {
    Assumptions.assumeTrue(
        !isMaxscale()
            && !"skysql".equals(System.getenv("srv"))
            && !"skysql-ha".equals(System.getenv("srv"))
            && !isXpand());
    MariadbConnection connection = null;
    MariadbConnection connection2 = null;
    try {
      connection2 = factory.create().block();
      connection2.createStatement("DROP TABLE IF EXISTS deadlock").execute().blockLast();
      connection2
          .createStatement("CREATE TABLE deadlock(a int primary key) engine=innodb")
          .execute()
          .blockLast();
      connection2.createStatement("insert into deadlock(a) values(0), (1)").execute().blockLast();
      connection2.setTransactionIsolationLevel(IsolationLevel.SERIALIZABLE);

      connection2.beginTransaction().block();
      connection2.createStatement("update deadlock set a = 2 where a <> 0").execute().blockLast();
      connection = factory.create().block();
      connection
          .createStatement("SET SESSION innodb_lock_wait_timeout=1")
          .execute()
          .map(res -> res.getRowsUpdated())
          .onErrorReturn(Mono.empty())
          .blockLast();
      connection.beginTransaction().block();
      connection
          .createStatement("update deadlock set a = 3 where a <> 1")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .as(StepVerifier::create)
          .expectErrorMatches(
              throwable ->
                  throwable instanceof R2dbcTransientResourceException
                      && throwable
                          .getMessage()
                          .contains("Lock wait timeout exceeded; try restarting transaction"))
          .verify();

    } finally {
      try {
        if (connection != null) connection.close().block();
      } catch (Throwable e) {
      } finally {
        try {
          if (connection2 != null) connection2.close().block();
        } catch (Throwable e) {
        }
      }
    }
  }

  @Test
  void closeDuringSelect() {
    // sequence table requirement
    MariadbConnectionMetadata meta = sharedConn.getMetadata();
    Assumptions.assumeTrue(meta.isMariaDBServer() && minVersion(10, 1, 0));

    MariadbConnection connection2 = factory.create().block();
    connection2
        .createStatement("SELECT * FROM seq_1_to_100000")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectNextCount(100000)
        .verifyComplete();
    connection2.close().block();
  }
}
