// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.R2dbcBadGrammarException;
import java.util.Optional;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.test.StepVerifier;

public class MultiQueriesTest extends BaseConnectionTest {

  @Test
  void multiQueryDefault() {
    Assumptions.assumeTrue(
        !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")) && !isXpand());
    sharedConn
        .createStatement("SELECT 1; SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
  }

  @Test
  void multiQueryEnable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(1 as CHAR); SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("1"), Optional.of("a"))
        .verifyComplete();
    connection.close().subscribe();
  }

  @Test
  void multiQueryDisable() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")) && !isXpand());

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(1 as CHAR); SELECT 'a'")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
    connection.close().subscribe();
  }

  @Test
  void multiQueryWithParameterDefault() {
    Assumptions.assumeTrue(
        !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")) && !isXpand());

    sharedConn
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
  }

  @Test
  void multiQueryWithParameterEnable() throws Exception {
    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(true).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> Optional.ofNullable(row.get(0))))
        .as(StepVerifier::create)
        .expectNext(Optional.of("1"), Optional.of("a"))
        .verifyComplete();
    connection.close().subscribe();
  }

  @Test
  void multiQueryWithParameterDisable() throws Exception {
    Assumptions.assumeTrue(
        !isMaxscale() && !"skysql-ha".equals(System.getenv("srv")) && !isXpand());

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().allowMultiQueries(false).build();
    MariadbConnection connection = new MariadbConnectionFactory(conf).create().block();
    connection
        .createStatement("SELECT CAST(? as CHAR); SELECT ?")
        .bind(0, 1)
        .bind(1, "a")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0)))
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcBadGrammarException
                    && throwable.getMessage().contains("You have an error in your SQL syntax"))
        .verify();
    connection.close().subscribe();
  }
}
