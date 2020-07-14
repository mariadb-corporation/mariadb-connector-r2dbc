/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.integration;

import io.r2dbc.spi.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class ErrorTest extends BaseTest {

  @AfterAll
  public static void after2() {
    sharedConn.createStatement("DROP TABLE deadlock").execute().blockLast();
  }

  @Test
  void queryTimeout() throws Exception {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(10, 2, 0));
    MariadbConnection connection = factory.create().block();
    connection
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
    sharedConn.createStatement("CREATE USER userWithoutRight").execute().blockLast();

    MariadbConnectionConfiguration conf =
        TestConfiguration.defaultBuilder.clone().username("userWithoutRight").password("").build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                        .getMessage()
                        .contains("Access denied for user 'userWithoutRight'@'%' to database")))
        .verify();

    conf =
        TestConfiguration.defaultBuilder
            .clone()
            .username("userWithoutRight")
            .password("wrongpassword")
            .build();
    new MariadbConnectionFactory(conf)
        .create()
        .as(StepVerifier::create)
        .expectErrorMatches(
            throwable ->
                throwable instanceof R2dbcNonTransientResourceException
                    && (throwable
                        .getMessage()
                        .contains("Access denied for user 'userWithoutRight'")))
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
    MariadbConnection connection = null;
    MariadbConnection connection2 = null;
    try {
      connection2 = factory.create().block();
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
      connection.close().block();
      connection2.close().block();
    }
  }
}
