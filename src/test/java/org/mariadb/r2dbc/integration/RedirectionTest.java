// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import java.io.IOException;
import org.junit.jupiter.api.*;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import org.mariadb.r2dbc.tools.TcpProxy;
import reactor.test.StepVerifier;

public class RedirectionTest extends BaseConnectionTest {
  @Test
  void basicRedirection() throws Exception {

    MariadbConnection connection = createProxyCon();
    Assertions.assertEquals(proxy.getLocalPort(), connection.getPort());
    boolean permitRedirection = true;
    try {
      connection
          .createStatement(
              String.format(
                  "set @@session.redirect_url=\"mariadb://%s:%s\"",
                  TestConfiguration.defaultConf.getHostAddresses().get(0).getHost(),
                  TestConfiguration.defaultConf.getPort()))
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .blockLast();
    } catch (Exception e) {
      // if server doesn't support redirection
      permitRedirection = false;
    }
    connection
        .createStatement("SELECT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(1))
        .verifyComplete();

    if (permitRedirection) {
      Assertions.assertEquals(TestConfiguration.defaultConf.getPort(), connection.getPort());
    }
    connection.close().block();
    proxy.stop();
  }

  @Test
  void redirectionDuringTransaction() throws Exception {

    MariadbConnection connection = createProxyCon();
    Assertions.assertEquals(proxy.getLocalPort(), connection.getPort());
    boolean permitRedirection = true;
    connection
        .createStatement("SELECT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(1))
        .expectComplete()
        .verifyLater();
    connection.beginTransaction().block();
    try {
      connection
          .createStatement("set @@session.redirect_url=null")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .blockLast();
      connection
          .createStatement(
              String.format(
                  "set @@session.redirect_url=\"mariadb://%s:%s\"",
                  TestConfiguration.defaultConf.getHostAddresses().get(0).getHost(),
                  TestConfiguration.defaultConf.getPort()))
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .blockLast();
    } catch (Exception e) {
      // if server doesn't support redirection
      permitRedirection = false;
    }

    connection
        .createStatement("SELECT 5")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(5))
        .verifyComplete();
    Assertions.assertEquals(proxy.getLocalPort(), connection.getPort());
    connection.commitTransaction().block();
    if (permitRedirection) {
      Assertions.assertEquals(TestConfiguration.defaultConf.getPort(), connection.getPort());
    }
    connection.close().block();
    proxy.stop();
  }

  @Test
  void redirectionDuringPipeline() throws Exception {

    MariadbConnection connection = createProxyCon();
    Assertions.assertEquals(proxy.getLocalPort(), connection.getPort());
    boolean permitRedirection = true;
    try {
      connection
          .createStatement("SELECT 1")
          .execute()
          .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
          .as(StepVerifier::create)
          .expectNext(Integer.valueOf(1))
          .expectComplete()
          .verifyLater();
      connection
          .createStatement("set @@session.redirect_url=\"\"")
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .blockLast();
    } catch (Exception e) {
      // if server doesn't support redirection
      permitRedirection = false;
    }

    connection
        .createStatement("SELECT 1")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(1))
        .expectComplete()
        .verifyLater();
    ;
    connection
        .createStatement("SELECT 2")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(2))
        .expectComplete()
        .verifyLater();
    if (permitRedirection) {
      connection
          .createStatement(
              String.format(
                  "set @@session.redirect_url=\"mariadb://%s:%s\"",
                  TestConfiguration.defaultConf.getHostAddresses().get(0).getHost(),
                  TestConfiguration.defaultConf.getPort()))
          .execute()
          .blockLast();
    }
    connection
        .createStatement("SELECT 4")
        .execute()
        .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
        .as(StepVerifier::create)
        .expectNext(Integer.valueOf(4))
        .verifyComplete();
    if (permitRedirection) {
      Assertions.assertEquals(TestConfiguration.defaultConf.getPort(), connection.getPort());
    }
    connection.close().block();
    proxy.stop();
  }

  @Test
  void connectionRedirection() throws Exception {
    // need maxscale 23.08+
    Assumptions.assumeTrue(getMaxScaleVersion() >= 230800);
    try {
      proxy =
          new TcpProxy(
              TestConfiguration.defaultConf.getHostAddresses().get(0).getHost(),
              TestConfiguration.defaultConf.getPort());
    } catch (IOException i) {
      throw new Exception("proxy error", i);
    }
    try {
      sharedConn
          .createStatement(
              String.format(
                  "set @@global.redirect_url=\"mariadb://localhost:%s\"", proxy.getLocalPort()))
          .execute()
          .flatMap(r -> r.getRowsUpdated())
          .blockLast();
    } catch (Exception e) {
      Assumptions.abort("not supporting redirection");
    }
    MariadbConnectionConfiguration confProxy =
        TestConfiguration.defaultBuilder
            .clone()
            .port(proxy.getLocalPort())
            .host("localhost")
            .build();
    try {
      MariadbConnection connection = new MariadbConnectionFactory(confProxy).create().block();
      try {
        Assertions.assertEquals(TestConfiguration.defaultConf.getPort(), connection.getPort());
      } finally {
        connection.close().block();
      }
    } finally {
      proxy.stop();
      sharedConn.createStatement("set @@global.redirect_url=\"\"").execute().blockLast();
    }
  }
}
