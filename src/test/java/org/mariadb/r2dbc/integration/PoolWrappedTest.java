// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Wrapped;
import java.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import reactor.core.scheduler.Scheduler;

public class PoolWrappedTest extends BaseConnectionTest {

  @Test
  public void testWrappedSchedulerExposure() {
    // Test that MariadbConnection implements Wrapped and exposes Scheduler
    Connection connection = sharedConn;

    Assertions.assertTrue(connection instanceof Wrapped, "Connection should implement Wrapped");

    @SuppressWarnings("unchecked")
    Wrapped<Object> wrapped = (Wrapped<Object>) connection;

    // Test unwrap(Scheduler.class)
    Scheduler scheduler = wrapped.unwrap(Scheduler.class);
    Assertions.assertNotNull(scheduler, "Scheduler should not be null");

    // Test unwrap()
    Object unwrapped = wrapped.unwrap();
    Assertions.assertNull(unwrapped, "unwrap() should return null");
  }

  @Test
  public void testPoolWithWrappedScheduler() throws Exception {
    // Test that r2dbc-pool can use the wrapped scheduler
    MariadbConnectionConfiguration conf = TestConfiguration.defaultBuilder.build();
    MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

    ConnectionPoolConfiguration poolConfig =
        ConnectionPoolConfiguration.builder(factory)
            .maxIdleTime(Duration.ofMinutes(30))
            .maxSize(5)
            .initialSize(1)
            .build();

    ConnectionPool pool = new ConnectionPool(poolConfig);

    try {
      // Get a connection from the pool
      Connection connection = pool.create().block();
      Assertions.assertNotNull(connection, "Pooled connection should not be null");

      // Verify the underlying connection exposes Scheduler
      if (connection instanceof Wrapped) {
        @SuppressWarnings("unchecked")
        Wrapped<Object> wrapped = (Wrapped<Object>) connection;
        Scheduler scheduler = wrapped.unwrap(Scheduler.class);
        Assertions.assertNotNull(scheduler, "Pooled connection should expose scheduler");
      }

      // Test basic operation
      reactor.core.publisher.Flux.from(connection.createStatement("SELECT 1").execute())
          .flatMap(r -> r.map((row, metadata) -> row.get(0, Integer.class)))
          .blockLast();

      reactor.core.publisher.Mono.from(connection.close()).block();
    } finally {
      pool.dispose();
    }
  }

  @Test
  public void testMultiplePooledConnections() throws Exception {
    // Test that multiple pooled connections each expose their own scheduler
    MariadbConnectionConfiguration conf = TestConfiguration.defaultBuilder.build();
    MariadbConnectionFactory factory = new MariadbConnectionFactory(conf);

    ConnectionPoolConfiguration poolConfig =
        ConnectionPoolConfiguration.builder(factory)
            .maxIdleTime(Duration.ofMinutes(30))
            .maxSize(3)
            .initialSize(2)
            .build();

    ConnectionPool pool = new ConnectionPool(poolConfig);

    try {
      Connection conn1 = pool.create().block();
      Connection conn2 = pool.create().block();

      Assertions.assertNotNull(conn1);
      Assertions.assertNotNull(conn2);

      // Both should expose schedulers
      if (conn1 instanceof Wrapped && conn2 instanceof Wrapped) {
        @SuppressWarnings("unchecked")
        Wrapped<Object> wrapped1 = (Wrapped<Object>) conn1;
        @SuppressWarnings("unchecked")
        Wrapped<Object> wrapped2 = (Wrapped<Object>) conn2;

        Scheduler scheduler1 = wrapped1.unwrap(Scheduler.class);
        Scheduler scheduler2 = wrapped2.unwrap(Scheduler.class);

        Assertions.assertNotNull(scheduler1);
        Assertions.assertNotNull(scheduler2);
      }

      reactor.core.publisher.Mono.from(conn1.close()).block();
      reactor.core.publisher.Mono.from(conn2.close()).block();
    } finally {
      pool.dispose();
    }
  }

  @Test
  public void testUnwrapUnsupportedClass() {
    // Test that unwrapping an unsupported class returns null
    Connection connection = sharedConn;

    if (connection instanceof Wrapped) {
      @SuppressWarnings("unchecked")
      Wrapped<Object> wrapped = (Wrapped<Object>) connection;

      String result = wrapped.unwrap(String.class);
      Assertions.assertNull(result, "Unwrapping unsupported class should return null");
    }
  }
}
