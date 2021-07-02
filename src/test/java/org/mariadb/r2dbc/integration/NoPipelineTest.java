// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.integration;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.BaseConnectionTest;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.MariadbConnectionFactory;
import org.mariadb.r2dbc.TestConfiguration;
import org.mariadb.r2dbc.api.MariadbConnection;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public class NoPipelineTest extends BaseConnectionTest {

  @Test
  void noPipelineConnect() throws Exception {
    // use sequence engine
    Assumptions.assumeTrue(!isMariaDBServer() && minVersion(10, 0, 3));

    MariadbConnectionConfiguration confPipeline =
        TestConfiguration.defaultBuilder.clone().allowPipelining(true).build();
    MariadbConnection connection = new MariadbConnectionFactory(confPipeline).create().block();

    try {
      runWithPipeline(connection);
    } finally {
      connection.close().block();
    }
  }

  private Duration runWithPipeline(MariadbConnection connection) {
    Instant initial = Instant.now();
    int MAX = 100;
    List<Flux<BigInteger>> fluxes = new ArrayList<>();
    for (int i = 0; i < MAX; i++) {
      fluxes.add(
          connection
              .createStatement("SELECT * from seq_" + (100 * i) + "_to_" + (100 * (i + 1) - 1))
              .execute()
              .flatMap(r -> r.map((row, metadata) -> row.get(0, BigInteger.class))));
    }

    for (int i = 0; i < MAX - 1; i++) {
      fluxes.get(i).subscribe();
    }
    Flux.concat(fluxes.get(MAX - 1)).as(StepVerifier::create).expectNextCount(100).verifyComplete();
    return Duration.between(initial, Instant.now());
  }
}
