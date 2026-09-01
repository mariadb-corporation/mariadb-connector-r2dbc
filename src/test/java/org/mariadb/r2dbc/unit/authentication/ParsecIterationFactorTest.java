// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.R2dbcException;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.standard.ParsecPasswordPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;

/**
 * R2DBC-129 (report by fg0x0): the parsec PBKDF2 iteration factor is server given and is an
 * exponent, effective work being 1024 &lt;&lt; factor rounds. It must be bounded by the connection
 * time budget, otherwise a rogue server forces minutes of PBKDF2 per connection attempt.
 */
public class ParsecIterationFactorTest {

  @Test
  void maxIterationFactorFollowsConnectTimeout() {
    assertEquals(6, ParsecPasswordPlugin.maxIterationFactor(Duration.ofMillis(100)));
    assertEquals(11, ParsecPasswordPlugin.maxIterationFactor(Duration.ofMillis(2500)));
    assertEquals(13, ParsecPasswordPlugin.maxIterationFactor(Duration.ofSeconds(10)));
    assertEquals(15, ParsecPasswordPlugin.maxIterationFactor(Duration.ofSeconds(30)));

    // no budget declared: default connect timeout applies
    assertEquals(13, ParsecPasswordPlugin.maxIterationFactor(null));
    assertEquals(13, ParsecPasswordPlugin.maxIterationFactor(Duration.ZERO));
    assertEquals(13, ParsecPasswordPlugin.maxIterationFactor(Duration.ofSeconds(-1)));

    // 1024 << factor must never overflow, whatever the declared budget
    assertEquals(20, ParsecPasswordPlugin.maxIterationFactor(Duration.ofDays(10)));
  }

  @Test
  void iterationFactorAboveBudgetIsRefused() {
    // default 10s budget allows factor 13
    R2dbcException e = assertThrows(R2dbcException.class, () -> parsecNext(14, null));
    assertTrue(e.getMessage().contains("iteration factor 14 exceeds"), e.getMessage());

    e = assertThrows(R2dbcException.class, () -> parsecNext(7, Duration.ofMillis(100)));
    assertTrue(e.getMessage().contains("iteration factor 7 exceeds"), e.getMessage());
  }

  @Test
  void negativeIterationFactorByteIsRefused() {
    // 0x94 is -108 as a signed byte, which passed a `factor > max` validation while the JVM masks
    // the shift distance of `1024 << factor` to its 5 lower bits, ending on the maximum 1024 << 20
    R2dbcException e = assertThrows(R2dbcException.class, () -> parsecNext(0x94, null));
    assertTrue(e.getMessage().contains("iteration factor 148 exceeds"), e.getMessage());

    e = assertThrows(R2dbcException.class, () -> parsecNext(0xff, null));
    assertTrue(e.getMessage().contains("iteration factor 255 exceeds"), e.getMessage());
  }

  @Test
  void wrongKdfAlgorithmIsRefused() {
    R2dbcException e =
        assertThrows(
            R2dbcException.class,
            () ->
                new ParsecPasswordPlugin()
                    .next(configuration(null), new byte[32], sequencer(), extSalt(0x51, 3)));
    assertTrue(e.getMessage().contains("Wrong parsec authentication format"), e.getMessage());
  }

  @Test
  void truncatedExtSaltIsRefused() {
    AuthMoreData authMoreData = authMoreData(Unpooled.wrappedBuffer(new byte[] {0x50, 3}));
    R2dbcException e =
        assertThrows(
            R2dbcException.class,
            () ->
                new ParsecPasswordPlugin()
                    .next(configuration(null), new byte[32], sequencer(), authMoreData));
    assertTrue(e.getMessage().contains("Wrong parsec authentication format"), e.getMessage());
  }

  private void parsecNext(int iterationFactor, Duration connectTimeout) {
    new ParsecPasswordPlugin()
        .next(
            configuration(connectTimeout),
            new byte[32],
            sequencer(),
            extSalt(0x50, iterationFactor));
  }

  private MariadbConnectionConfiguration configuration(Duration connectTimeout) {
    return MariadbConnectionConfiguration.builder()
        .host("localhost")
        .username("user")
        .password("password")
        .connectTimeout(connectTimeout)
        .build();
  }

  private Sequencer sequencer() {
    return new Sequencer((byte) 0);
  }

  /** ext-salt server answer: KDF algorithm, iteration factor, then the salt itself. */
  private AuthMoreData extSalt(int firstByte, int iterationFactor) {
    byte[] extSalt = new byte[20];
    extSalt[0] = (byte) firstByte;
    extSalt[1] = (byte) iterationFactor;
    return authMoreData(Unpooled.wrappedBuffer(extSalt));
  }

  private AuthMoreData authMoreData(ByteBuf buf) {
    return new AuthMoreData() {
      @Override
      public MessageSequence getSequencer() {
        return sequencer();
      }

      @Override
      public ByteBuf getBuf() {
        return buf;
      }
    };
  }
}
