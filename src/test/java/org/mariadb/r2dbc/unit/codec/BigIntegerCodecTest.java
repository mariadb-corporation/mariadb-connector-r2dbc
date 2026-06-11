// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.R2dbcException;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.list.BigDecimalCodec;
import org.mariadb.r2dbc.codec.list.BigIntegerCodec;

/**
 * Mirror of {@link BigDecimalCodecTest} for {@link BigIntegerCodec#parseBigInteger(String,
 * ExceptionFactory)} (CONJ-1315). Both helpers share the same cap; the trap they guard against is
 * {@code new BigInteger(String)} having the same O(n²) parsing cost as {@code new
 * BigDecimal(String)}.
 */
public class BigIntegerCodecTest {

  @Test
  void parseAtCapAccepted() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH; i++) sb.append('9');
    BigInteger parsed = BigIntegerCodec.parseBigInteger(sb.toString(), ExceptionFactory.INSTANCE);
    assertEquals(sb.toString(), parsed.toString());
  }

  @Test
  void parseOneOverCapRejected() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH + 1; i++) sb.append('9');
    R2dbcException thrown =
        assertThrows(
            R2dbcException.class,
            () -> BigIntegerCodec.parseBigInteger(sb.toString(), ExceptionFactory.INSTANCE));
    assertTrue(thrown.getMessage().contains("exceeds"));
  }

  @Test
  void parseAttackPayloadRejectedQuickly() {
    StringBuilder sb = new StringBuilder(1_000_000);
    for (int i = 0; i < 1_000_000; i++) sb.append('1');
    long start = System.nanoTime();
    R2dbcException thrown =
        assertThrows(
            R2dbcException.class,
            () -> BigIntegerCodec.parseBigInteger(sb.toString(), ExceptionFactory.INSTANCE));
    long elapsedNanos = System.nanoTime() - start;
    assertTrue(thrown.getMessage().contains("1000000"));
    assertTrue(
        elapsedNanos < 100_000_000L,
        "expected rejection in <100ms, took " + (elapsedNanos / 1_000_000) + "ms");
  }

  @Test
  void parseValidIntegerRoundtrips() {
    assertEquals(BigInteger.ZERO, BigIntegerCodec.parseBigInteger("0", ExceptionFactory.INSTANCE));
    assertEquals(
        new BigInteger("123456789012345"),
        BigIntegerCodec.parseBigInteger("123456789012345", ExceptionFactory.INSTANCE));
    assertEquals(
        new BigInteger("-42"), BigIntegerCodec.parseBigInteger("-42", ExceptionFactory.INSTANCE));
  }

  @Test
  void parseInvalidIntegerStillThrowsNumberFormat() {
    assertThrows(
        NumberFormatException.class,
        () -> BigIntegerCodec.parseBigInteger("not-a-number", ExceptionFactory.INSTANCE));
  }
}
