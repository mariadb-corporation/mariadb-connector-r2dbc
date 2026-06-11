// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.r2dbc.spi.R2dbcException;
import java.math.BigDecimal;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.list.BigDecimalCodec;

/**
 * Regression test for the CPU-exhaustion issue (CONJ-1315): attacker-controlled long-digit strings
 * reaching {@code new BigDecimal(String)} cause O(n²) parsing inside the JDK constructor. {@link
 * BigDecimalCodec#parseBigDecimal(String, ExceptionFactory)} caps input at {@link
 * BigDecimalCodec#MAX_BIG_DECIMAL_STRING_LENGTH} characters and rejects longer input.
 */
public class BigDecimalCodecTest {

  @Test
  void parseAtCapAccepted() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH; i++) sb.append('9');
    BigDecimal parsed = BigDecimalCodec.parseBigDecimal(sb.toString(), ExceptionFactory.INSTANCE);
    assertEquals(sb.toString(), parsed.toPlainString());
  }

  @Test
  void parseOneOverCapRejected() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < BigDecimalCodec.MAX_BIG_DECIMAL_STRING_LENGTH + 1; i++) sb.append('9');
    R2dbcException thrown =
        assertThrows(
            R2dbcException.class,
            () -> BigDecimalCodec.parseBigDecimal(sb.toString(), ExceptionFactory.INSTANCE));
    assertTrue(thrown.getMessage().contains("exceeds"));
  }

  @Test
  void parseAttackPayloadRejectedQuickly() {
    // A 1,000,000-digit payload would hold a worker thread for seconds under the unguarded path.
    // The cap check is a String.length() comparison — sub-millisecond on any JVM.
    StringBuilder sb = new StringBuilder(1_000_000);
    for (int i = 0; i < 1_000_000; i++) sb.append('1');
    long start = System.nanoTime();
    R2dbcException thrown =
        assertThrows(
            R2dbcException.class,
            () -> BigDecimalCodec.parseBigDecimal(sb.toString(), ExceptionFactory.INSTANCE));
    long elapsedNanos = System.nanoTime() - start;
    assertTrue(thrown.getMessage().contains("1000000"));
    assertTrue(
        elapsedNanos < 100_000_000L,
        "expected rejection in <100ms, took " + (elapsedNanos / 1_000_000) + "ms");
  }

  @Test
  void parseValidNumberRoundtrips() {
    assertEquals(BigDecimal.ZERO, BigDecimalCodec.parseBigDecimal("0", ExceptionFactory.INSTANCE));
    assertEquals(
        new BigDecimal("3.14159265358979"),
        BigDecimalCodec.parseBigDecimal("3.14159265358979", ExceptionFactory.INSTANCE));
    assertEquals(
        new BigDecimal("-1e10"),
        BigDecimalCodec.parseBigDecimal("-1e10", ExceptionFactory.INSTANCE));
  }

  @Test
  void parseInvalidNumberStillThrowsNumberFormat() {
    // Below-cap garbage propagates as NumberFormatException (callers wrap with column context).
    assertThrows(
        NumberFormatException.class,
        () -> BigDecimalCodec.parseBigDecimal("not-a-number", ExceptionFactory.INSTANCE));
  }
}
