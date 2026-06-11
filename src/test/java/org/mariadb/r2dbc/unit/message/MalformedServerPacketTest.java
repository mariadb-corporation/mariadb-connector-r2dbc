// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.message;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.Unpooled;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.standard.CachingSha2PasswordFlow;
import org.mariadb.r2dbc.message.client.NativePasswordPacket;
import org.mariadb.r2dbc.message.client.Sha256PasswordPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

/**
 * A malicious/buggy server must not be able to make connection establishment throw a raw
 * RuntimeException (NegativeArraySizeException / ArithmeticException) via an empty auth-switch seed
 * or a truncated column-definition packet.
 */
public class MalformedServerPacketTest {

  @Test
  void truncatedSeedHandlesEmpty() {
    assertArrayEquals(new byte[0], AuthSwitchPacket.getTruncatedSeed(new byte[0]));
    assertArrayEquals(new byte[] {1, 2}, AuthSwitchPacket.getTruncatedSeed(new byte[] {1, 2, 3}));
  }

  @Test
  void emptySeedDoesNotCrashNativeAndCachingSha2() {
    // previously: `new byte[seed.length - 1]` -> new byte[-1] -> NegativeArraySizeException
    assertDoesNotThrow(() -> new NativePasswordPacket(null, "pwd", new byte[0]));
    assertDoesNotThrow(() -> CachingSha2PasswordFlow.sha256encryptPassword("pwd", new byte[0]));
  }

  @Test
  void emptySeedRejectedCleanlyBySha256() {
    // previously: `seed[i % seedLength]` with seedLength == 0 -> ArithmeticException (/ by zero)
    assertThrows(
        R2dbcException.class, () -> Sha256PasswordPacket.encrypt(null, "pwd", new byte[0]));
  }

  @Test
  void truncatedColumnDefinitionRejectedCleanly() throws Exception {
    // previously: `new byte[readableBytes() - 12]` -> new byte[-7] -> NegativeArraySizeException
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder().host("localhost").username("u").build();
    assertThrows(
        R2dbcException.class,
        () -> ColumnDefinitionPacket.decode(Unpooled.wrappedBuffer(new byte[5]), true, conf));
  }
}
