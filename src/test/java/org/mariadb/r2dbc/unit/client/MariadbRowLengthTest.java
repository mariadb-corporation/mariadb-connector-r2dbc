// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.client.MariadbRowMetadata;
import org.mariadb.r2dbc.client.MariadbRowText;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

/**
 * A row-field length is read from the (server-controlled) packet; it must be validated against the
 * bytes actually present before a codec allocates {@code new byte[length]}. Otherwise a malicious
 * or buggy server can make a tiny packet trigger a multi-GB allocation (OutOfMemoryError). With the
 * guard, an over-long field length is rejected with a clean protocol exception instead.
 */
public class MariadbRowLengthTest {

  /** Build a single TEXT column (server type 0xfc, utf8 charset) so byte[] decoding is selected. */
  private static ColumnDefinitionPacket textColumn() throws Exception {
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder().host("localhost").username("u").build();
    byte[] colDef = {
      0,
      0,
      0,
      0,
      0,
      0, // catalog/schema/table/org_table/name/org_name (empty length-enc strings)
      33,
      0, //            charset = utf8
      (byte) 0xFF,
      (byte) 0xFF,
      (byte) 0xFF,
      0, // column length
      (byte) 252, //       column type 0xfc -> TEXT (charset != binary)
      0,
      0, //             flags
      0, //                decimals
      0,
      0 //              filler
    };
    return ColumnDefinitionPacket.decode(Unpooled.wrappedBuffer(colDef), true, conf);
  }

  @Test
  void fieldLengthExceedingPacketIsRejected() throws Exception {
    MariadbRowMetadata meta = new MariadbRowMetadata(new ColumnDefinitionPacket[] {textColumn()});

    // Text row, field 0: 0xfe length-encoded header declaring ~2GB, but NO field bytes follow.
    // Without the guard this length reaches `new byte[length]` in ByteArrayCodec ->
    // OutOfMemoryError.
    ByteBuf buf =
        Unpooled.wrappedBuffer(
            new byte[] {
              (byte) 254, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7F, 0, 0, 0, 0
            });
    MariadbRowText row = new MariadbRowText(buf, meta, ExceptionFactory.INSTANCE);

    assertThrows(R2dbcException.class, () -> row.get(0, byte[].class));
  }
}
