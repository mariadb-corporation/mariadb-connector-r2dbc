// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.client.Exchange;
import org.mariadb.r2dbc.client.MariadbFrameDecoder;
import org.mariadb.r2dbc.client.SimpleContext;
import org.mariadb.r2dbc.message.Context;
import reactor.util.concurrent.Queues;

/**
 * {@link MariadbFrameDecoder} reassembles multipart packets by appending each 0xffffff-length
 * fragment to a growing buffer with no size ceiling. During the handshake/authentication phase no
 * legitimate packet is anywhere near 16MB, so a rogue or MitM'd server could otherwise stream
 * endless max-length fragments to drive the client to {@link OutOfMemoryError} before
 * authentication. The decoder now refuses any multipart packet until the context is initialized.
 */
public class MariadbFrameDecoderTest {

  private static MariadbFrameDecoder decoder() throws Exception {
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder().host("localhost").username("u").build();
    return new MariadbFrameDecoder(Queues.<Exchange>small().get(), null, conf);
  }

  /**
   * A single 0xffffff-length fragment header (3-byte little-endian length + sequence + payload).
   */
  private static ByteBuf multipartFragment() {
    return Unpooled.wrappedBuffer(
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x00, 0x00, 0x00});
  }

  @Test
  void multipartRejectedBeforeContextExists() throws Exception {
    // No InitialHandshakePacket processed yet -> decoder context is null (initial handshake phase).
    MariadbFrameDecoder decoder = decoder();
    ByteBuf buf = multipartFragment();
    try {
      assertThrows(
          R2dbcNonTransientResourceException.class,
          () -> decoder.decode(null, buf, new ArrayList<>()));
    } finally {
      buf.release();
    }
  }

  @Test
  void multipartRejectedDuringAuthentication() throws Exception {
    // Context set (handshake received) but authentication not yet completed.
    MariadbFrameDecoder decoder = decoder();
    Context context = notInitializedContext();
    decoder.setContext(context);

    ByteBuf buf = multipartFragment();
    try {
      R2dbcNonTransientResourceException ex =
          assertThrows(
              R2dbcNonTransientResourceException.class,
              () -> decoder.decode(null, buf, new ArrayList<>()));
      assertEquals("08000", ex.getSqlState());
    } finally {
      buf.release();
    }
  }

  @Test
  void multipartAcceptedOnceInitialized() throws Exception {
    // After authentication, large (multipart) result sets must still be reassembled: the guard must
    // not fire, so the decoder waits for the rest of the fragment instead of throwing.
    MariadbFrameDecoder decoder = decoder();
    Context context = notInitializedContext();
    context.setInitialized();
    decoder.setContext(context);

    ByteBuf buf = multipartFragment();
    try {
      // Only the fragment header is present; without the guard the decoder simply waits for the
      // remaining bytes and returns without throwing.
      decoder.decode(null, buf, new ArrayList<>());
    } finally {
      buf.release();
    }
  }

  private static Context notInitializedContext() {
    return new SimpleContext(
        "10.11.0-MariaDB", 1L, 0L, (short) 0, true, 0L, "db", PooledByteBufAllocator.DEFAULT, null);
  }
}
