// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * fragment to a growing buffer, so without a ceiling a rogue or MitM'd server could stream endless
 * max-length fragments to drive the client to {@link OutOfMemoryError}. Received packets are
 * therefore size-checked as soon as their length header is seen, against a 1Mb cap until
 * authentication completes and against maxAllowedPacket (or a heap-relative default) afterwards.
 */
public class MariadbFrameDecoderTest {

  private static final int ONE_MB = 1024 * 1024;

  private static MariadbFrameDecoder decoder(Integer maxAllowedPacket) throws Exception {
    MariadbConnectionConfiguration conf =
        MariadbConnectionConfiguration.builder()
            .host("localhost")
            .username("u")
            .maxAllowedPacket(maxAllowedPacket)
            .build();
    return new MariadbFrameDecoder(Queues.<Exchange>small().get(), null, conf);
  }

  private static MariadbFrameDecoder decoder() throws Exception {
    return decoder(null);
  }

  /**
   * A packet header (3-byte little-endian length + sequence) announcing {@code length} payload
   * bytes, followed by only the first two of them: enough for the decoder to read the header, but
   * incomplete, so a packet within the limit simply waits for the rest instead of being decoded.
   */
  private static ByteBuf packetFragment(int length) {
    ByteBuf buf = Unpooled.buffer(6);
    buf.writeMediumLE(length);
    buf.writeByte(0);
    buf.writeZero(2);
    return buf;
  }

  /** A single 0xffffff-length fragment header, announcing a multipart packet. */
  private static ByteBuf multipartFragment() {
    return packetFragment(0xffffff);
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
    decoder.setContext(notInitializedContext());

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
  void oversizedPacketRejectedBeforeAuthentication() throws Exception {
    // Not multipart, but still far beyond anything the handshake phase legitimately sends.
    MariadbFrameDecoder decoder = decoder();
    decoder.setContext(notInitializedContext());

    ByteBuf buf = packetFragment(ONE_MB + 1);
    try {
      R2dbcNonTransientResourceException ex =
          assertThrows(
              R2dbcNonTransientResourceException.class,
              () -> decoder.decode(null, buf, new ArrayList<>()));
      assertTrue(
          ex.getMessage().contains("before authentication completes"),
          "unexpected message: " + ex.getMessage());
    } finally {
      buf.release();
    }
  }

  @Test
  void packetWithinPreAuthCapAccepted() throws Exception {
    // At the cap exactly: the decoder must wait for the remaining bytes rather than reject.
    MariadbFrameDecoder decoder = decoder();
    decoder.setContext(notInitializedContext());

    ByteBuf buf = packetFragment(ONE_MB);
    try {
      decoder.decode(null, buf, new ArrayList<>());
    } finally {
      buf.release();
    }
  }

  @Test
  void multipartAcceptedOnceInitialized() throws Exception {
    // After authentication, large (multipart) result sets must still be reassembled: with no
    // maxAllowedPacket set the default ceiling is at least 16Mb, so a single 0xffffff fragment is
    // under the limit and the decoder waits for the remaining bytes instead of throwing.
    MariadbFrameDecoder decoder = decoder();
    decoder.setContext(initializedContext());

    ByteBuf buf = multipartFragment();
    try {
      decoder.decode(null, buf, new ArrayList<>());
    } finally {
      buf.release();
    }
  }

  @Test
  void oversizedPacketRejectedAgainstConfiguredMaxAllowedPacket() throws Exception {
    // Once authenticated, a configured maxAllowedPacket replaces the pre-auth cap - in both
    // directions: 8Kb is well under the 1Mb connection-phase cap.
    MariadbFrameDecoder decoder = decoder(8192);
    decoder.setContext(initializedContext());

    ByteBuf buf = packetFragment(8193);
    try {
      R2dbcNonTransientResourceException ex =
          assertThrows(
              R2dbcNonTransientResourceException.class,
              () -> decoder.decode(null, buf, new ArrayList<>()));
      assertTrue(
          ex.getMessage().contains("maxAllowedPacket (8192)"),
          "unexpected message: " + ex.getMessage());
    } finally {
      buf.release();
    }
  }

  @Test
  void packetWithinConfiguredMaxAllowedPacketAccepted() throws Exception {
    MariadbFrameDecoder decoder = decoder(8192);
    decoder.setContext(initializedContext());

    ByteBuf buf = packetFragment(8192);
    try {
      decoder.decode(null, buf, new ArrayList<>());
    } finally {
      buf.release();
    }
  }

  /**
   * The reassembled total is what ends up in memory, so a multipart packet must be checked on its
   * running total rather than fragment by fragment: each fragment on its own is always under a
   * 16Mb-or-more limit.
   */
  @Test
  void multipartRejectedOnceAccumulatedSizeExceedsLimit() throws Exception {
    int maxAllowedPacket = 20 * 1024 * 1024;
    MariadbFrameDecoder decoder = decoder(maxAllowedPacket);
    decoder.setContext(initializedContext());

    // one complete 0xffffff fragment (~16Mb, accepted), immediately followed by the header of a
    // second one: 32Mb total announced, beyond the 20Mb limit.
    ByteBuf buf = Unpooled.buffer(0xffffff + 12);
    buf.writeMediumLE(0xffffff);
    buf.writeByte(0);
    buf.writeZero(0xffffff);
    buf.writeMediumLE(0xffffff);
    buf.writeByte(1);
    buf.writeZero(2);
    try {
      R2dbcNonTransientResourceException ex =
          assertThrows(
              R2dbcNonTransientResourceException.class,
              () -> decoder.decode(null, buf, new ArrayList<>()));
      assertTrue(
          ex.getMessage().contains("maxAllowedPacket (" + maxAllowedPacket + ")"),
          "unexpected message: " + ex.getMessage());
    } finally {
      buf.release();
    }
  }

  private static Context notInitializedContext() {
    return new SimpleContext(
        "10.11.0-MariaDB", 1L, 0L, (short) 0, true, 0L, "db", PooledByteBufAllocator.DEFAULT, null);
  }

  private static Context initializedContext() {
    Context context = notInitializedContext();
    context.setInitialized();
    return context;
  }
}
