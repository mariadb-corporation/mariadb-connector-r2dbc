// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import reactor.core.publisher.Mono;

public class MariadbPacketEncoder {
  private final MariadbConnectionConfiguration configuration;
  private Context context = null;

  public MariadbPacketEncoder(MariadbConnectionConfiguration configuration) {
    this.configuration = configuration;
  }

  public Mono<CompositeByteBuf> encodeFlux(ClientMessage msg) {
    ByteBufAllocator allocator = context.getByteBufAllocator();

    return msg.encode(context, allocator)
        .map(
            buf -> {
              try {
                checkMaxAllowedLength(buf.readableBytes());
              } catch (RuntimeException e) {
                buf.release();
                throw e;
              }
              CompositeByteBuf out = allocator.compositeBuffer();

              int initialReaderIndex = buf.readerIndex();
              int packetLength;
              do {
                packetLength = Math.min(0xffffff, buf.readableBytes());

                ByteBuf header = Unpooled.buffer(4, 4);
                header.writeMediumLE(packetLength);
                header.writeByte(msg.getSequencer().next());

                out.addComponent(true, header);
                out.addComponent(true, buf.readRetainedSlice(packetLength));

              } while (buf.readableBytes() > 0);

              if (packetLength == 0xffffff) {
                // in case last packet is full, sending an empty packet to indicate that command is
                // complete
                ByteBuf header = Unpooled.buffer(4, 4);
                header.writeMediumLE(0);
                header.writeByte(msg.getSequencer().next());
                out.addComponent(true, header);
              }

              context.saveRedo(msg, buf, initialReaderIndex);
              buf.release();
              return out;
            });
  }

  /**
   * Ensure a command stays within the configured {@code maxAllowedPacket}. Checked on the whole
   * command payload, before it is split into protocol packets, since that is the size the server
   * compares against its own {@code max_allowed_packet}. No limit applies when the option is unset.
   *
   * @param length command payload length
   * @throws MaxAllowedPacketException if the command is larger than the configured limit
   */
  private void checkMaxAllowedLength(int length) {
    Integer maxAllowedPacket = configuration == null ? null : configuration.getMaxAllowedPacket();
    if (maxAllowedPacket != null && length > maxAllowedPacket) {
      throw new MaxAllowedPacketException(
          "Command size ("
              + length
              + " bytes) is greater than maxAllowedPacket ("
              + maxAllowedPacket
              + "). The connection has been closed.");
    }
  }

  public void setContext(Context context) {
    this.context = context;
  }
}
