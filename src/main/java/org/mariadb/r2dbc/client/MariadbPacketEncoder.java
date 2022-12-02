// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;

public class MariadbPacketEncoder {
  private Context context = null;

  public CompositeByteBuf encodeFlux(ClientMessage msg) {
    ByteBufAllocator allocator = context.getByteBufAllocator();
    CompositeByteBuf out = allocator.compositeBuffer();

    ByteBuf buf = msg.encode(context, allocator);
    int initialReaderIndex = buf.readerIndex();
    int packetLength;
    do {
      packetLength = Math.min(0xffffff, buf.readableBytes());

      ByteBuf header = Unpooled.buffer(4, 4);
      header.writeMediumLE(packetLength);
      header.writeByte(msg.getSequencer().next());

      out.addComponent(true, header);
      out.addComponent(true, buf.retainedSlice(buf.readerIndex(), packetLength));
      buf.skipBytes(packetLength);
    } while (buf.readableBytes() > 0);

    if (packetLength == 0xffffff) {
      // in case last packet is full, sending an empty packet to indicate that command is complete
      ByteBuf header = Unpooled.buffer(4, 4);
      header.writeMediumLE(0);
      header.writeByte(msg.getSequencer().next());
      out.addComponent(true, header);
    }

    context.saveRedo(msg, buf, initialReaderIndex);
    buf.release();
    return out;
  }

  public void setContext(Context context) {
    this.context = context;
  }
}
