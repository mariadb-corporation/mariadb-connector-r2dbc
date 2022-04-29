// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;

public class MariadbPacketEncoder extends MessageToByteEncoder<ClientMessage> {
  private Context context = null;

  public static CompositeByteBuf encodeFlux(
      ClientMessage msg, Context context, ByteBufAllocator allocator) {
    CompositeByteBuf out = allocator.compositeBuffer();
    try {
      ByteBuf buf = msg.encode(context, allocator);

      int packetLength;
      do {
        packetLength = Math.min(0xffffff, buf.readableBytes());
        ByteBuf header = Unpooled.buffer(4, 4);
        header.writeMediumLE(packetLength);
        header.writeByte(msg.getSequencer().next());
        out.addComponent(true, header);
        out.addComponent(true, buf.slice(buf.readerIndex(), packetLength));
        buf.readerIndex(buf.readerIndex() + packetLength);
      } while (buf.readableBytes() > 0);

      if (packetLength == 0xffffff) {
        // in case last packet is full, sending an empty packet to indicate that command is complete
        ByteBuf header = Unpooled.buffer(4, 4);
        header.writeMediumLE(0);
        header.writeByte(msg.getSequencer().next());
        out.addComponent(true, header);
      }

    } finally {
      msg.releaseEncodedBinds();
    }
    return out;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out)
      throws Exception {

    ByteBuf buf = null;
    try {
      buf = msg.encode(this.context, ctx.alloc());

      // single mysql packet
      if (buf.writerIndex() - buf.readerIndex() < 0xffffff) {
        out.writeMediumLE(buf.writerIndex() - buf.readerIndex());
        out.writeByte(msg.getSequencer().next());
        out.writeBytes(buf);
        //        buf.release();
        return;
      }

      // multiple mysql packet - split in 16mb packet
      int readerIndex = buf.readerIndex();
      int packetLength = -1;
      while (readerIndex < buf.writerIndex()) {
        packetLength = Math.min(0xffffff, buf.writerIndex() - readerIndex);
        out.writeMediumLE(packetLength);
        out.writeByte(msg.getSequencer().next());
        out.writeBytes(buf.slice(readerIndex, packetLength));
        readerIndex += packetLength;
      }

      if (packetLength == 0xffffff) {
        // in case last packet is full, sending an empty packet to indicate that command is complete
        out.writeMediumLE(packetLength);
        out.writeByte(msg.getSequencer().next());
      }

    } finally {
      if (buf != null) buf.release();
      msg.releaseEncodedBinds();
    }
  }

  public void setContext(Context context) {
    this.context = context;
  }
}
