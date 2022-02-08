// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;

public class MariadbPacketEncoder extends MessageToByteEncoder<ClientMessage> {
  private Context context = null;

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
    }
  }

  public void setContext(Context context) {
    this.context = context;
  }
}
