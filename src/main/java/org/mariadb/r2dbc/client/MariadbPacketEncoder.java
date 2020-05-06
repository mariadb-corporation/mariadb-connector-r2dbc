/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.mariadb.r2dbc.message.client.ClientMessage;
import reactor.util.Logger;
import reactor.util.Loggers;

public class MariadbPacketEncoder extends MessageToByteEncoder<ClientMessage> {
  private static final Logger logger = Loggers.getLogger(MessageToByteEncoder.class);
  private ConnectionContext context = null;

  @Override
  protected void encode(ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out)
      throws Exception {
    if (logger.isDebugEnabled()) {
      logger.debug("Request:  {}", msg);
    }

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

  public void setContext(ConnectionContext context) {
    this.context = context;
  }
}
