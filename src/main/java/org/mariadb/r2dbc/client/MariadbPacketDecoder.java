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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.mariadb.r2dbc.message.server.Sequencer;

public class MariadbPacketDecoder extends ByteToMessageDecoder {

  private volatile ServerPacketState decoder;
  private ConnectionContext context = null;
  private volatile CompositeByteBuf multipart;
  private AtomicBoolean isMultipart = new AtomicBoolean();

  public MariadbPacketDecoder(ServerPacketState decoder) {
    this.decoder = decoder;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
    if (decoder.response == null && !decoder.loadNextResponse()) {
      throw new R2dbcNonTransientResourceException(
          "unexpected message received when no command was send");
    }
    while (buf.readableBytes() > 4) {
      int length = buf.getUnsignedMediumLE(buf.readerIndex());

      // packet not complete
      if (buf.readableBytes() < length + 4) return;

      // extract packet
      if (length == 0xffffff) {
        // multipart packet
        if (!isMultipart.getAndSet(true)) {
          multipart = buf.alloc().compositeBuffer();
        }
        buf.skipBytes(4); // skip length + header
        multipart.addComponent(true, buf.readRetainedSlice(length));
        continue;
      }

      // wait for complete packet
      if (isMultipart.get()) {
        // last part of multipart packet
        buf.skipBytes(3); // skip length
        Sequencer sequencer = new Sequencer(buf.readByte());
        multipart.addComponent(true, buf.readRetainedSlice(length));

        decoder.next.apply(multipart, sequencer, context);

        multipart.release();
        isMultipart.set(false);
        continue;
      }

      // create Object from packet
      ByteBuf packet = buf.readSlice(4 + length);
      packet.skipBytes(3); // skip length
      Sequencer sequencer = new Sequencer(packet.readByte());
      decoder.next.apply(packet, sequencer, context);
    }
  }

  public void setContext(ConnectionContext context) {
    this.context = context;
  }
}
