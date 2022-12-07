/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public class MariadbFrameDecoder extends ByteToMessageDecoder {
  private CompositeByteBuf multipart = null;

  @Override
  public void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
    while (buf.readableBytes() > 4) {
      int length = buf.getUnsignedMediumLE(buf.readerIndex());

      // packet not complete
      if (buf.readableBytes() < length + 4) return;

      // extract packet
      if (length == 0xffffff) {
        // multipart packet
        if (multipart == null) {
          multipart = buf.alloc().compositeBuffer();
        }
        buf.skipBytes(4); // skip length + header
        multipart.addComponent(true, buf.readRetainedSlice(length));
        continue;
      }

      // wait for complete packet
      if (multipart != null) {
        // last part of multipart packet
        buf.skipBytes(3); // skip length

        // add sequence byte
        multipart.addComponent(true, 0, Unpooled.wrappedBuffer(new byte[] {buf.readByte()}));
        // add data
        multipart.addComponent(true, buf.readRetainedSlice(length));
        out.add(multipart.retain());
        multipart = null;
        continue;
      }

      // create Object from packet
      ByteBuf packet = buf.readRetainedSlice(4 + length);
      packet.skipBytes(3); // skip length
      out.add(packet.retain());
    }
  }
}
