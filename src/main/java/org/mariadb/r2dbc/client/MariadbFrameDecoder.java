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
import java.util.Queue;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.message.server.PrepareResultPacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.util.concurrent.Queues;

public class MariadbFrameDecoder extends ByteToMessageDecoder {
  private CompositeByteBuf multipart = null;
  private final Queue<Exchange> exchangeQueue;
  private final Client client;
  private final MariadbConnectionConfiguration configuration;
  private DecoderState state = null;
  private final Queue<String> prepareSql = Queues.<String>small().get();
  private long clientCapabilities;
  private int stateCounter = 0;
  private boolean metaFollows = false;
  private PrepareResultPacket prepare;
  private ColumnDefinitionPacket[] prepareColumns;
  private Context context = null;

  public MariadbFrameDecoder(
      Queue<Exchange> exchangeQueue, Client client, MariadbConnectionConfiguration configuration) {
    this.exchangeQueue = exchangeQueue;
    this.client = client;
    this.configuration = configuration;
  }

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
        out.add(decode(multipart));
        //        multipart.release();
        multipart = null;
        continue;
      }

      // create Object from packet
      buf.skipBytes(3); // skip length
      ByteBuf packet = buf.readRetainedSlice(1 + length);
      out.add(decode(packet));
      packet.release();
    }
  }

  private ServerMessage decode(ByteBuf packet) {
    Sequencer sequencer = new Sequencer(packet.readByte());
    Exchange exchange = this.exchangeQueue.peek();
    if (state == null)
      state = exchange == null ? DecoderState.QUERY_RESPONSE : exchange.getInitialState();
    state = state.decoder(packet.getUnsignedByte(packet.readerIndex()), packet.readableBytes());
    ServerMessage msg = state.decode(packet, sequencer, this);
    state = msg.ending() ? null : state.next(this);
    return msg;
  }

  public Context getContext() {
    return context;
  }

  public int getStateCounter() {
    return stateCounter;
  }

  public void setStateCounter(int counter) {
    stateCounter = counter;
  }

  public PrepareResultPacket getPrepare() {
    return prepare;
  }

  public void setPrepare(PrepareResultPacket prepare) {
    this.prepare = prepare;
    this.prepareColumns =
        (prepare == null) ? null : new ColumnDefinitionPacket[prepare.getNumColumns()];
  }

  public ColumnDefinitionPacket[] getPrepareColumns() {
    return prepareColumns;
  }

  public MariadbConnectionConfiguration getConf() {
    return configuration;
  }

  public ServerPrepareResult endPrepare() {
    ServerPrepareResult prepareResult =
        new ServerPrepareResult(
            this.prepare.getStatementId(), this.prepare.getNumParams(), prepareColumns);
    String sql = prepareSql.poll();
    PrepareCache prepareCache = client.getPrepareCache();
    if (prepareCache != null) {
      ServerPrepareResult cached = prepareCache.put(sql, prepareResult);
      if (cached != null) {
        // race condition, remove new one to get the one in cache
        prepareResult.decrementUse(client);
        prepareResult = cached;
      }
    }
    return prepareResult;
  }

  public void decrementStateCounter() {
    stateCounter--;
  }

  public long getClientCapabilities() {
    return clientCapabilities;
  }

  public boolean addPrepare(String sql) {
    return this.prepareSql.offer(sql);
  }

  public void setContext(Context context) {
    this.context = context;
    this.clientCapabilities = this.context.getClientCapabilities();
  }

  public boolean isMetaFollows() {
    return metaFollows;
  }

  public void setMetaFollows(boolean metaFollows) {
    this.metaFollows = metaFollows;
  }
}
