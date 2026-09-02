// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
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

  /**
   * Cap on a received packet until authentication completes. No legitimate handshake/authentication
   * packet comes close to 1Mb, so this bounds what a rogue or MitM'd server can make the client
   * buffer before it has proven itself.
   */
  private static final long MAX_PACKET_LENGTH_BEFORE_AUTH = 1024 * 1024;

  /**
   * Cap on a received packet once authenticated, when {@code maxAllowedPacket} is not configured. A
   * quarter of the JVM max heap, clamped between 16Mb and 1Gb: a server-independent ceiling that
   * keeps a single result set from exhausting the heap, without depending on the server's {@code
   * max_allowed_packet}.
   */
  private static final long DEFAULT_MAX_RECEIVE_PACKET_LENGTH =
      Math.max(
          16L * 1024 * 1024, Math.min(1024L * 1024 * 1024, Runtime.getRuntime().maxMemory() / 4));

  private final Queue<Exchange> exchangeQueue;
  private final Client client;
  private final MariadbConnectionConfiguration configuration;
  private final Queue<String> prepareSql = Queues.<String>small().get();
  private CompositeByteBuf multipart = null;
  private DecoderState state = null;
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

      // Reject an oversized packet
      checkPacketLength((multipart == null ? 0L : multipart.readableBytes()) + length);

      // packet not complete
      if (buf.readableBytes() < length + 4) return;

      // extract packet
      if (length == 0xffffff) {
        // multipart packet
        if (multipart == null) {
          multipart = buf.alloc().compositeBuffer();
        }
        buf.skipBytes(4); // skip length + header
        ByteBuf slice = buf.readRetainedSlice(length);
        try {
          multipart.addComponent(true, slice);
        } catch (Throwable t) {
          slice.release();
          throw t;
        }
        continue;
      }

      // wait for complete packet
      if (multipart != null) {
        // last part of multipart packet
        buf.skipBytes(3); // skip length

        // add sequence byte
        multipart.addComponent(true, 0, Unpooled.wrappedBuffer(new byte[] {buf.readByte()}));
        // add data
        ByteBuf dataSlice = buf.readRetainedSlice(length);
        boolean sliceAdded = false;
        try {
          multipart.addComponent(true, dataSlice);
          sliceAdded = true;
          out.add(decode(multipart));
        } catch (Throwable t) {
          // If addComponent failed, release the slice that wasn't added to multipart
          if (!sliceAdded) {
            dataSlice.release();
          }
          throw t;
        } finally {
          multipart.release();
          multipart = null;
        }
        continue;
      }

      // create Object from packet
      buf.skipBytes(3); // skip length
      ByteBuf packet = null;
      try {
        packet = buf.readRetainedSlice(1 + length);
        out.add(decode(packet));
      } finally {
        if (packet != null) {
          packet.release();
        }
      }
    }
  }

  @Override
  protected void handlerRemoved0(ChannelHandlerContext ctx) {
    // a partially reassembled multipart packet is still holding retained slices when the connection
    // goes away - on an oversized packet, on a decoding error, or on a plain close.
    if (multipart != null) {
      multipart.release();
      multipart = null;
    }
  }

  /**
   * Ensure a packet the server is about to send stays within the limit applicable to the current
   * phase.
   *
   * @param length packet length, or running total for a multipart packet
   * @throws R2dbcNonTransientResourceException if the limit is exceeded, closing the connection:
   *     the remaining bytes of the rejected packet would desynchronize the stream anyway.
   */
  private void checkPacketLength(long length) {
    boolean authenticated = context != null && context.isInitialized();
    Integer confMax = configuration == null ? null : configuration.getMaxAllowedPacket();
    long limit;
    String limitDescription;
    if (!authenticated) {
      limit = MAX_PACKET_LENGTH_BEFORE_AUTH;
      limitDescription = "the " + limit + " bytes limit applicable before authentication completes";
    } else if (confMax != null) {
      limit = confMax;
      limitDescription = "maxAllowedPacket (" + limit + ")";
    } else {
      limit = DEFAULT_MAX_RECEIVE_PACKET_LENGTH;
      limitDescription =
          "the default limit of "
              + limit
              + " bytes (a quarter of the JVM max heap, clamped between 16Mb and 1Gb). Set the"
              + " maxAllowedPacket option to change it";
    }

    if (length > limit) {
      throw new R2dbcNonTransientResourceException(
          "Received packet size ("
              + length
              + " bytes) is greater than "
              + limitDescription
              + ". The connection has been closed.",
          "08000");
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

  public void setContext(Context context) {
    this.context = context;
    this.clientCapabilities = this.context.getClientCapabilities();
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

  public boolean isMetaFollows() {
    return metaFollows;
  }

  public void setMetaFollows(boolean metaFollows) {
    this.metaFollows = metaFollows;
  }
}
