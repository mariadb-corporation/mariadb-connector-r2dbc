// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.BindEncodedValue;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Mono;

public final class ExecutePacket implements ClientMessage {
  private final List<BindEncodedValue> bindValues;
  private int statementId;
  private final int parameterCount;
  private final String sql;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private ByteBuf savedBuf = null;

  public ExecutePacket(
      String sql, ServerPrepareResult prepareResult, List<BindEncodedValue> bindValues) {
    this.sql = sql;
    this.bindValues = bindValues;
    this.statementId = prepareResult == null ? -1 : prepareResult.getStatementId();
    this.parameterCount = prepareResult == null ? bindValues.size() : prepareResult.getNumParams();
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    if (savedBuf != null) return savedBuf;
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x17);
    buf.writeIntLE(statementId);
    buf.writeByte(0x00); // NO CURSOR
    buf.writeIntLE(1); // Iteration pos

    // create null bitmap
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        if (bindValues.get(i).getValue() == null) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        buf.writeShortLE(bindValues.get(i).getCodec().getBinaryEncodeType().get());
      }
    }

    for (int i = 0; i < parameterCount; i++) {
      ByteBuf param = bindValues.get(i).getValue();
      if (param != null) {
        buf.writeBytes(param);
      }
    }

    return buf;
  }

  public Mono<ClientMessage> rePrepare(Client client) {
    ServerPrepareResult res;
    if (client.getPrepareCache() != null && (res = client.getPrepareCache().get(sql)) != null) {
      this.forceStatementId(res.getStatementId());
      return Mono.just(this);
    }
    return client
        .sendPrepare(new PreparePacket(sql), ExceptionFactory.INSTANCE, sql)
        .flatMap(
            serverPrepareResult -> {
              this.forceStatementId(serverPrepareResult.getStatementId());
              return Mono.just(this);
            });
  }

  public void save(ByteBuf buf, int initialReaderIndex) {
    savedBuf = buf.readerIndex(initialReaderIndex).retain();
  }

  public void forceStatementId(int statementId) {
    this.statementId = statementId;
    if (savedBuf != null) {
      // replace byte at position 1 with new statement id
      int writerIndex = this.savedBuf.writerIndex();
      this.savedBuf.writerIndex(this.savedBuf.readerIndex() + 1);
      this.savedBuf.writeIntLE(statementId);
      this.savedBuf.writerIndex(writerIndex);
    }
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }

  public void resetSequencer() {
    sequencer.reset();
  }

  public String getSql() {
    return sql;
  }

  @Override
  public void releaseEncodedBinds() {
    bindValues.forEach(
        b -> {
          if (b.getValue() != null) b.getValue().release();
        });
    bindValues.clear();
  }

  @Override
  public String toString() {
    return "ExecutePacket{" + "sql='" + sql + '\'' + '}';
  }
}
