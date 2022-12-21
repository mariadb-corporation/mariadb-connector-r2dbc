// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.BindValue;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ExecutePacket implements ClientMessage {
  private final BindValue[] bindValues;
  private int statementId;
  private final int parameterCount;
  private final String sql;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);
  private ByteBuf savedBuf = null;

  public ExecutePacket(String sql, ServerPrepareResult prepareResult, BindValue[] bindValues) {
    this.sql = sql;
    this.bindValues = bindValues;
    this.statementId = prepareResult == null ? -1 : prepareResult.getStatementId();
    this.parameterCount = prepareResult == null ? bindValues.length : prepareResult.getNumParams();
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    if (savedBuf != null) {
      ByteBuf tmp = savedBuf;
      this.savedBuf = null;
      return Mono.just(tmp);
    }
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x17);
    buf.writeIntLE(statementId);
    buf.writeByte(0x00); // NO CURSOR
    buf.writeIntLE(1); // Iteration pos

    // create null bitmap
    boolean direct = true;
    if (parameterCount > 0) {
      int nullCount = (parameterCount + 7) / 8;

      byte[] nullBitsBuffer = new byte[nullCount];
      for (int i = 0; i < parameterCount; i++) {
        BindValue param = bindValues[i];
        if (param.isNull()) {
          nullBitsBuffer[i / 8] |= (1 << (i % 8));
        }
        if (!param.getCodec().isDirect()) {
          direct = false;
        }
      }
      buf.writeBytes(nullBitsBuffer);

      buf.writeByte(0x01); // Send Parameter type flag
      // Store types of parameters in first package that is sent to the server.
      for (int i = 0; i < parameterCount; i++) {
        buf.writeShortLE(bindValues[i].getCodec().getBinaryEncodeType().get());
      }
    }

    if (direct) {
      for (int i = 0; i < parameterCount; i++) {
        if (!bindValues[i].isNull()) bindValues[i].encodeDirectBinary(allocator, buf, context);
      }
      return Mono.just(buf);
    } else {
      return Flux.range(0, parameterCount)
          .flatMap(
              i -> {
                BindValue param = bindValues[i];
                if (param.getValue() != null) {
                  if (param.getCodec().isDirect()) {
                    param.encodeDirectBinary(allocator, buf, context);
                  } else {
                    return param
                        .encodeBinary(allocator)
                        .map(
                            b -> {
                              buf.writeBytes(b);
                              b.release();
                              return Mono.empty();
                            });
                  }
                }
                return Mono.empty();
              })
          .then(Mono.just(buf));
    }
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

  public void releaseSave() {
    if (savedBuf != null) {
      savedBuf.release();
      savedBuf = null;
    }
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
  public String toString() {
    return "ExecutePacket{" + "sql='" + sql + '\'' + '}';
  }
}
