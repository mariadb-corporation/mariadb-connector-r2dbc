// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;
import reactor.core.publisher.Mono;

public final class QueryPacket implements ClientMessage {

  private final String sql;
  private final MessageSequence sequencer = new Sequencer((byte) 0xff);

  public QueryPacket(String sql) {
    this.sql = Assert.requireNonNull(sql, "query must not be null");
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator byteBufAllocator) {
    ByteBuf out = byteBufAllocator.ioBuffer(this.sql.length() + 1);
    out.writeByte(0x03);
    out.writeCharSequence(this.sql, StandardCharsets.UTF_8);
    return Mono.just(out);
  }

  public MessageSequence getSequencer() {
    return sequencer;
  }

  public void resetSequencer() {
    sequencer.reset();
  }

  @Override
  public String toString() {
    return "QueryPacket{" + "sql='" + sql + '\'' + '}';
  }
}
