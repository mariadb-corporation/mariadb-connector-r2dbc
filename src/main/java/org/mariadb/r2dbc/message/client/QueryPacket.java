// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.Assert;

public final class QueryPacket implements ClientMessage {

  private final String sql;
  private final Sequencer sequencer = new Sequencer((byte) 0xff);

  public QueryPacket(String sql) {
    this.sql = Assert.requireNonNull(sql, "query must not be null");
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator) {
    Assert.requireNonNull(byteBufAllocator, "byteBufAllocator must not be null");
    ByteBuf out = byteBufAllocator.ioBuffer(this.sql.length() + 1);
    out.writeByte(0x03);
    out.writeCharSequence(this.sql, StandardCharsets.UTF_8);
    return out;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }
}
