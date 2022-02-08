// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;

/**
 * see COM_INIT_DB https://mariadb.com/kb/en/com_init_db/ COM_INIT_DB is used to specify the default
 * schema for the connection.
 */
public final class ChangeSchemaPacket implements ClientMessage {
  private final String schema;

  /**
   * Constructor
   *
   * @param schema new default schema
   */
  public ChangeSchemaPacket(String schema) {
    this.schema = schema;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x02);
    buf.writeCharSequence(this.schema, StandardCharsets.UTF_8);
    return buf;
  }
}
