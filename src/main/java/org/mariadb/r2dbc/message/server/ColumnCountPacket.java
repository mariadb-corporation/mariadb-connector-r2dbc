// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.Capabilities;

public class ColumnCountPacket implements ServerMessage {

  private int columnCount;
  private boolean metaFollows;

  public ColumnCountPacket(int columnCount, boolean metaFollows) {
    this.columnCount = columnCount;
    this.metaFollows = metaFollows;
  }

  public static ColumnCountPacket decode(Sequencer sequencer, ByteBuf buf, Context context) {
    long columnCount = BufferUtils.readLengthEncodedInt(buf);
    if ((context.getServerCapabilities() & Capabilities.MARIADB_CLIENT_CACHE_METADATA) > 0) {
      int metaFollow = buf.readByte();
      return new ColumnCountPacket((int) columnCount, metaFollow == 1);
    }
    return new ColumnCountPacket((int) columnCount, true);
  }

  public int getColumnCount() {
    return columnCount;
  }

  public boolean isMetaFollows() {
    return metaFollows;
  }
}
