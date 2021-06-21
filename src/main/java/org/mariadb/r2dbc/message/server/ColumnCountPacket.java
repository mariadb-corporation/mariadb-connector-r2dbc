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
