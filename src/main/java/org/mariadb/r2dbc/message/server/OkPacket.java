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
import java.util.Objects;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.mariadb.r2dbc.util.constants.StateChange;
import reactor.util.Logger;
import reactor.util.Loggers;

public class OkPacket implements ServerMessage {
  public static final byte TYPE = (byte) 0x00;
  private static final Logger logger = Loggers.getLogger(OkPacket.class);
  private final Sequencer sequencer;
  private final long affectedRows;
  private final long lastInsertId;
  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;

  public OkPacket(
      Sequencer sequencer,
      long affectedRows,
      long lastInsertId,
      short serverStatus,
      short warningCount,
      final boolean ending) {
    this.sequencer = sequencer;
    this.affectedRows = affectedRows;
    this.lastInsertId = lastInsertId;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.ending = ending;
  }

  public static OkPacket decode(Sequencer sequencer, ByteBuf buf, ConnectionContext context) {
    buf.skipBytes(1);
    long affectedRows = BufferUtils.readLengthEncodedInt(buf);
    long lastInsertId = BufferUtils.readLengthEncodedInt(buf);
    short serverStatus = buf.readShortLE();
    short warningCount = buf.readShortLE();

    if ((context.getServerCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0
        && buf.isReadable()) {
      BufferUtils.skipLengthEncode(buf); // skip info
      while (buf.isReadable()) {
        ByteBuf stateInfo = BufferUtils.readLengthEncodedBuffer(buf);
        if (stateInfo.isReadable()) {
          switch (stateInfo.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              ByteBuf sessionVariableBuf = BufferUtils.readLengthEncodedBuffer(stateInfo);
              String variable = BufferUtils.readLengthEncodedString(sessionVariableBuf);
              String value = BufferUtils.readLengthEncodedString(sessionVariableBuf);
              logger.debug("System variable change :  {} = {}", variable, value);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              ByteBuf sessionSchemaBuf = BufferUtils.readLengthEncodedBuffer(stateInfo);
              String database = BufferUtils.readLengthEncodedString(sessionSchemaBuf);
              context.setDatabase(database);
              logger.debug("Database change : now is '{}'", database);
              break;

            default:
              int len = (int) BufferUtils.readLengthEncodedInt(stateInfo);
              stateInfo.skipBytes(len);
          }
        }
      }
    }
    context.setServerStatus(serverStatus);
    return new OkPacket(
        sequencer,
        affectedRows,
        lastInsertId,
        serverStatus,
        warningCount,
        (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
  }

  public long getAffectedRows() {
    return affectedRows;
  }

  public long getLastInsertId() {
    return lastInsertId;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public short getWarningCount() {
    return warningCount;
  }

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }

  @Override
  public boolean resultSetEnd() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OkPacket okPacket = (OkPacket) o;
    return affectedRows == okPacket.affectedRows
        && lastInsertId == okPacket.lastInsertId
        && serverStatus == okPacket.serverStatus
        && warningCount == okPacket.warningCount
        && sequencer.equals(okPacket.sequencer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sequencer, affectedRows, lastInsertId, serverStatus, warningCount);
  }

  @Override
  public String toString() {
    return "OkPacket{"
        + "sequencer="
        + sequencer
        + ", affectedRows="
        + affectedRows
        + ", lastInsertId="
        + lastInsertId
        + ", serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + '}';
  }
}
