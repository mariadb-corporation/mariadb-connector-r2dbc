// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Result;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.mariadb.r2dbc.util.constants.StateChange;
import reactor.util.Logger;
import reactor.util.Loggers;

public class OkPacket implements ServerMessage, Result.UpdateCount {
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

  public static OkPacket decode(Sequencer sequencer, ByteBuf buf, Context context) {
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
              // context.setDatabase(database);
              logger.debug("Database change : now is '{}'", database);
              break;
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

  public long getLastInsertId() {
    return lastInsertId;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public short getWarningCount() {
    return warningCount;
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
  public long value() {
    return affectedRows;
  }

  @Override
  public String toString() {
    return "OkPacket{"
        + "affectedRows="
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
