// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import org.mariadb.r2dbc.util.constants.StateChange;
import reactor.util.Logger;
import reactor.util.Loggers;

public class OkPacket implements ServerMessage, Result.UpdateCount {
  public static final byte TYPE = (byte) 0x00;
  private static final Logger logger = Loggers.getLogger(OkPacket.class);
  private final long affectedRows;
  private final long lastInsertId;
  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;

  public OkPacket(
      long affectedRows,
      long lastInsertId,
      short serverStatus,
      short warningCount,
      final boolean ending) {
    this.affectedRows = affectedRows;
    this.lastInsertId = lastInsertId;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.ending = ending;
  }

  public static OkPacket decode(ByteBuf buf, Context context) {
    buf.skipBytes(1);
    long affectedRows = BufferUtils.readLengthEncodedInt(buf);
    long lastInsertId = BufferUtils.readLengthEncodedInt(buf);
    short serverStatus = buf.readShortLE();
    short warningCount = buf.readShortLE();
    context.setServerStatus(serverStatus);

    if ((context.getClientCapabilities() & Capabilities.CLIENT_SESSION_TRACK) != 0
        && buf.isReadable()) {
      BufferUtils.skipLengthEncode(buf); // skip info
      while (buf.isReadable()) {
        ByteBuf stateInfo = BufferUtils.readLengthEncodedBuffer(buf);
        while (stateInfo.isReadable()) {
          switch (stateInfo.readByte()) {
            case StateChange.SESSION_TRACK_SYSTEM_VARIABLES:
              ByteBuf sessionVariableBuf;
              do {
                sessionVariableBuf = BufferUtils.readLengthEncodedBuffer(stateInfo);
                String variable = BufferUtils.readLengthEncodedString(sessionVariableBuf);
                String value = BufferUtils.readLengthEncodedString(sessionVariableBuf);
                logger.debug("System variable change :  {} = {}", variable, value);

                switch (variable) {
                  case "transaction_isolation":
                  case "tx_isolation":
                    switch (value) {
                      case "REPEATABLE-READ":
                        context.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
                        break;

                      case "READ-UNCOMMITTED":
                        context.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
                        break;

                      case "SERIALIZABLE":
                        context.setIsolationLevel(IsolationLevel.SERIALIZABLE);
                        break;

                      default:
                        context.setIsolationLevel(IsolationLevel.READ_COMMITTED);
                        break;
                    }
                    break;

                  case "redirect_url":
                    context.setRedirect(value.isEmpty() ? null : value);
                    break;
                }
              } while (sessionVariableBuf.readableBytes() > 0);
              break;

            case StateChange.SESSION_TRACK_SCHEMA:
              ByteBuf sessionSchemaBuf = BufferUtils.readLengthEncodedBuffer(stateInfo);
              String schema = BufferUtils.readLengthEncodedString(sessionSchemaBuf);
              context.setDatabase(schema);
              logger.debug("Schema change : now is '{}'", schema);
              break;

            default:
              stateInfo.skipBytes((int) BufferUtils.readLengthEncodedInt(stateInfo));
              break;
          }
        }
      }
    }
    return new OkPacket(
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
