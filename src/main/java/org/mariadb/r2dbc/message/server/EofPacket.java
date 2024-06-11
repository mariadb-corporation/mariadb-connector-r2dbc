// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.util.constants.ServerStatus;

public class EofPacket implements ServerMessage {

  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;
  private final boolean resultSetEnd;

  public EofPacket(
      final short serverStatus,
      final short warningCount,
      final boolean resultSetEnd,
      final boolean ending) {
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.resultSetEnd = resultSetEnd;
    this.ending = ending;
  }

  public static EofPacket decode(ByteBuf buf, Context context, boolean resultSetEnd) {
    buf.skipBytes(1);
    short warningCount = buf.readShortLE();
    short serverStatus = buf.readShortLE();
    context.setServerStatus(serverStatus);
    return new EofPacket(
        serverStatus,
        warningCount,
        resultSetEnd,
        resultSetEnd && (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
  }

  /**
   * This is for mysql that doesn't send MORE_RESULTS_EXISTS flag, but sending an OK_Packet after,
   * breaking protocol.
   *
   * @param buf current EOF buf
   * @param context current context
   * @return Eof packet
   */
  public static EofPacket decodeOutputParam(ByteBuf buf, Context context) {
    buf.skipBytes(1);
    short warningCount = buf.readShortLE();
    short serverStatus =
        (short)
            (buf.readShortLE() | ServerStatus.PS_OUT_PARAMETERS | ServerStatus.MORE_RESULTS_EXISTS);
    context.setServerStatus(serverStatus);
    return new EofPacket(serverStatus, warningCount, false, false);
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
    return resultSetEnd;
  }

  @Override
  public String toString() {
    return "EofPacket{"
        + "serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + ", resultSetEnd="
        + resultSetEnd
        + '}';
  }
}
