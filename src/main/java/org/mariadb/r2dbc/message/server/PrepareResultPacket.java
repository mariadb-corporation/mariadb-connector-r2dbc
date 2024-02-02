// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;

public final class PrepareResultPacket implements ServerMessage {

  private final int statementId;
  private final int numColumns;
  private final int numParams;
  private final Sequencer sequencer;
  private final boolean continueOnEnd;

  private PrepareResultPacket(
      final Sequencer sequencer,
      final int statementId,
      final int numColumns,
      final int numParams,
      boolean continueOnEnd) {
    this.sequencer = sequencer;
    this.statementId = statementId;
    this.numColumns = numColumns;
    this.numParams = numParams;
    this.continueOnEnd = continueOnEnd;
  }

  public static PrepareResultPacket decode(
      Sequencer sequencer, ByteBuf buffer, Context context, boolean continueOnEnd) {
    /* Prepared Statement OK */
    buffer.readByte(); /* skip field count */
    final int statementId = buffer.readIntLE();
    final int numColumns = buffer.readUnsignedShortLE();
    final int numParams = buffer.readUnsignedShortLE();
    return new PrepareResultPacket(sequencer, statementId, numColumns, numParams, continueOnEnd);
  }

  @Override
  public boolean ending() {
    return false;
  }

  public boolean isContinueOnEnd() {
    return continueOnEnd;
  }

  public int getStatementId() {
    return statementId;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumParams() {
    return numParams;
  }
}
