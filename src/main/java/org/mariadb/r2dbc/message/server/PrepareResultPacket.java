// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.util.constants.Capabilities;

public final class PrepareResultPacket implements ServerMessage {

  private final int statementId;
  private final int numColumns;
  private final int numParams;
  private final boolean eofDeprecated;
  private final Sequencer sequencer;
  private final boolean continueOnEnd;

  private PrepareResultPacket(
      final Sequencer sequencer,
      final int statementId,
      final int numColumns,
      final int numParams,
      final boolean eofDeprecated,
      boolean continueOnEnd) {
    this.sequencer = sequencer;
    this.statementId = statementId;
    this.numColumns = numColumns;
    this.numParams = numParams;
    this.eofDeprecated = eofDeprecated;
    this.continueOnEnd = continueOnEnd;
  }

  @Override
  public boolean ending() {
    return continueOnEnd && numParams == 0 && numColumns == 0 && eofDeprecated;
  }

  public boolean isContinueOnEnd() {
    return continueOnEnd;
  }

  public static PrepareResultPacket decode(
      Sequencer sequencer, ByteBuf buffer, Context context, boolean continueOnEnd) {
    /* Prepared Statement OK */
    buffer.readByte(); /* skip field count */
    final int statementId = buffer.readIntLE();
    final int numColumns = buffer.readUnsignedShortLE();
    final int numParams = buffer.readUnsignedShortLE();
    return new PrepareResultPacket(
        sequencer,
        statementId,
        numColumns,
        numParams,
        ((context.getClientCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0),
        continueOnEnd);
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

  public boolean isEofDeprecated() {
    return eofDeprecated;
  }
}
