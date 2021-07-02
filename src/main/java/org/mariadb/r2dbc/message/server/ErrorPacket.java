// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class ErrorPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ErrorPacket.class);
  private final short errorCode;
  private final String message;
  private final String sqlState;
  private Sequencer sequencer;
  private final boolean ending;

  private ErrorPacket(
      Sequencer sequencer, short errorCode, String sqlState, String message, boolean ending) {
    this.sequencer = sequencer;
    this.errorCode = errorCode;
    this.message = message;
    this.sqlState = sqlState;
    this.ending = ending;
  }

  public static ErrorPacket decode(Sequencer sequencer, ByteBuf buf, boolean ending) {
    Assert.requireNonNull(buf, "buffer must not be null");
    buf.skipBytes(1);
    short errorCode = buf.readShortLE();
    byte next = buf.getByte(buf.readerIndex());
    String sqlState;
    String msg;
    if (next == (byte) '#') {
      buf.skipBytes(1); // skip '#'
      sqlState = buf.readCharSequence(5, StandardCharsets.UTF_8).toString();
      msg = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8).toString();
    } else {
      // Pre-4.1 message, still can be output in newer versions (e.g with 'Too many connections')
      msg = buf.readCharSequence(buf.readableBytes(), StandardCharsets.UTF_8).toString();
      sqlState = "HY000";
    }
    ErrorPacket err = new ErrorPacket(sequencer, errorCode, sqlState, msg, ending);
    logger.warn("Error: '{}' sqlState='{}' code={} ", msg, sqlState, errorCode);
    return err;
  }

  public short getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return message;
  }

  public String getSqlState() {
    return sqlState;
  }

  @Override
  public boolean ending() {
    return ending;
  }
}
