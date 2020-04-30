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
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.util.Assert;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class ErrorPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ErrorPacket.class);
  private final short errorCode;
  private final String message;
  private final String sqlState;
  private Sequencer sequencer;

  private ErrorPacket(Sequencer sequencer, short errorCode, String sqlState, String message) {
    this.sequencer = sequencer;
    this.errorCode = errorCode;
    this.message = message;
    this.sqlState = sqlState;
  }

  public static ErrorPacket decode(Sequencer sequencer, ByteBuf buf, ConnectionContext context) {
    Assert.requireNonNull(buf, "buffer must not be null");
    buf.skipBytes(1);
    short errorCode = buf.readShortLE();
    byte next = buf.getByte(buf.readerIndex());
    String sqlState;
    String msg;
    if (next == (byte) '#') {
      buf.skipBytes(1); // skip '#'
      sqlState = buf.readCharSequence(5, StandardCharsets.UTF_8).toString();
      msg = buf.toString(StandardCharsets.UTF_8);
    } else {
      msg = buf.toString(StandardCharsets.UTF_8);
      sqlState = "HY000";
    }
    ErrorPacket err = new ErrorPacket(sequencer, errorCode, sqlState, msg);
    logger.warn("Error: {}", err.toString());
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

  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public boolean ending() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ErrorPacket that = (ErrorPacket) o;
    return errorCode == that.errorCode
        && sequencer.equals(that.sequencer)
        && message.equals(that.message)
        && Objects.equals(sqlState, that.sqlState);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sequencer, errorCode, message, sqlState);
  }

  @Override
  public String toString() {
    return "ErrorPacket{"
        + "errorCode="
        + errorCode
        + ", message='"
        + message
        + '\''
        + ", sqlState='"
        + sqlState
        + '\''
        + ", sequencer="
        + sequencer
        + '}';
  }
}
