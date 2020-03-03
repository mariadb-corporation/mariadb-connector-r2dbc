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
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.util.constants.ServerStatus;

import java.util.Objects;

public class EofPacket implements ServerMessage {

  private final Sequencer sequencer;
  private final short serverStatus;
  private final short warningCount;
  private final boolean ending;
  private final boolean resultSetEnd;

  public EofPacket(
      final Sequencer sequencer,
      final short serverStatus,
      final short warningCount,
      final boolean resultSetEnd,
      final boolean ending) {
    this.sequencer = sequencer;
    this.serverStatus = serverStatus;
    this.warningCount = warningCount;
    this.resultSetEnd = resultSetEnd;
    this.ending = ending;
  }

  public static EofPacket decode(
      Sequencer sequencer, ByteBuf buf, ConnectionContext context, boolean resultSetEnd) {
    buf.skipBytes(1);
    short warningCount = buf.readShortLE();
    short serverStatus = buf.readShortLE();
    context.setServerStatus(serverStatus);
    return new EofPacket(
        sequencer,
        serverStatus,
        warningCount,
        resultSetEnd,
        resultSetEnd && (serverStatus & ServerStatus.MORE_RESULTS_EXISTS) == 0);
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
    return resultSetEnd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EofPacket okPacket = (EofPacket) o;
    return serverStatus == okPacket.serverStatus
        && warningCount == okPacket.warningCount
        && sequencer.equals(okPacket.sequencer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sequencer, serverStatus, warningCount);
  }

  @Override
  public String toString() {
    return "EofPacket{"
        + "sequencer="
        + sequencer
        + ", serverStatus="
        + serverStatus
        + ", warningCount="
        + warningCount
        + ", ending="
        + ending
        + '}';
  }
}
