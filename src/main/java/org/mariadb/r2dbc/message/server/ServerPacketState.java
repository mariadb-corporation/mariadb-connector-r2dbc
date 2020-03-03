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
import io.netty.buffer.ByteBufUtil;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.util.constants.Capabilities;

public class ServerPacketState {
  public TriFunction<ByteBuf, Sequencer, ConnectionContext, ServerMessage> next;
  private volatile int columnCount = 0;

  public ServerPacketState() {
    next = this::initHandshake;
  }

  public ServerMessage initHandshake(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    next = this::fastAuthResponse;
    return InitialHandshakePacket.decode(sequencer, body);
  }

  public ServerMessage fastAuthResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        return OkPacket.decode(sequencer, body, context);
      case 254: // 0xFE
        next = this::authSwitchResponse;
        return AuthSwitchPacket.decode(sequencer, body, context);
      case 255: // 0xFF
        next = this::queryResponse;
        return ErrorPacket.decode(sequencer, body, context);
    }
    throw new IllegalArgumentException(
        String.format("Error in protocol: %s is not a supported", ByteBufUtil.prettyHexDump(body)));
  }

  public ServerMessage authSwitchResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        return OkPacket.decode(sequencer, body, context);
      case 1:
        next = this::authSwitchResponse;
        return AuthMoreDataPacket.decode(sequencer, body, context);
      case 254: // 0xFE
        next = this::authSwitchResponse;
        return AuthSwitchPacket.decode(sequencer, body, context);
      case 255: // 0xFF
        next = this::queryResponse;
        return ErrorPacket.decode(sequencer, body, context);
    }
    throw new IllegalArgumentException(
        String.format("Error in protocol: %s is not a supported", ByteBufUtil.prettyHexDump(body)));
  }

  public ServerMessage queryResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        return OkPacket.decode(sequencer, body, context);
      case 255:
        next = this::queryResponse;
        return ErrorPacket.decode(sequencer, body, context);
      default:
        next = this::columnDefinitionResponse;
        ColumnCountPacket message = ColumnCountPacket.decode(sequencer, body, context);
        columnCount = message.getColumnCount();
        return message;
    }
  }

  public ServerMessage columnDefinitionResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    ColumnDefinitionPacket column = new ColumnDefinitionPacket(sequencer, body, context);
    columnCount--;
    if (columnCount == 0) {
      if ((context.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        next = this::resultRowResponse;
      } else {
        next = this::eofResponse;
      }
    }
    return column;
  }

  public ServerMessage eofResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    next = this::resultRowResponse;
    return EofPacket.decode(sequencer, body, context, false);
  }

  public ServerMessage resultRowResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 255:
        next = this::queryResponse;
        return ErrorPacket.decode(sequencer, body, context);
      case 254:
        if (body.writerIndex() < 0xffffff) {
          next = this::queryResponse;
          if ((context.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
            return OkPacket.decode(sequencer, body, context);
          } else {
            return EofPacket.decode(sequencer, body, context, true);
          }
        }
        return new RowPacket(body);

      default:
        return new RowPacket(body);
    }
  }

  @FunctionalInterface
  public interface TriFunction<ByteBuf, Sequencer, ConnectionContext, ServerMessage> {
    ServerMessage apply(ByteBuf body, Sequencer sequencer, ConnectionContext context);
  }
}
