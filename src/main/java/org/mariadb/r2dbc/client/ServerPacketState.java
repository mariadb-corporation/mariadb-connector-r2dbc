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

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.util.Queue;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;
import org.mariadb.r2dbc.message.server.ColumnCountPacket;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.message.server.EofPacket;
import org.mariadb.r2dbc.message.server.ErrorPacket;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.OkPacket;
import org.mariadb.r2dbc.message.server.PrepareResultPacket;
import org.mariadb.r2dbc.message.server.RowPacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.message.server.ServerMessage;
import org.mariadb.r2dbc.util.constants.Capabilities;

public class ServerPacketState {

  public Queue<CommandResponse> responseReceivers;
  public Client client;
  public CommandResponse response;
  public TriFunction<ByteBuf, Sequencer, ConnectionContext> next;
  private volatile PrepareResultPacket prepareResult = null;
  private volatile int counter = 0;

  public ServerPacketState(Queue<CommandResponse> responseReceivers, Client client) {
    this.responseReceivers = responseReceivers;
    this.client = client;
    next = this::initHandshake; // response.getNextState();
  }

  public boolean loadNextResponse() {
    response = responseReceivers.poll();
    if (response == null) return false;
    next = response.getNextState();
    return true;
  }

  public void handle(ServerMessage msg) {
    response.getSink().next(msg);
    if (msg.ending()) {
      response.getSink().complete();
      client.sendNext();
      prepareResult = null;
      loadNextResponse();
    }
  }

  public void initHandshake(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    next = this::fastAuthResponse;
    handle(InitialHandshakePacket.decode(sequencer, body));
  }

  public void fastAuthResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        handle(OkPacket.decode(sequencer, body, context));
        break;

      case 254: // 0xFE
        next = this::authSwitchResponse;
        handle(AuthSwitchPacket.decode(sequencer, body, context));
        break;

      case 255: // 0xFF
        next = this::queryResponse;
        handle(ErrorPacket.decode(sequencer, body, context));
        break;

      default:
        throw new IllegalArgumentException(
            String.format(
                "Error in protocol: %s is not a supported", ByteBufUtil.prettyHexDump(body)));
    }
  }

  public void authSwitchResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        handle(OkPacket.decode(sequencer, body, context));
        break;

      case 1:
        next = this::authSwitchResponse;
        handle(AuthMoreDataPacket.decode(sequencer, body, context));
        break;

      case 254: // 0xFE
        next = this::authSwitchResponse;
        handle(AuthSwitchPacket.decode(sequencer, body, context));
        break;

      case 255: // 0xFF
        handle(ErrorPacket.decode(sequencer, body, context));
        break;

      default:
        throw new IllegalArgumentException(
            String.format(
                "Error in protocol: %s is not a supported", ByteBufUtil.prettyHexDump(body)));
    }
  }

  public void queryResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 0:
        next = this::queryResponse;
        handle(OkPacket.decode(sequencer, body, context));
        break;

      case 255:
        next = this::queryResponse;
        handle(ErrorPacket.decode(sequencer, body, context));
        break;

      default:
        ColumnCountPacket message = ColumnCountPacket.decode(sequencer, body, context);
        counter = message.getColumnCount();
        next = this::columnDefinitionResponse;
        handle(message);
    }
  }

  public void prepareResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    PrepareResultPacket prepareResult = PrepareResultPacket.decode(sequencer, body, context);
    if (prepareResult.getNumParams() > 0) {
      this.prepareResult = prepareResult;
      counter = prepareResult.getNumParams();
      next = this::prepareParamResponse;
    } else {
      if (prepareResult.getNumColumns() > 0) {
        this.prepareResult = prepareResult;
        counter = prepareResult.getNumColumns();
        next = this::prepareColumnResponse;
      } else {
        next = this::queryResponse;
      }
    }
    handle(prepareResult);
  }

  public void prepareParamResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    counter--;
    ColumnDefinitionPacket param =
        ColumnDefinitionPacket.decode(
            sequencer,
            body,
            context,
            prepareResult.isEofDeprecated() && counter == 0 && prepareResult.getNumColumns() == 0);
    if (counter == 0) {
      counter = prepareResult.getNumColumns();
      next =
          prepareResult.isEofDeprecated()
              ? this::prepareColumnResponse
              : this::eofPrepareParamResponse;
    }
    handle(param);
  }

  public void prepareColumnResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    counter--;
    ColumnDefinitionPacket column =
        ColumnDefinitionPacket.decode(
            sequencer, body, context, prepareResult.isEofDeprecated() && counter == 0);

    if (counter == 0) {
      next = this::eofPrepareColumnResponse;
    }
    handle(column);
  }

  public void columnDefinitionResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    ColumnDefinitionPacket column = ColumnDefinitionPacket.decode(sequencer, body, context, false);
    counter--;
    if (counter == 0) {
      if ((context.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        next = this::resultRowResponse;
      } else {
        next = this::eofResponse;
      }
    }
    handle(column);
  }

  public void eofPrepareParamResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {

    if (prepareResult.getNumColumns() == 0) {
      handle(EofPacket.decode(sequencer, body, context, true));
      return;
    }
    handle(EofPacket.decode(sequencer, body, context, false));
    next = this::prepareColumnResponse;
  }

  public void eofPrepareColumnResponse(
      ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    handle(EofPacket.decode(sequencer, body, context, true));
  }

  public void eofResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    next = this::resultRowResponse;
    handle(EofPacket.decode(sequencer, body, context, false));
  }

  public void resultRowResponse(ByteBuf body, Sequencer sequencer, ConnectionContext context) {
    switch (body.getUnsignedByte(body.readerIndex())) {
      case 255:
        next = this::queryResponse;
        handle(ErrorPacket.decode(sequencer, body, context));
        break;

      case 254:
        if (body.writerIndex() < 0xffffff) {
          next = this::queryResponse;
          if ((context.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
            handle(OkPacket.decode(sequencer, body, context));
          } else {
            handle(EofPacket.decode(sequencer, body, context, true));
          }
          break;
        }
        handle(new RowPacket(body));
        break;

      default:
        handle(new RowPacket(body));
        break;
    }
  }

  @FunctionalInterface
  public interface TriFunction<ByteBuf, Sequencer, ConnectionContext> {
    void apply(ByteBuf body, Sequencer sequencer, ConnectionContext context);
  }
}
