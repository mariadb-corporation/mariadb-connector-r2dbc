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
import org.mariadb.r2dbc.message.server.SkipPacket;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.Capabilities;

public enum DecoderState implements DecoderStateInterface {
  INIT_HANDSHAKE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return InitialHandshakePacket.decode(sequencer, body);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return FAST_AUTH_RESPONSE;
    }
  },

  FAST_AUTH_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 0:
          return OK_PACKET;

        case 254: // 0xFE
          return AUTHENTICATION_SWITCH;

        case 255: // 0xFF
          return ERROR;

        default:
          throw new IllegalArgumentException(
              String.format("Error in protocol: %s is not a supported", val));
      }
    }
  },

  OK_PACKET {
    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return OkPacket.decode(sequencer, body, decoder.getContext());
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH {
    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return AuthSwitchPacket.decode(sequencer, body, decoder.getContext());
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return AUTHENTICATION_SWITCH_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 0:
          return OK_PACKET;
        case 1:
          return AUTHENTICATION_MORE_DATA;
        case 254: // 0xFE
          return AUTHENTICATION_SWITCH;
        case 255: // 0xFF
          return ERROR;
        default:
          throw new IllegalArgumentException(
              String.format("Error in protocol: %s is not a supported", val));
      }
    }
  },

  AUTHENTICATION_MORE_DATA {
    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return AuthMoreDataPacket.decode(sequencer, body, decoder.getContext());
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return AUTHENTICATION_SWITCH_RESPONSE;
    }
  },

  QUERY_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 0:
          return OK_PACKET;
        case 255: // 0xFF
          return ERROR;
        default:
          return COLUMN_COUNT;
      }
    }
  },

  COLUMN_COUNT {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      ColumnCountPacket columnCountPacket =
          ColumnCountPacket.decode(sequencer, body, decoder.getContext());
      decoder.setStateCounter(columnCountPacket.getColumnCount());
      return columnCountPacket;
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return COLUMN_DEFINITION;
    }
  },

  COLUMN_DEFINITION {

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.decrementStateCounter();
      return ColumnDefinitionPacket.decode(sequencer, body, decoder.getContext(), false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
          return ROW_RESPONSE;
        } else {
          return EOF_INTERMEDIATE_RESPONSE;
        }
      }
      return this;
    }
  },

  EOF_INTERMEDIATE_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return EofPacket.decode(sequencer, body, decoder.getContext(), false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return ROW_RESPONSE;
    }
  },

  EOF_END {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return EofPacket.decode(sequencer, body, decoder.getContext(), true);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ROW_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 254:
          if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) == 0) {
            return EOF_END;
          } else if (len < 0xffffff) {
            return OK_PACKET;
          } else {
            // normal ROW
            return ROW;
          }
        case 255: // 0xFF
          return ERROR;
        default:
          return ROW;
      }
    }
  },

  ROW {
    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return new RowPacket(body);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return ROW_RESPONSE;
    }
  },

  PREPARE_RESPONSE {
    PrepareResultPacket packet;

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      packet = PrepareResultPacket.decode(sequencer, body, decoder.getContext());
      ServerPrepareResult prepareResult =
          new ServerPrepareResult(
              packet.getStatementId(), packet.getNumColumns(), packet.getNumParams());

      PrepareCache prepareCache = decoder.getClient().getPrepareCache();
      if (prepareCache != null && prepareCache.put(element.getSql(), prepareResult) != null) {
        // race condition, remove new one to get the one in cache
        prepareResult.decrementUse(decoder.getClient());
      }
      return packet;
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        decoder.setStateCounter(packet.getNumParams() + packet.getNumColumns());
      } else {
        decoder.setStateCounter(
            (packet.getNumParams() > 0 ? packet.getNumParams() + 1 : 0)
                + (packet.getNumColumns() > 0 ? packet.getNumColumns() + 1 : 0));
      }
      if (decoder.getStateCounter() > 0) {
        return SKIP;
      }
      throw new IllegalArgumentException("unexpected state");
    }
  },

  PREPARE_AND_EXECUTE_RESPONSE {
    PrepareResultPacket packet;

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {

      packet = PrepareResultPacket.decode(sequencer, body, decoder.getContext());

      ServerPrepareResult prepareResult =
          new ServerPrepareResult(
              packet.getStatementId(), packet.getNumColumns(), packet.getNumParams());

      PrepareCache prepareCache = decoder.getClient().getPrepareCache();
      if (prepareCache != null && prepareCache.put(element.getSql(), prepareResult) != null) {
        // race condition, remove new one to get the one in cache
        prepareResult.decrementUse(decoder.getClient());
      }

      return packet;
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        decoder.setStateCounter(packet.getNumParams() + packet.getNumColumns());
      } else {
        decoder.setStateCounter(
            (packet.getNumParams() > 0 ? packet.getNumParams() + 1 : 0)
                + (packet.getNumColumns() > 0 ? packet.getNumColumns() + 1 : 0));
      }
      if (decoder.getStateCounter() > 0) {
        return SKIP_EXECUTE;
      }
      return QUERY_RESPONSE;
    }
  },

  SKIP {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(decoder.getStateCounter() == 0);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return SKIP;
    }
  },

  SKIP_EXECUTE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        return QUERY_RESPONSE;
      }
      return SKIP_EXECUTE;
    }
  },

  ERROR {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return ErrorPacket.decode(sequencer, body);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      throw new IllegalArgumentException("unexpected state");
    }
  }
}
