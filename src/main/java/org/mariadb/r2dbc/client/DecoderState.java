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
import org.mariadb.r2dbc.util.constants.Capabilities;

public enum DecoderState implements DecoderStateInterface {
  INIT_HANDSHAKE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return InitialHandshakePacket.decode(sequencer, body);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return OkPacket.decode(sequencer, body, context);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      return QUERY_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH {
    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return AuthSwitchPacket.decode(sequencer, body, context);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return AuthMoreDataPacket.decode(sequencer, body, context);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      ColumnCountPacket columnCountPacket = ColumnCountPacket.decode(sequencer, body, context);
      stateCounter[0] = columnCountPacket.getColumnCount();
      return columnCountPacket;
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      return COLUMN_DEFINITION;
    }
  },

  COLUMN_DEFINITION {

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      stateCounter[0]--;
      return ColumnDefinitionPacket.decode(sequencer, body, context, false);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      if (stateCounter[0] <= 0) {
        if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return EofPacket.decode(sequencer, body, context, false);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      return ROW_RESPONSE;
    }
  },

  EOF_END {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return EofPacket.decode(sequencer, body, context, true);
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return new RowPacket(body);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
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
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      packet = PrepareResultPacket.decode(sequencer, body, context);
      return packet;
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      if (packet.getNumParams() > 0) {
        DecoderState nextState = PREPARE_PARAM_DEFINITION;
        stateCounter[0] = packet.getNumParams();
        stateCounter[1] = packet.getNumColumns();
        return nextState;
      } else {
        if (packet.getNumColumns() > 0) {
          DecoderState nextState = PREPARE_COLUMN_DEFINITION;
          stateCounter[1] = packet.getNumColumns();
          return nextState;
        }
        throw new IllegalArgumentException("unexpected state");
      }
    }
  },

  PREPARE_PARAM_DEFINITION {

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      stateCounter[0]--;
      return ColumnDefinitionPacket.decode(
          sequencer,
          body,
          context,
          stateCounter[0] == 0
              && stateCounter[1] == 0
                  & (serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      if (stateCounter[0] == 0) {
        if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
          return PREPARE_COLUMN_DEFINITION;
        } else if (stateCounter[1] == 0) {
          return EOF_END;
        } else {
          return PREPARE_EOF_PARAM_END;
        }
      }
      return this;
    }
  },

  PREPARE_COLUMN_DEFINITION {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      stateCounter[1]--;
      return ColumnDefinitionPacket.decode(
          sequencer,
          body,
          context,
          stateCounter[1] == 0 && (serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) > 0);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      if (stateCounter[1] == 0) {
        if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) == 0) {
          return EOF_END;
        }
        throw new IllegalArgumentException("unexpected state");
      }
      return this;
    }
  },

  PREPARE_EOF_PARAM_END {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return EofPacket.decode(sequencer, body, context, stateCounter[1] == 0);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      return PREPARE_COLUMN_DEFINITION;
    }
  },

  ERROR {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      return this;
    }

    @Override
    public ServerMessage decode(
        ByteBuf body,
        Sequencer sequencer,
        ConnectionContext context,
        long serverCapabilities,
        int[] stateCounter) {
      return ErrorPacket.decode(sequencer, body, context);
    }

    @Override
    public DecoderState next(long serverCapabilities, int[] stateCounter) {
      throw new IllegalArgumentException("unexpected state");
    }
  };
}
