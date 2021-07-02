// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.Capabilities;

public enum DecoderState implements DecoderStateInterface {
  INIT_HANDSHAKE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 255: // 0xFF
          return ERROR;
        default:
          return this;
      }
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return InitialHandshakePacket.decode(sequencer, body);
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
  },

  AUTHENTICATION_SWITCH_RESPONSE {
    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 1:
          return AUTHENTICATION_MORE_DATA;
        case 254: // 0xFE
          return AUTHENTICATION_SWITCH;
        case 255: // 0xFF
          return ERROR;
        default:
          return OK_PACKET;
      }
    }
  },

  AUTHENTICATION_MORE_DATA {
    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return AuthMoreDataPacket.decode(sequencer, body, decoder.getContext());
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
    ColumnCountPacket columnCountPacket;

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      columnCountPacket = ColumnCountPacket.decode(sequencer, body, decoder.getContext());
      decoder.setStateCounter(columnCountPacket.getColumnCount());
      return columnCountPacket;
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (columnCountPacket.isMetaFollows()) return COLUMN_DEFINITION;
      if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        return ROW_RESPONSE;
      } else {
        return EOF_INTERMEDIATE_RESPONSE;
      }
    }
  },

  COLUMN_DEFINITION {

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
          if ((serverCapabilities & Capabilities.CLIENT_DEPRECATE_EOF) == 0 && len < 0xffffff) {
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

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 255: // 0xFF
          return ERROR;
        default:
          return this;
      }
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      PrepareResultPacket packet =
          PrepareResultPacket.decode(sequencer, body, decoder.getContext(), false);
      decoder.setPrepare(packet);
      if (packet.getNumParams() == 0 && packet.getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, false);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        // if next, then columns > 0
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF if needed
      decoder.setStateCounter(
          decoder.getPrepare().getNumParams()
              + (((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0)
                  ? 0
                  : 1));
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_AND_EXECUTE_RESPONSE {

    public DecoderState decoder(short val, int len, long serverCapabilities) {
      switch (val) {
        case 255: // 0xFF
          return ERROR_AND_EXECUTE_RESPONSE;
        default:
          return this;
      }
    }

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.setPrepare(PrepareResultPacket.decode(sequencer, body, decoder.getContext(), true));
      if (decoder.getPrepare().getNumParams() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, true);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        if (decoder.getPrepare().getNumColumns() == 0) {
          decoder.setPrepare(null);
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF if needed
      decoder.setStateCounter(
          decoder.getPrepare().getNumParams()
              + (((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0)
                  ? 0
                  : 1));
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_PARAMETER {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.decrementStateCounter();
      if (decoder.getStateCounter() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        // end parameter without columns
        boolean ending = !decoder.getPrepare().isContinueOnEnd();
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, ending);
      }
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        if (decoder.getPrepare() == null) {
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_COLUMN {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      ColumnDefinitionPacket columnDefinitionPacket =
          ColumnDefinitionPacket.decode(sequencer, body, decoder.getContext(), false);
      decoder
              .getPrepareColumns()[
              decoder.getPrepare().getNumColumns() - decoder.getStateCounter()] =
          columnDefinitionPacket;
      decoder.decrementStateCounter();

      if (decoder.getStateCounter() <= 0) {
        if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
          boolean ending = !decoder.getPrepare().isContinueOnEnd();
          ServerPrepareResult prepareResult = decoder.endPrepare();
          return new CompletePrepareResult(prepareResult, ending);
        }
      }

      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        if ((decoder.getServerCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
          return QUERY_RESPONSE;
        }
        return PREPARE_COLUMN_EOF;
      }
      return this;
    }
  },

  PREPARE_COLUMN_EOF {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      boolean ending = !decoder.getPrepare().isContinueOnEnd();
      ServerPrepareResult prepareResult = decoder.endPrepare();
      return new CompletePrepareResult(prepareResult, ending);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ERROR {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return ErrorPacket.decode(sequencer, body, true);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      throw new IllegalArgumentException("unexpected state");
    }
  },

  ERROR_AND_EXECUTE_RESPONSE {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      return ErrorPacket.decode(sequencer, body, false);
    }

    @Override
    public DecoderState next(MariadbPacketDecoder decoder) {
      return SKIP_EXECUTE;
    }
  },

  SKIP_EXECUTE {

    @Override
    public ServerMessage decode(
        ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(true);
    }
  }
}
