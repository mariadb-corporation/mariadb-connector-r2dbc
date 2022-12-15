// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.*;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import org.mariadb.r2dbc.util.constants.Capabilities;
import org.mariadb.r2dbc.util.constants.ServerStatus;

public enum DecoderState implements DecoderStateInterface {
  INIT_HANDSHAKE {
    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return InitialHandshakePacket.decode(sequencer, body);
    }
  },

  OK_PACKET {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return OkPacket.decode(sequencer, body, decoder.getContext());
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  AUTHENTICATION_SWITCH {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return AuthSwitchPacket.decode(sequencer, body, decoder.getContext());
    }
  },

  AUTHENTICATION_SWITCH_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
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
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return AuthMoreDataPacket.decode(sequencer, body, decoder.getContext());
    }
  },

  QUERY_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
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
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      ColumnCountPacket columnCountPacket =
          ColumnCountPacket.decode(sequencer, body, decoder.getContext());
      decoder.setStateCounter(columnCountPacket.getColumnCount());
      decoder.setMetaFollows(columnCountPacket.isMetaFollows());
      return columnCountPacket;
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.isMetaFollows()) {
        return COLUMN_DEFINITION;
      }
      if ((decoder.getClientCapabilities() & Capabilities.CLIENT_DEPRECATE_EOF) > 0) {
        return ROW_RESPONSE;
      } else {
        return EOF_INTERMEDIATE_RESPONSE;
      }
    }
  },

  COLUMN_DEFINITION {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return ColumnDefinitionPacket.decode(
          sequencer, body, decoder.getContext(), false, decoder.getConf());
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        return EOF_INTERMEDIATE_RESPONSE;
      }
      return this;
    }
  },

  EOF_INTERMEDIATE_RESPONSE {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      EofPacket eof = EofPacket.decode(sequencer, body, decoder.getContext(), false);
      decoder.setStateCounter((eof.getServerStatus() & ServerStatus.PS_OUT_PARAMETERS) > 0 ? 1 : 0);
      return eof;
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      // mysql has a broken protocol for output parameter, then driver need to know state
      if (decoder.getStateCounter() > 0) {
        decoder.setStateCounter(0);
        return ROW_RESPONSE_OUT_PARAM;
      }
      return ROW_RESPONSE;
    }
  },

  EOF_END {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return EofPacket.decode(sequencer, body, decoder.getContext(), true);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  EOF_END_OUT_PARAM {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      // specific for mysql that break protocol, forgetting sometime to set PS_OUT_PARAMETERS and
      // more importantly MORE_RESULTS_EXISTS
      // breaking protocol
      return EofPacket.decodeOutputParam(sequencer, body, decoder.getContext());
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ROW_RESPONSE {
    @Override
    public DecoderState decoder(short val, int len) {
      switch (val) {
        case 254:
          if (len < 0xffffff) {
            return EOF_END;
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

  ROW_RESPONSE_OUT_PARAM {
    @Override
    public DecoderState decoder(short val, int len) {
      switch (val) {
        case 254:
          if (len < 0xffffff) {
            return EOF_END_OUT_PARAM;
          } else {
            // normal ROW
            return ROW_OUTPUT_PARAM;
          }
        case 255: // 0xFF
          return ERROR;
        default:
          return ROW_OUTPUT_PARAM;
      }
    }
  },

  ROW {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return new RowPacket(body);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return ROW_RESPONSE;
    }
  },

  ROW_OUTPUT_PARAM {
    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return new RowPacket(body);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return ROW_RESPONSE_OUT_PARAM;
    }
  },

  PREPARE_RESPONSE {

    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      decoder.setPrepare(PrepareResultPacket.decode(sequencer, body, decoder.getContext(), false));
      if (decoder.getPrepare().getNumParams() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, false);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        if (decoder.getPrepare().getNumColumns() == 0) {
          decoder.setPrepare(null);
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF
      decoder.setStateCounter(decoder.getPrepare().getNumParams());
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_AND_EXECUTE_RESPONSE {

    @Override
    public DecoderState decoder(short val, int len) {
      if (val == 255) { // 0xFF
        return ERROR_AND_EXECUTE_RESPONSE;
      }
      return this;
    }

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      decoder.setPrepare(PrepareResultPacket.decode(sequencer, body, decoder.getContext(), true));
      if (decoder.getPrepare().getNumParams() == 0 && decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(serverPrepareResult, true);
      }
      return new SkipPacket(false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getPrepare().getNumParams() == 0) {
        if (decoder.getPrepare().getNumColumns() == 0) {
          decoder.setPrepare(null);
          return QUERY_RESPONSE;
        }
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      // skip param and EOF
      decoder.setStateCounter(decoder.getPrepare().getNumParams());
      return PREPARE_PARAMETER;
    }
  },

  PREPARE_PARAMETER {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getStateCounter() == 0) {
        return PREPARE_PARAMETER_EOF;
      }
      return this;
    }
  },

  PREPARE_PARAMETER_EOF {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      if (decoder.getPrepare().getNumColumns() == 0) {
        ServerPrepareResult serverPrepareResult = decoder.endPrepare();
        return new CompletePrepareResult(
            serverPrepareResult, decoder.getPrepare().isContinueOnEnd());
      }
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getPrepare().getNumColumns() > 0) {
        decoder.setStateCounter(decoder.getPrepare().getNumColumns());
        return PREPARE_COLUMN;
      }
      decoder.setPrepare(null);
      return QUERY_RESPONSE;
    }
  },

  PREPARE_COLUMN {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      ColumnDefinitionPacket columnDefinitionPacket =
          ColumnDefinitionPacket.decode(
              sequencer, body, decoder.getContext(), false, decoder.getConf());
      decoder
              .getPrepareColumns()[
              decoder.getPrepare().getNumColumns() - decoder.getStateCounter()] =
          columnDefinitionPacket;
      decoder.decrementStateCounter();
      return SkipPacket.decode(false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      if (decoder.getStateCounter() <= 0) {
        return PREPARE_COLUMN_EOF;
      }
      return this;
    }
  },

  PREPARE_COLUMN_EOF {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      boolean continueOnEnd = decoder.getPrepare().isContinueOnEnd();
      ServerPrepareResult prepareResult = decoder.endPrepare();
      return new CompletePrepareResult(prepareResult, continueOnEnd);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return QUERY_RESPONSE;
    }
  },

  ERROR {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return ErrorPacket.decode(sequencer, body, true);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      throw new IllegalArgumentException("unexpected state");
    }
  },

  ERROR_AND_EXECUTE_RESPONSE {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      return ErrorPacket.decode(sequencer, body, false);
    }

    @Override
    public DecoderState next(MariadbFrameDecoder decoder) {
      return SKIP_EXECUTE;
    }
  },

  SKIP_EXECUTE {

    @Override
    public ServerMessage decode(ByteBuf body, Sequencer sequencer, MariadbFrameDecoder decoder) {
      decoder.decrementStateCounter();
      return SkipPacket.decode(true);
    }
  }
}
