// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.Sequencer;

public interface DecoderStateInterface {

  //  default DecoderState decoder(short val, int len, long serverCapabilities) {
  //    throw new IllegalArgumentException("unexpected state");
  //  }

  default DecoderState decoder(short val, int len) {
    return (DecoderState) this;
  }

  default ServerMessage decode(
      ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
    throw new IllegalArgumentException("unexpected state");
  }

  default DecoderState next(MariadbPacketDecoder decoder) {
    throw new IllegalArgumentException("unexpected state");
  }
}
