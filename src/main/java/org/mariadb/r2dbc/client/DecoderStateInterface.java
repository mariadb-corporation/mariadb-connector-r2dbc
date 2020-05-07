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
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.message.server.ServerMessage;

public interface DecoderStateInterface {

  default DecoderState decoder(short val, int len, long serverCapabilities) {
    throw new IllegalArgumentException("unexpected state");
  }

  default ServerMessage decode(
      ByteBuf body, Sequencer sequencer, MariadbPacketDecoder decoder, CmdElement element) {
    throw new IllegalArgumentException("unexpected state");
  }

  default DecoderState next(MariadbPacketDecoder decoder) {
    throw new IllegalArgumentException("unexpected state");
  }
}
