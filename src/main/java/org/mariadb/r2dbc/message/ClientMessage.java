// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.server.Sequencer;

public interface ClientMessage {
  default MessageSequence getSequencer() {
    return new Sequencer((byte) 0xff);
  }

  default void releaseEncodedBinds() {}

  ByteBuf encode(Context context, ByteBufAllocator byteBufAllocator);
}
