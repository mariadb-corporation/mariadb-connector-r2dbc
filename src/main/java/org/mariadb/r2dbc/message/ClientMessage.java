// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.server.Sequencer;
import reactor.core.publisher.Mono;

public interface ClientMessage {
  default MessageSequence getSequencer() {
    return new Sequencer((byte) 0xff);
  }

  Mono<ByteBuf> encode(Context context, ByteBufAllocator byteBufAllocator);

  default void save(ByteBuf buf, int initialReaderIndex) {}

  default void releaseSave() {}

  default void resetSequencer() {}
}
