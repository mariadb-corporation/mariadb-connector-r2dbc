// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import reactor.core.publisher.Mono;

public final class RequestExtSaltPacket implements ClientMessage {

  private final MessageSequence sequencer;

  public RequestExtSaltPacket(MessageSequence sequencer) {
    this.sequencer = sequencer;
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    return Mono.just(allocator.ioBuffer(0));
  }

  @Override
  public MessageSequence getSequencer() {
    return sequencer;
  }
}
