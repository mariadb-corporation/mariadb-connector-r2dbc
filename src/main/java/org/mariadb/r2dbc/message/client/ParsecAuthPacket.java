// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import reactor.core.publisher.Mono;

public final class ParsecAuthPacket implements ClientMessage {

  private final MessageSequence sequencer;
  private final byte[] clientScramble;
  private final byte[] signature;

  public ParsecAuthPacket(MessageSequence sequencer, byte[] clientScramble, byte[] signature) {
    this.sequencer = sequencer;
    this.clientScramble = clientScramble;
    this.signature = signature;
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer(256);
    buf.writeBytes(clientScramble);
    buf.writeBytes(signature);
    return Mono.just(buf);
  }

  @Override
  public MessageSequence getSequencer() {
    return sequencer;
  }
}
