// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import reactor.core.publisher.Mono;

/**
 * COM_STMT_CLOSE packet. See
 * https://mariadb.com/kb/en/3-binary-protocol-prepared-statements-com_stmt_close/
 */
public final class ClosePreparePacket implements ClientMessage {

  private final int statementId;

  public ClosePreparePacket(int statementId) {
    this.statementId = statementId;
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    ByteBuf buf = allocator.ioBuffer();
    buf.writeByte(0x19);
    buf.writeIntLE(statementId);
    return Mono.just(buf);
  }
}
