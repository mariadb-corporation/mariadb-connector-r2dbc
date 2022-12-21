// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import reactor.core.publisher.Mono;

public final class NativePasswordPacket implements ClientMessage {

  private final MessageSequence sequencer;
  private final CharSequence password;
  private final byte[] seed;

  public NativePasswordPacket(MessageSequence sequencer, CharSequence password, byte[] seed) {
    this.sequencer = sequencer;
    this.password = password;
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);

    this.seed = truncatedSeed;
  }

  public static byte[] encrypt(CharSequence authenticationData, byte[] seed) {
    if (authenticationData == null) {
      return new byte[0];
    }

    try {
      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
      byte[] bytePwd = authenticationData.toString().getBytes(StandardCharsets.UTF_8);

      final byte[] stage1 = messageDigest.digest(bytePwd);
      messageDigest.reset();
      final byte[] stage2 = messageDigest.digest(stage1);
      messageDigest.reset();
      messageDigest.update(seed);
      messageDigest.update(stage2);

      final byte[] digest = messageDigest.digest();
      final byte[] returnBytes = new byte[digest.length];
      for (int i = 0; i < digest.length; i++) {
        returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
      }
      return returnBytes;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-1, failing", e);
    }
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return Mono.just(allocator.ioBuffer(0));
    ByteBuf buf = allocator.ioBuffer(32);
    buf.writeBytes(encrypt(password, seed));
    return Mono.just(buf);
  }

  @Override
  public MessageSequence getSequencer() {
    return sequencer;
  }
}
