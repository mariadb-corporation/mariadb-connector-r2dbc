// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Arrays;
import javax.crypto.Cipher;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.MessageSequence;
import reactor.core.publisher.Mono;

public final class Sha256PasswordPacket implements ClientMessage {

  private final MessageSequence sequencer;
  private final CharSequence password;
  private final byte[] seed;
  private final PublicKey publicKey;

  public Sha256PasswordPacket(
      MessageSequence sequencer, CharSequence password, byte[] seed, PublicKey publicKey) {
    this.sequencer = sequencer;
    this.password = password;
    this.seed = seed;
    this.publicKey = publicKey;
  }

  /**
   * Encode password with seed and public key.
   *
   * @param publicKey public key
   * @param password password
   * @param seed seed
   * @return encoded password
   * @throws R2dbcException if cannot encode password
   */
  public static byte[] encrypt(PublicKey publicKey, CharSequence password, byte[] seed)
      throws R2dbcException {

    byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

    byte[] nullFinishedPwd = Arrays.copyOf(bytePwd, bytePwd.length + 1);
    byte[] xorBytes = new byte[nullFinishedPwd.length];
    int seedLength = seed.length;

    for (int i = 0; i < xorBytes.length; i++) {
      xorBytes[i] = (byte) (nullFinishedPwd[i] ^ seed[i % seedLength]);
    }

    try {
      Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
      cipher.init(Cipher.ENCRYPT_MODE, publicKey);
      return cipher.doFinal(xorBytes);
    } catch (Exception ex) {
      throw new R2dbcPermissionDeniedException(
          "Could not connect using SHA256 plugin : " + ex.getMessage(), "S1009", ex);
    }
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return Mono.just(allocator.ioBuffer(0));
    ByteBuf buf = allocator.ioBuffer(256);
    buf.writeBytes(encrypt(publicKey, password, seed));
    return Mono.just(buf);
  }

  @Override
  public MessageSequence getSequencer() {
    return sequencer;
  }
}
