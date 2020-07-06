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

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcPermissionDeniedException;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.util.Arrays;
import javax.crypto.Cipher;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class Sha256PasswordPacket implements ClientMessage {

  private Sequencer sequencer;
  private CharSequence password;
  private byte[] seed;
  private PublicKey publicKey;

  public Sha256PasswordPacket(
      Sequencer sequencer, CharSequence password, byte[] seed, PublicKey publicKey) {
    this.sequencer = sequencer;
    this.password = password;
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);
    this.seed = truncatedSeed;
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
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return allocator.ioBuffer(0);
    ByteBuf buf = allocator.ioBuffer(256);
    buf.writeBytes(encrypt(publicKey, password, seed));
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }

  @Override
  public String toString() {
    return "Sha256PasswordPacket{"
        + "sequencer="
        + sequencer
        + ", password=*******"
        + ", seed="
        + Arrays.toString(seed)
        + '}';
  }
}
