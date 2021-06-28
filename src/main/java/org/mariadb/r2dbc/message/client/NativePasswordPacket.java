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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class NativePasswordPacket implements ClientMessage {

  private Sequencer sequencer;
  private CharSequence password;
  private byte[] seed;

  public NativePasswordPacket(Sequencer sequencer, CharSequence password, byte[] seed) {
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
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return allocator.ioBuffer(0);
    ByteBuf buf = allocator.ioBuffer(32);
    buf.writeBytes(encrypt(password, seed));
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }
}
