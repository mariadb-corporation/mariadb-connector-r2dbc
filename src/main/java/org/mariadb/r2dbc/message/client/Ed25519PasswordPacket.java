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
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import org.mariadb.r2dbc.authentication.ed25519.math.GroupElement;
import org.mariadb.r2dbc.authentication.ed25519.math.ed25519.ScalarOps;
import org.mariadb.r2dbc.authentication.ed25519.spec.EdDSANamedCurveTable;
import org.mariadb.r2dbc.authentication.ed25519.spec.EdDSAParameterSpec;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class Ed25519PasswordPacket implements ClientMessage {

  private Sequencer sequencer;
  private CharSequence password;
  private byte[] seed;

  public Ed25519PasswordPacket(Sequencer sequencer, CharSequence password, byte[] seed) {
    this.sequencer = sequencer;
    this.password = password;
    this.seed = seed;
  }

  private static byte[] ed25519SignWithPassword(final CharSequence password, final byte[] seed)
      throws R2dbcNonTransientResourceException {

    try {
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

      MessageDigest hash = MessageDigest.getInstance("SHA-512");

      int mlen = seed.length;
      final byte[] sm = new byte[64 + mlen];

      byte[] az = hash.digest(bytePwd);
      az[0] &= 248;
      az[31] &= 63;
      az[31] |= 64;

      System.arraycopy(seed, 0, sm, 64, mlen);
      System.arraycopy(az, 32, sm, 32, 32);

      byte[] buff = Arrays.copyOfRange(sm, 32, 96);
      hash.reset();
      byte[] nonce = hash.digest(buff);

      ScalarOps scalar = new ScalarOps();

      EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName("Ed25519");
      GroupElement elementAvalue = spec.getB().scalarMultiply(az);
      byte[] elementAarray = elementAvalue.toByteArray();
      System.arraycopy(elementAarray, 0, sm, 32, elementAarray.length);

      nonce = scalar.reduce(nonce);
      GroupElement elementRvalue = spec.getB().scalarMultiply(nonce);
      byte[] elementRarray = elementRvalue.toByteArray();
      System.arraycopy(elementRarray, 0, sm, 0, elementRarray.length);

      hash.reset();
      byte[] hram = hash.digest(sm);
      hram = scalar.reduce(hram);
      byte[] tt = scalar.multiplyAndAdd(hram, az, nonce);
      System.arraycopy(tt, 0, sm, 32, tt.length);

      return Arrays.copyOfRange(sm, 0, 64);

    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-512, failing", e);
    }
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {
    if (password == null) return allocator.ioBuffer(0);
    ByteBuf buf = allocator.ioBuffer(64);
    buf.writeBytes(ed25519SignWithPassword(password, seed));
    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return sequencer;
  }
}
