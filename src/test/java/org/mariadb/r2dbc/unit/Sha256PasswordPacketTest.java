// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2026 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import javax.crypto.Cipher;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.message.client.Sha256PasswordPacket;

/**
 * Verifies the password obfuscation used by the sha256/caching_sha2 RSA full-authentication path,
 * without needing a database. The server recovers the password by XOR-ing the decrypted bytes
 * against the 20-byte authentication scramble cycled over its own length; the client must use the
 * same 20-byte cycle. Regression test for GH issue #84, where a trailing null terminator on the
 * auth-switch scramble made the client cycle over 21 bytes and corrupted long passwords.
 */
public class Sha256PasswordPacketTest {

  /** Decrypt with the private key and undo the XOR exactly as the MySQL server does. */
  private static byte[] serverRecover(byte[] cipherText, KeyPair kp, byte[] scramble20)
      throws Exception {
    Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
    cipher.init(Cipher.DECRYPT_MODE, kp.getPrivate());
    byte[] xored = cipher.doFinal(cipherText);
    byte[] out = new byte[xored.length];
    for (int i = 0; i < xored.length; i++) {
      out[i] = (byte) (xored[i] ^ scramble20[i % scramble20.length]); // server cycles % 20
    }
    return out;
  }

  private static byte[] nullTerminated(String password) {
    byte[] pwd = password.getBytes(StandardCharsets.UTF_8);
    return Arrays.copyOf(pwd, pwd.length + 1);
  }

  private void assertRoundTrips(String password) throws Exception {
    KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
    gen.initialize(2048);
    KeyPair kp = gen.generateKeyPair();

    // 20-byte scramble, deliberately non-zero so a stray trailing null is unambiguous
    byte[] scramble20 = new byte[20];
    for (int i = 0; i < 20; i++) scramble20[i] = (byte) (i + 1);

    // auth-switch delivers the scramble null-terminated (21 bytes) ...
    byte[] seedWithNull = Arrays.copyOf(scramble20, 21); // trailing 0x00
    byte[] enc1 = Sha256PasswordPacket.encrypt(kp.getPublic(), password, seedWithNull);
    assertArrayEquals(
        nullTerminated(password), serverRecover(enc1, kp, scramble20), "auth-switch seed (21B)");

    // ... while the initial handshake delivers it raw (20 bytes)
    byte[] enc2 = Sha256PasswordPacket.encrypt(kp.getPublic(), password, scramble20);
    assertArrayEquals(
        nullTerminated(password), serverRecover(enc2, kp, scramble20), "handshake seed (20B)");
  }

  @Test
  void shortPasswordRoundTrips() throws Exception {
    assertRoundTrips("short");
  }

  @Test
  void longPasswordRoundTrips() throws Exception {
    // 25 chars > 20-byte scramble: the case that failed in issue #84
    assertRoundTrips("MySuperPasswordIsVeryLong");
  }

  @Test
  void passwordSpanningSeveralScrambleCyclesRoundTrips() throws Exception {
    assertRoundTrips("0123456789012345678901234567890123456789012345"); // 47 chars
  }
}
