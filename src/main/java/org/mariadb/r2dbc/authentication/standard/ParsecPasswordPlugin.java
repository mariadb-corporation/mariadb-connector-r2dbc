// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.ParsecAuthPacket;
import org.mariadb.r2dbc.message.client.RequestExtSaltPacket;
import org.mariadb.r2dbc.message.server.Sequencer;

/** Parsec password plugin */
public class ParsecPasswordPlugin implements AuthenticationPlugin {

  private static byte[] pkcs8Ed25519header =
      new byte[] {
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20
      };

  public ParsecPasswordPlugin create() {
    return new ParsecPasswordPlugin();
  }

  @Override
  public String type() {
    return "parsec";
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData) {

    // request ext-salt from server
    if (authMoreData == null) {
      return new RequestExtSaltPacket(sequencer);
    }

    byte firstByte = 0;
    int iterations = 100;
    ByteBuf buf = authMoreData.getBuf();

    if (buf.getByte(buf.readerIndex()) == 0x01) buf.readByte();

    if (buf.readableBytes() > 2) {
      firstByte = buf.readByte();
      iterations = buf.readByte();
    }

    if (firstByte != 0x50 || iterations > 3) {
      // expected 'P' for KDF algorithm (PBKDF2) and maximum iteration of 8192
      throw new R2dbcNonTransientResourceException("Wrong parsec authentication format", "S1009");
    }

    byte[] salt = new byte[buf.readableBytes()];
    buf.readBytes(salt);
    char[] password =
        configuration.getPassword() == null
            ? new char[0]
            : configuration.getPassword().toString().toCharArray();
    ;

    KeyFactory ed25519KeyFactory;
    Signature ed25519Signature;

    try {
      // in case using java 15+
      ed25519KeyFactory = KeyFactory.getInstance("Ed25519");
      ed25519Signature = Signature.getInstance("Ed25519");
    } catch (NoSuchAlgorithmException e) {
      try {
        // java before 15, try using BouncyCastle if present
        ed25519KeyFactory = KeyFactory.getInstance("Ed25519", "BC");
        ed25519Signature = Signature.getInstance("Ed25519", "BC");
      } catch (NoSuchAlgorithmException | NoSuchProviderException ee) {
        throw new R2dbcNonTransientResourceException(
            "Parsec authentication not available. Either use Java 15+ or add BouncyCastle"
                + " dependency",
            e);
      }
    }

    try {
      // hash password with PBKDF2
      PBEKeySpec spec = new PBEKeySpec(password, salt, 1024 << iterations, 256);
      SecretKey key = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512").generateSecret(spec);
      byte[] derivedKey = key.getEncoded();

      // create a PKCS8 ED25519 private key with raw secret
      PKCS8EncodedKeySpec keySpec =
          new PKCS8EncodedKeySpec(combineArray(pkcs8Ed25519header, derivedKey));
      PrivateKey privateKey = ed25519KeyFactory.generatePrivate(keySpec);

      // generate client nonce
      byte[] clientScramble = new byte[32];
      SecureRandom.getInstanceStrong().nextBytes(clientScramble);

      // sign concatenation of server nonce + client nonce with private key

      ed25519Signature.initSign(privateKey);
      ed25519Signature.update(combineArray(seed, clientScramble));
      byte[] signature = ed25519Signature.sign();
      return new ParsecAuthPacket(sequencer, clientScramble, signature);

    } catch (NoSuchAlgorithmException
        | InvalidKeySpecException
        | InvalidKeyException
        | SignatureException e) {
      // not expected
      throw new R2dbcNonTransientResourceException("Error during parsec authentication", e);
    }
  }

  private byte[] combineArray(byte[] arr1, byte[] arr2) {
    byte[] combined = new byte[arr1.length + arr2.length];
    System.arraycopy(arr1, 0, combined, 0, arr1.length);
    System.arraycopy(arr2, 0, combined, arr1.length, arr2.length);
    return combined;
  }
}
