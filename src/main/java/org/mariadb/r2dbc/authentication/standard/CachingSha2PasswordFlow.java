// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.AuthMoreRawPacket;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.client.Sha256PasswordPacket;
import org.mariadb.r2dbc.message.client.Sha2PublicKeyRequestPacket;
import org.mariadb.r2dbc.message.server.Sequencer;

public final class CachingSha2PasswordFlow extends Sha256PasswordPluginFlow {

  public static final String TYPE = "caching_sha2_password";
  private State state = State.INIT;
  private PublicKey publicKey;

  /**
   * Send a SHA-2 encrypted password. encryption XOR(SHA256(password), SHA256(seed,
   * SHA256(SHA256(password))))
   *
   * @param password password
   * @param seed seed
   * @return encrypted pwd
   */
  public static byte[] sha256encryptPassword(final CharSequence password, final byte[] seed) {

    if (password == null) {
      return new byte[0];
    }
    byte[] truncatedSeed = new byte[seed.length - 1];
    System.arraycopy(seed, 0, truncatedSeed, 0, seed.length - 1);
    try {
      final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
      byte[] bytePwd = password.toString().getBytes(StandardCharsets.UTF_8);

      final byte[] stage1 = messageDigest.digest(bytePwd);
      messageDigest.reset();

      final byte[] stage2 = messageDigest.digest(stage1);
      messageDigest.reset();

      messageDigest.update(stage2);
      messageDigest.update(truncatedSeed);

      final byte[] digest = messageDigest.digest();
      final byte[] returnBytes = new byte[digest.length];
      for (int i = 0; i < digest.length; i++) {
        returnBytes[i] = (byte) (stage1[i] ^ digest[i]);
      }
      return returnBytes;
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Could not use SHA-256, failing", e);
    }
  }

  public CachingSha2PasswordFlow create() {
    return new CachingSha2PasswordFlow();
  }

  public String type() {
    return TYPE;
  }

  public void setStateFastAuth() {
    state = State.FAST_AUTH_RESULT;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData)
      throws R2dbcException {

    if (authMoreData == null) state = State.INIT;

    CharSequence password = configuration.getPassword();
    switch (state) {
      case INIT:
        byte[] fastCryptedPwd = sha256encryptPassword(password, seed);
        state = State.FAST_AUTH_RESULT;
        return new AuthMoreRawPacket(sequencer, fastCryptedPwd);

      case FAST_AUTH_RESULT:
        ByteBuf buf = authMoreData.getBuf();
        if (buf.getByte(buf.readerIndex()) == 0x01) buf.readByte();
        byte fastAuthResult = buf.readByte();
        switch (fastAuthResult) {
          case 3:
            // success authentication
            return null;

          case 4:
            if (configuration.getSslConfig().getSslMode() != SslMode.DISABLE) {
              // send clear password
              state = State.SEND_AUTH;
              return new ClearPasswordPacket(authMoreData.getSequencer(), password);
            }

            // retrieve public key from configuration or from server
            if (configuration.getCachingRsaPublicKey() != null
                && !configuration.getCachingRsaPublicKey().isEmpty()) {
              publicKey = readPublicKeyFromFile(configuration.getCachingRsaPublicKey());
              state = State.SEND_AUTH;

              return new Sha256PasswordPacket(
                  authMoreData.getSequencer(), configuration.getPassword(), seed, publicKey);
            }

            if (!configuration.allowPublicKeyRetrieval()) {
              throw new R2dbcNonTransientResourceException(
                  "RSA public key is not available client side (option serverRsaPublicKeyFile) and"
                      + " option 'allowPublicKeyRetrieval' is disabled. Either set one or the"
                      + " other",
                  "S1009");
            }

            state = State.REQUEST_SERVER_KEY;
            // ask public Key Retrieval
            return new Sha2PublicKeyRequestPacket(authMoreData.getSequencer());

          default:
            throw new R2dbcNonTransientResourceException(
                "Protocol exchange error. Expect login success or RSA login request message",
                "S1009");
        }

      case REQUEST_SERVER_KEY:
        publicKey = readPublicKey(authMoreData.getBuf());
        state = State.SEND_AUTH;
        return new Sha256PasswordPacket(
            authMoreData.getSequencer(), configuration.getPassword(), seed, publicKey);

      default:
        throw new R2dbcNonTransientResourceException("Wrong state", "S1009");
    }
  }

  public enum State {
    INIT,
    FAST_AUTH_RESULT,
    REQUEST_SERVER_KEY,
    SEND_AUTH,
  }
}
