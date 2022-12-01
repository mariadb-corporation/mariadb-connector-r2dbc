// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication.standard;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.R2dbcException;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.SslMode;
import org.mariadb.r2dbc.authentication.AuthenticationPlugin;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.client.RsaPublicKeyRequestPacket;
import org.mariadb.r2dbc.message.client.Sha256PasswordPacket;
import org.mariadb.r2dbc.message.server.Sequencer;

public class Sha256PasswordPluginFlow implements AuthenticationPlugin {

  public static final String TYPE = "sha256_password";
  private State state = State.INIT;
  private PublicKey publicKey;

  /**
   * Read public Key from file.
   *
   * @param serverRsaPublicKeyFile RSA public key file
   * @return public key
   * @throws R2dbcException if cannot read file or file content is not a public key.
   */
  public static PublicKey readPublicKeyFromFile(String serverRsaPublicKeyFile)
      throws R2dbcException {
    byte[] keyBytes;
    try {
      keyBytes = Files.readAllBytes(Paths.get(serverRsaPublicKeyFile));
    } catch (IOException ex) {
      throw new R2dbcNonTransientResourceException(
          "Could not read server RSA public key from file : "
              + "serverRsaPublicKeyFile="
              + serverRsaPublicKeyFile,
          "S1009",
          ex);
    }
    return generatePublicKey(keyBytes);
  }

  /**
   * Read public pem key from String.
   *
   * @param publicKeyBytes public key bytes value
   * @return public key
   * @throws R2dbcException if key cannot be parsed
   */
  public static PublicKey generatePublicKey(byte[] publicKeyBytes) throws R2dbcException {
    try {
      String publicKey =
          new String(publicKeyBytes)
              .replaceAll("(-+BEGIN PUBLIC KEY-+\\r?\\n|\\n?-+END PUBLIC KEY-+\\r?\\n?)", "");
      byte[] keyBytes = Base64.getMimeDecoder().decode(publicKey);
      X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
      KeyFactory kf = KeyFactory.getInstance("RSA");
      return kf.generatePublic(spec);
    } catch (Exception ex) {
      throw new R2dbcNonTransientResourceException(
          "Could read server RSA public key: " + ex.getMessage(), "S1009", ex);
    }
  }

  /**
   * Read public Key
   *
   * @param buf more data buffer
   * @return public key
   * @throws R2dbcException if server return an Error packet or public key cannot be parsed.
   */
  public static PublicKey readPublicKey(ByteBuf buf) throws R2dbcException {
    byte[] key = new byte[buf.readableBytes()];
    buf.readBytes(key);
    return generatePublicKey(key);
  }

  public Sha256PasswordPluginFlow create() {
    return new Sha256PasswordPluginFlow();
  }

  public String type() {
    return TYPE;
  }

  public ClientMessage next(
      MariadbConnectionConfiguration configuration,
      byte[] seed,
      Sequencer sequencer,
      AuthMoreData authMoreData)
      throws R2dbcException {

    if (state == State.INIT) {
      CharSequence password = configuration.getPassword();
      if (password == null || configuration.getSslConfig().getSslMode() != SslMode.DISABLE) {
        return new ClearPasswordPacket(sequencer, password);
      } else {
        // retrieve public key from configuration or from server
        if (configuration.getRsaPublicKey() != null && !configuration.getRsaPublicKey().isEmpty()) {
          publicKey = readPublicKeyFromFile(configuration.getRsaPublicKey());
        } else {
          if (!configuration.allowPublicKeyRetrieval()) {
            throw new R2dbcNonTransientResourceException(
                "RSA public key is not available client side (option " + "serverRsaPublicKeyFile)",
                "S1009");
          }
          state = State.REQUEST_SERVER_KEY;
          // ask public Key Retrieval
          return new RsaPublicKeyRequestPacket(sequencer);
        }
      }
      return new Sha256PasswordPacket(sequencer, configuration.getPassword(), seed, publicKey);
    } else {
      publicKey = readPublicKey(authMoreData.getBuf());
      return new Sha256PasswordPacket(
          authMoreData.getSequencer(), configuration.getPassword(), seed, publicKey);
    }
  }

  public enum State {
    INIT,
    REQUEST_SERVER_KEY,
    SEND_AUTH,
  }
}
