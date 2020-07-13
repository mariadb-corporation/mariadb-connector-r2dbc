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

package org.mariadb.r2dbc.message.flow;

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
import org.mariadb.r2dbc.message.client.ClearPasswordPacket;
import org.mariadb.r2dbc.message.client.ClientMessage;
import org.mariadb.r2dbc.message.client.RsaPublicKeyRequestPacket;
import org.mariadb.r2dbc.message.client.Sha256PasswordPacket;
import org.mariadb.r2dbc.message.server.AuthMoreDataPacket;
import org.mariadb.r2dbc.message.server.AuthSwitchPacket;

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
   * @param authMoreDataPacket More data packet
   * @return public key
   * @throws R2dbcException if server return an Error packet or public key cannot be parsed.
   */
  public static PublicKey readPublicKey(AuthMoreDataPacket authMoreDataPacket)
      throws R2dbcException {
    ByteBuf buf = authMoreDataPacket.getBuf();
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
      AuthSwitchPacket authSwitchPacket,
      AuthMoreDataPacket authMoreDataPacket)
      throws R2dbcException {

    switch (state) {
      case INIT:
        CharSequence password = configuration.getPassword();
        if (password == null
            || password.toString().isEmpty()
            || configuration.getSslConfig().getSslMode() != SslMode.DISABLED) {
          return new ClearPasswordPacket(authSwitchPacket.getSequencer(), password);
        } else {
          // retrieve public key from configuration or from server
          if (configuration.getRsaPublicKey() != null
              && !configuration.getRsaPublicKey().isEmpty()) {
            publicKey = readPublicKeyFromFile(configuration.getRsaPublicKey());
          } else {
            if (!configuration.allowPublicKeyRetrieval()) {
              throw new R2dbcNonTransientResourceException(
                  "RSA public key is not available client side (option "
                      + "serverRsaPublicKeyFile)",
                  "S1009");
            }
            state = State.REQUEST_SERVER_KEY;
            // ask public Key Retrieval
            return new RsaPublicKeyRequestPacket(authSwitchPacket.getSequencer());
          }
        }
        return new Sha256PasswordPacket(
            authSwitchPacket.getSequencer(),
            configuration.getPassword(),
            authSwitchPacket.getSeed(),
            publicKey);

      case REQUEST_SERVER_KEY:
        publicKey = readPublicKey(authMoreDataPacket);
        return new Sha256PasswordPacket(
            authMoreDataPacket.getSequencer(),
            configuration.getPassword(),
            authSwitchPacket.getSeed(),
            publicKey);

      default:
        throw new R2dbcNonTransientResourceException("Wrong state", "S1009");
    }
  }

  public enum State {
    INIT,
    REQUEST_SERVER_KEY,
    SEND_AUTH,
  }
}
