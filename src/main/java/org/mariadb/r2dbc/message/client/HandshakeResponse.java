// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.mariadb.r2dbc.MariadbConnectionFactoryProvider;
import org.mariadb.r2dbc.authentication.addon.ClearPasswordPluginFlow;
import org.mariadb.r2dbc.authentication.standard.CachingSha2PasswordFlow;
import org.mariadb.r2dbc.authentication.standard.NativePasswordPluginFlow;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.HostAddress;
import org.mariadb.r2dbc.util.constants.Capabilities;
import reactor.core.publisher.Mono;

public final class HandshakeResponse implements ClientMessage {

  private final InitialHandshakePacket initialHandshakePacket;
  private final String username;
  private final CharSequence password;
  private final String database;
  private final Map<String, String> connectionAttributes;
  private final HostAddress hostAddress;
  private final long clientCapabilities;

  public HandshakeResponse(
      InitialHandshakePacket initialHandshakePacket,
      String username,
      CharSequence password,
      String database,
      Map<String, String> connectionAttributes,
      HostAddress hostAddress,
      long clientCapabilities) {
    this.initialHandshakePacket = initialHandshakePacket;
    this.username = username;
    this.password = password;
    this.database = database;
    this.connectionAttributes = connectionAttributes;
    this.hostAddress = hostAddress;
    this.clientCapabilities = clientCapabilities;
  }

  /**
   * Default collation used for string exchanges with server. Always return 4 bytes utf8 collation
   * for server that permit it.
   *
   * @param serverLanguage server default collation
   * @param majorVersion server major version
   * @param minorVersion server minor version
   * @return collation byte
   */
  public static byte decideLanguage(short serverLanguage, int majorVersion, int minorVersion) {
    // return current server utf8mb4 collation
    if (serverLanguage == 45 // utf8mb4_general_ci
        || serverLanguage == 46 // utf8mb4_bin
        || (serverLanguage >= 224 && serverLanguage <= 247)) {
      return (byte) serverLanguage;
    }
    return (byte) ((majorVersion == 5 && minorVersion <= 1) ? 33 : 224);
  }

  @Override
  public Mono<ByteBuf> encode(Context context, ByteBufAllocator allocator) {

    byte exchangeCharset =
        decideLanguage(
            initialHandshakePacket.getDefaultCollation(),
            initialHandshakePacket.getMajorServerVersion(),
            initialHandshakePacket.getMinorServerVersion());

    ByteBuf buf = allocator.buffer(4096);

    final byte[] authData;
    String authenticationPluginType = initialHandshakePacket.getAuthenticationPluginType();
    switch (authenticationPluginType) {
      case ClearPasswordPluginFlow.TYPE:
        // TODO check that SSL is enable
        authData =
            (password == null) ? new byte[0] : password.toString().getBytes(StandardCharsets.UTF_8);
        break;
      case CachingSha2PasswordFlow.TYPE:
        authenticationPluginType = CachingSha2PasswordFlow.TYPE;
        authData =
            (password == null)
                ? new byte[0]
                : CachingSha2PasswordFlow.sha256encryptPassword(
                    password, initialHandshakePacket.getSeed());
        break;

      default:
        authenticationPluginType = NativePasswordPluginFlow.TYPE;
        authData = NativePasswordPacket.encrypt(password, initialHandshakePacket.getSeed());
        break;
    }

    buf.writeIntLE((int) clientCapabilities);
    buf.writeIntLE(1024 * 1024 * 1024);
    buf.writeByte(exchangeCharset); // 1

    buf.writeZero(19); // 19
    buf.writeIntLE((int) (clientCapabilities >> 32)); // Maria extended flag

    // to permit SSO
    buf.writeCharSequence(
        (username != null && !username.isEmpty()) ? username : System.getProperty("user.name"),
        StandardCharsets.UTF_8);
    buf.writeZero(1);

    if ((initialHandshakePacket.getCapabilities() & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA)
        != 0) {
      buf.writeBytes(BufferUtils.encodeLength(authData.length));
      buf.writeBytes(authData);
    } else if ((initialHandshakePacket.getCapabilities() & Capabilities.SECURE_CONNECTION) != 0) {
      buf.writeByte((byte) authData.length);
      buf.writeBytes(authData);
    } else {
      buf.writeBytes(authData);
      buf.writeZero(1);
    }

    if ((clientCapabilities & Capabilities.CONNECT_WITH_DB) != 0) {
      buf.writeCharSequence(database, StandardCharsets.UTF_8);
      buf.writeZero(1);
    }

    if ((initialHandshakePacket.getCapabilities() & Capabilities.PLUGIN_AUTH) != 0) {
      buf.writeCharSequence(authenticationPluginType, StandardCharsets.UTF_8);
      buf.writeZero(1);
    }

    if ((initialHandshakePacket.getCapabilities() & Capabilities.CONNECT_ATTRS) != 0) {
      ByteBuf bufAttributes = allocator.buffer(2048);
      writeConnectAttributes(bufAttributes, connectionAttributes, hostAddress);
      buf.writeBytes(BufferUtils.encodeLength(bufAttributes.writerIndex()));
      buf.writeBytes(bufAttributes, 0, bufAttributes.writerIndex());
      bufAttributes.release();
    }

    return Mono.just(buf);
  }

  @Override
  public Sequencer getSequencer() {
    return initialHandshakePacket.getSequencer();
  }

  private void writeConnectAttributes(
      ByteBuf buf, Map<String, String> connectionAttributes, HostAddress hostAddress) {
    BufferUtils.writeLengthEncode("_client_name", buf);
    BufferUtils.writeLengthEncode(MariadbConnectionFactoryProvider.MARIADB_DRIVER, buf);

    BufferUtils.writeLengthEncode("_server_host", buf);
    BufferUtils.writeLengthEncode(hostAddress != null ? hostAddress.getHost() : "", buf);

    BufferUtils.writeLengthEncode("_os", buf);
    BufferUtils.writeLengthEncode(System.getProperty("os.name"), buf);

    BufferUtils.writeLengthEncode("_thread", buf);
    BufferUtils.writeLengthEncode(Long.toString(Thread.currentThread().getId()), buf);

    BufferUtils.writeLengthEncode("_java_vendor", buf);
    BufferUtils.writeLengthEncode(System.getProperty("java.vendor"), buf);

    BufferUtils.writeLengthEncode("_java_version", buf);
    BufferUtils.writeLengthEncode(System.getProperty("java.version"), buf);

    if (connectionAttributes != null && !connectionAttributes.isEmpty()) {
      connectionAttributes.forEach(
          (key, val) -> {
            BufferUtils.writeLengthEncode(key, buf);
            BufferUtils.writeLengthEncode(val, buf);
          });
    }
  }
}
