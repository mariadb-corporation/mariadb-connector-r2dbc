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
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import org.mariadb.r2dbc.MariadbConnectionFactoryProvider;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.message.flow.ClearPasswordPluginFlow;
import org.mariadb.r2dbc.message.flow.NativePasswordPluginFlow;
import org.mariadb.r2dbc.message.server.InitialHandshakePacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.PidFactory;
import org.mariadb.r2dbc.util.constants.Capabilities;

public final class HandshakeResponse implements ClientMessage {

  private InitialHandshakePacket initialHandshakePacket;
  private String username;
  private CharSequence password;
  private String database;
  private Map<String, String> connectionAttributes;
  private String host;
  private long clientCapabilities;

  public HandshakeResponse(
      InitialHandshakePacket initialHandshakePacket,
      String username,
      CharSequence password,
      String database,
      Map<String, String> connectionAttributes,
      String host,
      long clientCapabilities) {
    this.initialHandshakePacket = initialHandshakePacket;
    this.username = username;
    this.password = password;
    this.database = database;
    this.connectionAttributes = connectionAttributes;
    this.host = host;
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
    if (majorVersion == 5 && minorVersion <= 1) {
      // 5.1 version doesn't know 4 bytes utf8
      return (byte) 33; // utf8_general_ci
    }
    return (byte) 224; // UTF8MB4_UNICODE_CI;
  }

  @Override
  public ByteBuf encode(Context context, ByteBufAllocator allocator) {

    byte exchangeCharset =
        decideLanguage(
            initialHandshakePacket.getDefaultCollation(),
            initialHandshakePacket.getMajorServerVersion(),
            initialHandshakePacket.getMinorServerVersion());

    ByteBuf buf = allocator.ioBuffer(4096);

    final byte[] authData;
    String authenticationPluginType = initialHandshakePacket.getAuthenticationPluginType();
    switch (authenticationPluginType) {
      case ClearPasswordPluginFlow.TYPE:
        // TODO check that SSL is enable
        if (password == null) {
          authData = new byte[0];
        } else {
          authData = password.toString().getBytes(StandardCharsets.UTF_8);
        }
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

    if (username != null && !username.isEmpty()) {
      buf.writeCharSequence(username, StandardCharsets.UTF_8);
    } else {
      // to permit SSO
      buf.writeCharSequence(System.getProperty("user.name"), StandardCharsets.UTF_8);
    }
    buf.writeZero(1);

    if ((initialHandshakePacket.getCapabilities() & Capabilities.PLUGIN_AUTH_LENENC_CLIENT_DATA)
        != 0) {
      BufferUtils.writeLengthEncode(authData.length, buf);
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
      writeConnectAttributes(bufAttributes, connectionAttributes, host);
      BufferUtils.writeLengthEncode(bufAttributes.writerIndex(), buf);
      buf.writeBytes(bufAttributes, 0, bufAttributes.writerIndex());
      bufAttributes.release();
    }

    return buf;
  }

  @Override
  public Sequencer getSequencer() {
    return initialHandshakePacket.getSequencer();
  }

  private void writeConnectAttributes(
      ByteBuf buf, Map<String, String> connectionAttributes, String host) {
    BufferUtils.writeLengthEncode("_client_name", buf);
    BufferUtils.writeLengthEncode(MariadbConnectionFactoryProvider.MARIADB_DRIVER, buf);

    final Properties properties = new Properties();
    try (InputStream inputStream =
        getClass().getClassLoader().getResourceAsStream("project" + ".properties")) {
      properties.load(inputStream);

      BufferUtils.writeLengthEncode("_client_version", buf);
      BufferUtils.writeLengthEncode(properties.getProperty("version"), buf);
    } catch (IOException ie) {
      // eat
    }

    BufferUtils.writeLengthEncode("_server_host", buf);
    BufferUtils.writeLengthEncode(host != null ? host : "", buf);

    BufferUtils.writeLengthEncode("_os", buf);
    BufferUtils.writeLengthEncode(System.getProperty("os.name"), buf);

    final Supplier<String> pidRequest = PidFactory.getInstance();
    String pid = pidRequest.get();
    if (pid != null) {
      BufferUtils.writeLengthEncode("_pid", buf);
      BufferUtils.writeLengthEncode(pid, buf);
    }

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
