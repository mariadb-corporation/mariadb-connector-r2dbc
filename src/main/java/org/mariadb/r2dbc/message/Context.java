// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.IsolationLevel;
import org.mariadb.r2dbc.client.ServerVersion;

public interface Context {

  long getThreadId();

  long getServerCapabilities();

  long getClientCapabilities();

  short getServerStatus();

  void setServerStatus(short serverStatus);

  IsolationLevel getIsolationLevel();

  void setIsolationLevel(IsolationLevel isolationLevel);

  void setRedirect(String redirectValue);

  String getRedirectValue();

  String getDatabase();

  void setDatabase(String database);

  ServerVersion getVersion();

  ByteBufAllocator getByteBufAllocator();

  /**
   * Indicate server charset change. After {@link #setInitialized()} has been called, only utf8 /
   * utf8mb3 / utf8mb4 charsets are accepted; any other value will close the connection and throw an
   * exception so that subsequent operations can't run against a session whose
   * character_set_client is out of sync with the driver's UTF-8 assumption.
   *
   * @param charset new server character_set_client value
   */
  void setCharset(String charset);

  /**
   * Mark the context as fully initialized so that any further server-side charset change to a
   * non-utf8 charset is rejected (see {@link #setCharset(String)}).
   */
  void setInitialized();

  default void saveRedo(ClientMessage msg, ByteBuf buf, int initialReaderIndex) {}
}
