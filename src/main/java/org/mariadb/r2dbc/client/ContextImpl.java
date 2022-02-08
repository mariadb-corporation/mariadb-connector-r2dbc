// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.r2dbc.spi.IsolationLevel;
import org.mariadb.r2dbc.message.Context;

public class ContextImpl implements Context {

  private final long threadId;
  private final long serverCapabilities;
  private final long clientCapabilities;
  private short serverStatus;
  private ServerVersion version;

  private IsolationLevel isolationLevel;
  private String database;

  public ContextImpl(
      String serverVersion,
      long threadId,
      long capabilities,
      short serverStatus,
      boolean mariaDBServer,
      long clientCapabilities,
      String database,
      IsolationLevel isolationLevel) {

    this.threadId = threadId;
    this.serverCapabilities = capabilities;
    this.clientCapabilities = clientCapabilities;
    this.serverStatus = serverStatus;
    this.version = new ServerVersion(serverVersion, mariaDBServer);
    this.isolationLevel = isolationLevel == null ? IsolationLevel.REPEATABLE_READ : isolationLevel;
    this.database = database;
  }

  public long getThreadId() {
    return threadId;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public long getClientCapabilities() {
    return clientCapabilities;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(short serverStatus) {
    this.serverStatus = serverStatus;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public IsolationLevel getIsolationLevel() {
    return isolationLevel;
  }

  public void setIsolationLevel(IsolationLevel isolationLevel) {
    this.isolationLevel = isolationLevel;
  }

  public ServerVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "ConnectionContext{" + "threadId=" + threadId + ", version=" + version + '}';
  }
}
