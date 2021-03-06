// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

public class Context {

  private final long threadId;
  private final long serverCapabilities;
  private short serverStatus;
  private ServerVersion version;

  public Context(
      String serverVersion,
      long threadId,
      long capabilities,
      short serverStatus,
      boolean mariaDBServer) {

    this.threadId = threadId;
    this.serverCapabilities = capabilities;
    this.serverStatus = serverStatus;
    this.version = new ServerVersion(serverVersion, mariaDBServer);
  }

  public long getThreadId() {
    return threadId;
  }

  public long getServerCapabilities() {
    return serverCapabilities;
  }

  public short getServerStatus() {
    return serverStatus;
  }

  public void setServerStatus(short serverStatus) {
    this.serverStatus = serverStatus;
  }

  public ServerVersion getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "ConnectionContext{" + "threadId=" + threadId + ", version=" + version + '}';
  }
}
