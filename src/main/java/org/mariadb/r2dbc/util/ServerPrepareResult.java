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

package org.mariadb.r2dbc.util;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.client.ClosePreparePacket;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;

public class ServerPrepareResult {

  private final int statementId;
  private final int numParams;
  private final ColumnDefinitionPacket[] columns;

  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicInteger use = new AtomicInteger(1);
  private final AtomicBoolean cached = new AtomicBoolean(false);

  public ServerPrepareResult(int statementId, int numParams, ColumnDefinitionPacket[] columns) {
    this.statementId = statementId;
    this.numParams = numParams;
    this.columns = columns;
  }

  public int getStatementId() {
    return statementId;
  }

  public int getNumParams() {
    return numParams;
  }

  public ColumnDefinitionPacket[] getColumns() {
    return columns;
  }

  public void close(Client client) {
    if (!cached.get() && closing.compareAndSet(false, true)) {
      client.sendCommandWithoutResult(new ClosePreparePacket(this.statementId));
    }
  }

  public void decrementUse(Client client) {
    if (use.decrementAndGet() <= 0 && !cached.get()) {
      close(client);
    }
  }

  public boolean incrementUse() {
    if (closing.get()) {
      return false;
    }
    use.getAndIncrement();
    return true;
  }

  public void unCache(Client client) {
    cached.set(false);
    if (use.get() <= 0) {
      close(client);
    }
  }

  public boolean cache() {
    if (closing.get()) {
      return false;
    }
    return cached.compareAndSet(false, true);
  }

  @Override
  public String toString() {
    return "ServerPrepareResult{"
        + "statementId="
        + statementId
        + ", numParams="
        + numParams
        + ", numColumns="
        + columns.length
        + ", closing="
        + closing
        + ", use="
        + use
        + ", cached="
        + cached
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerPrepareResult that = (ServerPrepareResult) o;
    return statementId == that.statementId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementId);
  }
}
