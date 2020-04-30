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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.message.client.ClosePreparePacket;

public class ServerPrepareResult {

  private final int statementId;
  private final int numColumns;
  private final int numParams;

  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicInteger use = new AtomicInteger(1);
  private final AtomicBoolean cached = new AtomicBoolean(true);

  public ServerPrepareResult(int statementId, int numColumns, int numParams) {
    this.statementId = statementId;
    this.numColumns = numColumns;
    this.numParams = numParams;
  }

  public int getStatementId() {
    return statementId;
  }

  public int getNumColumns() {
    return numColumns;
  }

  public int getNumParams() {
    return numParams;
  }

  public void close(Client client) {
    if (closing.compareAndSet(false, true)) {
      client.sendCommand(new ClosePreparePacket(this.statementId));
    }
  }

  public void decrementUse(Client client) {
    if (use.decrementAndGet() <= 0 && !cached.get()) {
      close(client);
    }
  }

  public void incrementUse() {
    use.getAndIncrement();
  }

  public void unCache(Client client) {
    cached.set(false);
    if (use.decrementAndGet() <= 0) {
      close(client);
    }
  }

  @Override
  public String toString() {
    return "ServerPrepareResult{"
        + "statementId="
        + statementId
        + ", numColumns="
        + numColumns
        + ", numParams="
        + numParams
        + ", closing="
        + closing
        + ", use="
        + use
        + ", cached="
        + cached
        + '}';
  }
}
