// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.util.LinkedHashMap;
import java.util.Map;
import org.mariadb.r2dbc.client.Client;

public class PrepareCache extends LinkedHashMap<String, ServerPrepareResult> {

  private static final long serialVersionUID = -8922905563713952695L;
  private final int maxSize;
  private final Client client;

  public PrepareCache(int size, Client client) {
    super(size, .75f, true);
    this.maxSize = size;
    this.client = client;
  }

  @Override
  public boolean removeEldestEntry(Map.Entry<String, ServerPrepareResult> eldest) {
    if (this.size() > maxSize) {
      eldest.getValue().unCache(client);
      return true;
    }
    return false;
  }

  public synchronized ServerPrepareResult put(String key, ServerPrepareResult result) {
    ServerPrepareResult cached = super.get(key);

    // if there is already some cached data, return existing cached data
    if (cached != null) {
      cached.incrementUse();
      result.unCache(client);
      return cached;
    }

    if (result.cache()) {
      super.put(key, result);
    }
    return null;
  }
}
