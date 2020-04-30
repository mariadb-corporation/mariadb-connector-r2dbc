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

    // if there is already some cached data (and not been deallocate), return existing cached data
    if (cached != null) {
      cached.incrementUse();
      return cached;
    }

    // if no cache data, or been deallocate, put new result in cache
    super.put(key, result);
    return null;
  }
}
