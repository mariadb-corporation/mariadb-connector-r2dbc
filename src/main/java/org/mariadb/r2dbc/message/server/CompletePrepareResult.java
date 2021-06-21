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

package org.mariadb.r2dbc.message.server;

import org.mariadb.r2dbc.util.ServerPrepareResult;

public final class CompletePrepareResult implements ServerMessage {

  private final ServerPrepareResult prepare;
  private boolean continueOnEnd;

  public CompletePrepareResult(final ServerPrepareResult prepare, boolean continueOnEnd) {
    this.prepare = prepare;
    this.continueOnEnd = continueOnEnd;
  }

  @Override
  public boolean ending() {
    return continueOnEnd;
  }

  public ServerPrepareResult getPrepare() {
    return prepare;
  }
}
