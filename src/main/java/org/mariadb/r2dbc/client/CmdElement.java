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

package org.mariadb.r2dbc.client;

import org.mariadb.r2dbc.message.server.ServerMessage;
import reactor.core.publisher.FluxSink;

public class CmdElement {

  private final FluxSink<ServerMessage> sink;
  private final DecoderState initialState;
  private final String sql;

  public CmdElement(FluxSink<ServerMessage> sink, DecoderState initialState) {
    this.sink = sink;
    this.initialState = initialState;
    this.sql = null;
  }

  public CmdElement(FluxSink<ServerMessage> sink, DecoderState initialState, String sql) {
    this.sink = sink;
    this.initialState = initialState;
    this.sql = sql;
  }

  public FluxSink<ServerMessage> getSink() {
    return sink;
  }

  public DecoderState getInitialState() {
    return initialState;
  }

  public String getSql() {
    return sql;
  }
}
