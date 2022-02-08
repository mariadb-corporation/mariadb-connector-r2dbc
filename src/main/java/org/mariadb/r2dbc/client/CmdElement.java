// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import org.mariadb.r2dbc.message.ServerMessage;
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
