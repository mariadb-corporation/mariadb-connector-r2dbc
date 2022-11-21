// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import java.util.concurrent.atomic.AtomicLong;
import org.mariadb.r2dbc.message.ServerMessage;
import reactor.core.publisher.FluxSink;

public class Exchange {

  private final FluxSink<ServerMessage> sink;
  private final DecoderState initialState;
  private final String sql;
  private final AtomicLong demand = new AtomicLong();

  public Exchange(FluxSink<ServerMessage> sink, DecoderState initialState) {
    this.sink = sink;
    this.initialState = initialState;
    this.sql = null;
  }

  public Exchange(FluxSink<ServerMessage> sink, DecoderState initialState, String sql) {
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

  public boolean hasDemandOrIsCancelled() {
    return demand.get() > 0 || this.sink.isCancelled();
  }

  public void emit(ServerMessage srvMsg) {
    demand.decrementAndGet();
    if (this.sink.isCancelled()) {
      return;
    }
    this.sink.next(srvMsg);
    if (srvMsg.ending()) {
      this.sink.complete();
    }
  }

  public void incrementDemand(long n) {
    demand.addAndGet(n);
  }
}
