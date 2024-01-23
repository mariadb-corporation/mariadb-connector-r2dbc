// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.mariadb.r2dbc.message.ServerMessage;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Operators;

public class Exchange {
  private static final AtomicLongFieldUpdater<Exchange> DEMAND_UPDATER =
      AtomicLongFieldUpdater.newUpdater(Exchange.class, "demand");
  private final FluxSink<ServerMessage> sink;
  private final DecoderState initialState;
  private final String sql;
  private volatile long demand = 0L;

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

  public DecoderState getInitialState() {
    return initialState;
  }

  public String getSql() {
    return sql;
  }

  public boolean hasDemand() {
    return demand > 0;
  }

  public boolean isCancelled() {
    return sink.isCancelled();
  }

  public void onError(Throwable throwable) {
    if (!this.sink.isCancelled()) {
      this.sink.error(throwable);
    }
  }

  /**
   * Emit server message.
   *
   * @param srvMsg message to emit
   * @return true if ending message
   */
  public boolean emit(ServerMessage srvMsg) {
    if (this.sink.isCancelled()) {
      srvMsg.release();
      return srvMsg.ending();
    }

    Operators.addCap(DEMAND_UPDATER, this, -1);
    this.sink.next(srvMsg);
    if (srvMsg.ending()) {
      if (!this.sink.isCancelled()) {
        this.sink.complete();
      }
      return true;
    }
    return false;
  }

  public void incrementDemand(long n) {
    Operators.addCap(DEMAND_UPDATER, this, n);
  }
}
