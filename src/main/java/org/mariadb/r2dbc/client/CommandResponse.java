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

import io.netty.buffer.ByteBuf;
import org.mariadb.r2dbc.client.ServerPacketState.TriFunction;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.message.server.ServerMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.MonoSink;

public class CommandResponse {

  private FluxSink<ServerMessage> sink;
  private TriFunction<ByteBuf, Sequencer, ConnectionContext> nextState;

  public CommandResponse(
      MonoSink<Flux<ServerMessage>> sink,
      TriFunction<ByteBuf, Sequencer, ConnectionContext> nextState) {

    sink.success(Flux.create(fluxSink -> this.sink = fluxSink));
    this.nextState = nextState;
  }

  public FluxSink<ServerMessage> getSink() {
    return sink;
  }

  public TriFunction<ByteBuf, Sequencer, ConnectionContext> getNextState() {
    return nextState;
  }
}
