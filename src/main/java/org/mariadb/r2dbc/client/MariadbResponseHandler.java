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

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import java.util.List;
import java.util.function.Consumer;
import org.mariadb.r2dbc.message.server.ServerMessage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.MonoSink;

public class MariadbResponseHandler extends MessageToMessageDecoder<ServerMessage> {

  private final Client client;
  private FluxSink<ServerMessage> fluxSink = null;
  private Consumer<FluxSink<ServerMessage>> sinkConsumer = sink -> fluxSink = sink;

  public MariadbResponseHandler(Client client) {
    this.client = client;
  }

  private void newReceiver() {

    MonoSink<Flux<ServerMessage>> receiver = client.nextReceiver();
    if (receiver == null) {
      throw new R2dbcNonTransientResourceException(
          "unexpected message received when no command was send");
    }
    receiver.success(Flux.create(sinkConsumer));
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ServerMessage msg, List<Object> out)
      throws Exception {
    if (fluxSink == null) {
      newReceiver();
    }
    fluxSink.next(msg);
    if (msg.ending()) {
      fluxSink.complete();
      client.sendNext();
      fluxSink = null;
    }
  }

  public void close() {
    if (fluxSink != null) {
      fluxSink.error(new R2dbcNonTransientResourceException("Connection is closing"));
    }
  }
}
