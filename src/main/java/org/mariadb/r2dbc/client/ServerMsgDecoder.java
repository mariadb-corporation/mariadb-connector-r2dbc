package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import java.util.Queue;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.message.server.PrepareResultPacket;
import org.mariadb.r2dbc.message.server.Sequencer;
import org.mariadb.r2dbc.util.PrepareCache;
import org.mariadb.r2dbc.util.ServerPrepareResult;
import reactor.util.concurrent.Queues;

public class ServerMsgDecoder {
  private final Client client;
  private DecoderState state = null;
  private final Queue<String> prepareSql = Queues.<String>small().get();
  private long clientCapabilities;
  private int stateCounter = 0;
  private PrepareResultPacket prepare;
  private ColumnDefinitionPacket[] prepareColumns;
  private Context context = null;

  public ServerMsgDecoder(Client client) {
    this.client = client;
  }

  public ServerMessage decode(ByteBuf packet, Exchange exchange) {
    Sequencer sequencer = new Sequencer(packet.readByte());
    if (state == null)
      state = exchange == null ? DecoderState.QUERY_RESPONSE : exchange.getInitialState();
    state = state.decoder(packet.getUnsignedByte(packet.readerIndex()), packet.readableBytes());
    ServerMessage msg = state.decode(packet, sequencer, this);
    state = msg.ending() ? null : state.next(this);
    return msg;
  }

  public Context getContext() {
    return context;
  }

  public int getStateCounter() {
    return stateCounter;
  }

  public void setStateCounter(int counter) {
    stateCounter = counter;
  }

  public PrepareResultPacket getPrepare() {
    return prepare;
  }

  public void setPrepare(PrepareResultPacket prepare) {
    this.prepare = prepare;
    this.prepareColumns =
        (prepare == null) ? null : new ColumnDefinitionPacket[prepare.getNumColumns()];
  }

  public ColumnDefinitionPacket[] getPrepareColumns() {
    return prepareColumns;
  }

  public MariadbConnectionConfiguration getConf() {
    return this.client.getConf();
  }

  public ServerPrepareResult endPrepare() {
    ServerPrepareResult prepareResult =
        new ServerPrepareResult(
            this.prepare.getStatementId(), this.prepare.getNumParams(), prepareColumns);
    String sql = prepareSql.poll();
    PrepareCache prepareCache = client.getPrepareCache();
    if (prepareCache != null) {
      ServerPrepareResult cached = prepareCache.put(sql, prepareResult);
      if (cached != null) {
        // race condition, remove new one to get the one in cache
        prepareResult.decrementUse(client);
        prepareResult = cached;
      }
    }
    return prepareResult;
  }

  public void decrementStateCounter() {
    stateCounter--;
  }

  public long getClientCapabilities() {
    return clientCapabilities;
  }

  public boolean addPrepare(String sql) {
    return this.prepareSql.offer(sql);
  }

  public void setContext(Context context) {
    this.context = context;
    this.clientCapabilities = this.context.getClientCapabilities();
  }
}
