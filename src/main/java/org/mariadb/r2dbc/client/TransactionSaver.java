package org.mariadb.r2dbc.client;

import java.util.Queue;
import org.mariadb.r2dbc.message.ClientMessage;
import reactor.util.concurrent.Queues;

/**
 * Transaction cache Huge command are not cached, cache is limited to configuration
 * transactionReplaySize commands
 */
public class TransactionSaver {
  private final Queue<ClientMessage> messages =
      Queues.<ClientMessage>get(Queues.SMALL_BUFFER_SIZE).get();
  private boolean dirty = false;

  /**
   * Add a command to cache.
   *
   * @param clientMessage client message
   */
  public void add(ClientMessage clientMessage) {
    if (!messages.offer(clientMessage)) {
      dirty = true;
    }
  }

  /** Transaction finished, clearing cache */
  public void clear() {
    messages.clear();
    dirty = false;
  }

  /**
   * Is cache not valid (some commands have not been cached)
   *
   * @return is dirty
   */
  public boolean isDirty() {
    return dirty;
  }

  public void forceDirty() {
    dirty = true;
  }

  /**
   * cache buffer
   *
   * @return cached messages
   */
  public Queue<ClientMessage> getMessages() {
    return messages;
  }
}
