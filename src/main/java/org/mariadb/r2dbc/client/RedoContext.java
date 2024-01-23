// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.IsolationLevel;
import org.mariadb.r2dbc.message.ClientMessage;
import org.mariadb.r2dbc.util.constants.ServerStatus;

public class RedoContext extends SimpleContext {

  private final TransactionSaver transactionSaver;

  public RedoContext(
      String serverVersion,
      long threadId,
      long capabilities,
      short serverStatus,
      boolean mariaDBServer,
      long clientCapabilities,
      String database,
      ByteBufAllocator byteBufAllocator,
      IsolationLevel isolationLevel) {
    super(
        serverVersion,
        threadId,
        capabilities,
        serverStatus,
        mariaDBServer,
        clientCapabilities,
        database,
        byteBufAllocator,
        isolationLevel);
    transactionSaver = new TransactionSaver();
  }

  /**
   * Set server status
   *
   * @param serverStatus server status
   */
  public void setServerStatus(short serverStatus) {
    super.setServerStatus(serverStatus);
    if ((serverStatus & ServerStatus.IN_TRANSACTION) == 0) {
      transactionSaver.clear();
    }
  }

  /**
   * Save client message
   *
   * @param msg client message
   */
  public void saveRedo(ClientMessage msg, ByteBuf buf, int initialReaderIndex) {
    msg.save(buf, initialReaderIndex);
    transactionSaver.add(msg);
  }

  /**
   * Get transaction saver cache
   *
   * @return transaction saver cache
   */
  public TransactionSaver getTransactionSaver() {
    return transactionSaver;
  }
}
