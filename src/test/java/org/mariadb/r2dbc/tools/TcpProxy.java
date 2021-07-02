// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.tools;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TcpProxy {
  private static final Logger logger = LoggerFactory.getLogger(TcpProxy.class);

  private final String host;
  private TcpProxySocket socket;

  /**
   * Initialise proxy.
   *
   * @param host host (ip / dns)
   * @param remoteport port
   * @throws IOException exception
   */
  public TcpProxy(String host, int remoteport) throws IOException {
    this.host = host;
    socket = new TcpProxySocket(host, remoteport);
    Executors.newSingleThreadScheduledExecutor().schedule(socket, 0, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    socket.kill();
  }

  public void setDelay(int delay) {
    socket.setDelay(delay);
  }

  public void removeDelay() {
    socket.setDelay(1);
  }

  /**
   * Stop proxy and restart after X milliseconds.
   *
   * @param sleepTime sleep time in milliseconds
   */
  public void restart(long sleepTime) {
    socket.kill();
    logger.trace("host proxy port " + socket.getLocalport() + " for " + host + " started");
    Executors.newSingleThreadScheduledExecutor().schedule(socket, sleepTime, TimeUnit.MILLISECONDS);
  }

  public void forceClose() {
    socket.sendRst();
  }

  /** Restart proxy. */
  public void restart() {
    Executors.newSingleThreadExecutor().execute(socket);
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      // eat Exception
    }
  }

  /** Assure that proxy is in a stable status. */
  public void assureProxyOk() {
    if (socket.isClosed()) {
      restart();
    }
  }

  public int getLocalPort() {
    return socket.getLocalPort();
  }
}
