// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.tools;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TcpProxy {

  private final String host;
  private final TcpProxySocket socket;
  private ScheduledExecutorService executorService;

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
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(socket, 0, TimeUnit.MILLISECONDS);
  }

  public void stop() throws InterruptedException {
    socket.kill();
    executorService.shutdownNow();
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
  public void restart(long sleepTime) throws InterruptedException {
    socket.kill();
    executorService.shutdownNow();

    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.schedule(socket, sleepTime, TimeUnit.MILLISECONDS);
  }

  public void forceClose() {
    socket.sendRst();
    executorService.shutdownNow();
  }

  /** Restart proxy. */
  public void restart() throws InterruptedException {
    socket.kill();
    executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.execute(socket);
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      // eat Exception
    }
  }

  public int getLocalPort() {
    return socket.getLocalPort();
  }
}
