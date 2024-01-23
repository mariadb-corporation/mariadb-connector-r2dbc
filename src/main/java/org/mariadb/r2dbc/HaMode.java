// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.mariadb.r2dbc.client.Client;
import org.mariadb.r2dbc.client.SimpleClient;
import org.mariadb.r2dbc.message.flow.AuthenticationFlow;
import org.mariadb.r2dbc.util.HostAddress;
import reactor.core.publisher.Mono;
import reactor.netty.resources.ConnectionProvider;

/** Failover (High-availability) mode */
public enum HaMode {
  /** sequential: driver will always connect according to connection string order */
  SEQUENTIAL(new String[] {"sequential"}) {
    public List<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      return getAvailableHostInOrder(hostAddresses, denyList);
    }

    public Mono<Client> connectHost(
        MariadbConnectionConfiguration conf, ReentrantLock lock, boolean failFast) {
      long endingNanoTime =
          CONNECTION_LOOP_DURATION.getSeconds() * 1_000_000_000 + System.nanoTime();
      return connectHost(conf, lock, failFast, this::getAvailableHost, endingNanoTime);
    }
  },

  /** load-balance: driver will randomly connect to any host, permitting balancing connections */
  LOADBALANCE(new String[] {"load-balance", "loadbalance", "loadbalancing"}) {
    public List<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      // use in order not blacklisted server
      List<HostAddress> loopAddress =
          new ArrayList<>(HaMode.getAvailableHostInOrder(hostAddresses, denyList));
      Collections.shuffle(loopAddress);
      return loopAddress;
    }

    public Mono<Client> connectHost(
        MariadbConnectionConfiguration conf, ReentrantLock lock, boolean failFast) {
      long endingNanoTime =
          CONNECTION_LOOP_DURATION.getSeconds() * 1_000_000_000 + System.nanoTime();
      return connectHost(conf, lock, failFast, this::getAvailableHost, endingNanoTime);
    }
  },

  /** no ha-mode. Connect to first host only */
  NONE(new String[0]) {
    public List<HostAddress> getAvailableHost(
        List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
      return hostAddresses;
    }

    public Mono<Client> connectHost(
        MariadbConnectionConfiguration conf, ReentrantLock lock, boolean failFast) {
      return connectHost(conf, lock, true, this::getAvailableHost, 0L);
    }
  };

  /** temporary blacklisted hosts */
  private static final ConcurrentMap<HostAddress, Long> denyList = new ConcurrentHashMap<>();

  /** denied timeout */
  private static final long DENIED_LIST_TIMEOUT =
      Long.parseLong(System.getProperty("deniedListTimeout", "60000000000"));

  private static final Duration CONNECTION_LOOP_DURATION =
      Duration.parse(System.getProperty("connectionLoopDuration", "PT10S"));

  private final String[] aliases;

  HaMode(String[] value) {
    this.aliases = value;
  }

  /**
   * Get HAMode from values or aliases
   *
   * @param value value or alias
   * @return HaMode if corresponding mode is found
   */
  public static HaMode from(String value) {
    for (HaMode haMode : values()) {
      if (haMode.name().equalsIgnoreCase(value)) {
        return haMode;
      }
      for (String alias : haMode.aliases) {
        if (alias.equalsIgnoreCase(value)) {
          return haMode;
        }
      }
    }
    throw new IllegalArgumentException(
        String.format("Wrong argument value '%s' for HaMode", value));
  }

  private static Mono<Client> connect(
      MariadbConnectionConfiguration conf, ReentrantLock lock, HostAddress hostAddress) {
    return SimpleClient.connect(
            ConnectionProvider.newConnection(),
            InetSocketAddress.createUnresolved(hostAddress.getHost(), hostAddress.getPort()),
            hostAddress,
            conf,
            lock)
        .delayUntil(client -> AuthenticationFlow.exchange(client, conf, hostAddress))
        .doOnError(e -> HaMode.failHost(hostAddress))
        .cast(Client.class)
        .flatMap(
            client ->
                MariadbConnectionFactory.setSessionVariables(conf, client).then(Mono.just(client)));
  }

  /**
   * return hosts of without blacklisted hosts. hosts in blacklist reaching blacklist timeout will
   * be present. order corresponds to connection string order.
   *
   * @param hostAddresses hosts
   * @param denyList blacklist
   * @return list without denied hosts
   */
  private static List<HostAddress> getAvailableHostInOrder(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList) {
    // use in order not blacklisted server
    List<HostAddress> copiedList = new ArrayList<>(hostAddresses);
    denyList.entrySet().stream()
        .filter(e -> e.getValue() < System.nanoTime())
        .forEach(e -> denyList.remove(e.getKey()));
    copiedList.removeAll(denyList.keySet());
    return copiedList;
  }

  public static Mono<Client> resumeConnect(
      Throwable t,
      MariadbConnectionConfiguration conf,
      ReentrantLock lock,
      boolean failFast,
      List<HostAddress> availableHosts,
      BiFunction<List<HostAddress>, ConcurrentMap<HostAddress, Long>, List<HostAddress>> availHost,
      Iterator<HostAddress> iterator,
      long endingNanoTime) {
    if (!iterator.hasNext()) {
      if (failFast || System.nanoTime() > endingNanoTime) {
        return Mono.error(
            ExceptionFactory.INSTANCE.createParsingException(
                String.format(
                    "Fail to establish connection to %s %s: %s",
                    availableHosts,
                    (failFast || endingNanoTime == 0L) ? "" : ", reaching timeout",
                    t.getMessage()),
                t));
      }
      try {
        Thread.sleep(250);
      } catch (InterruptedException e) {
        // eat
      }
      return connectHost(conf, lock, failFast, availHost, endingNanoTime);
    }
    return HaMode.connect(conf, lock, iterator.next())
        .onErrorResume(
            tt ->
                resumeConnect(
                    tt, conf, lock, failFast, availableHosts, availHost, iterator, endingNanoTime));
  }

  public static Mono<Client> connectHost(
      MariadbConnectionConfiguration conf,
      ReentrantLock lock,
      boolean failFast,
      BiFunction<List<HostAddress>, ConcurrentMap<HostAddress, Long>, List<HostAddress>> availHost,
      long endingNanoTime) {
    List<HostAddress> nonBlacklistHosts = availHost.apply(conf.getHostAddresses(), denyList);
    List<HostAddress> availableHosts;
    if (!failFast) {
      nonBlacklistHosts.addAll(denyList.keySet());
      // remove host from denyList not in initial host list
      availableHosts =
          nonBlacklistHosts.stream()
              .filter(h -> conf.getHostAddresses().contains(h))
              .collect(Collectors.toList());
    } else {
      availableHosts = nonBlacklistHosts;
    }

    Iterator<HostAddress> iterator = availableHosts.iterator();
    if (!iterator.hasNext())
      return Mono.error(
          ExceptionFactory.INSTANCE.createParsingException(
              "Fail to establish connection: no available host"));
    return HaMode.connect(conf, lock, iterator.next())
        .onErrorResume(
            t ->
                resumeConnect(
                    t, conf, lock, failFast, availableHosts, availHost, iterator, endingNanoTime));
  }

  public static void failHost(HostAddress hostAddress) {
    denyList.put(hostAddress, System.nanoTime() + DENIED_LIST_TIMEOUT);
  }

  /**
   * List of hosts without blacklist entries, ordered according to HA mode
   *
   * @param hostAddresses hosts
   * @param denyList hosts temporary denied
   * @return list without denied hosts
   */
  public abstract List<HostAddress> getAvailableHost(
      List<HostAddress> hostAddresses, ConcurrentMap<HostAddress, Long> denyList);

  public abstract Mono<Client> connectHost(
      MariadbConnectionConfiguration conf, ReentrantLock lock, boolean failFast);
}
