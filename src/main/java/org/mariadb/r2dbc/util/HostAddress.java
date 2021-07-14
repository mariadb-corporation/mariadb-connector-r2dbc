package org.mariadb.r2dbc.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HostAddress {
  String host;
  int port;

  public HostAddress(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public static List<HostAddress> parse(String hosts, int defaultPort) {
    // parse host for multiple hosts.
    if (hosts != null) {
      List<HostAddress> hostAddresses = new ArrayList<>();
      String[] tmpHosts = hosts.split(",");
      for (String tmpHost : tmpHosts) {
        if (tmpHost.contains(":")) {
          hostAddresses.add(
              new HostAddress(
                  tmpHost.substring(0, tmpHost.indexOf(":")),
                  Integer.parseInt(tmpHost.substring(tmpHost.indexOf(":") + 1))));
        } else {
          hostAddresses.add(new HostAddress(tmpHost, defaultPort));
        }
      }
      return hostAddresses;
    } else {
      return Collections.singletonList(new HostAddress("localhost", defaultPort));
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof HostAddress)) return false;
    HostAddress that = (HostAddress) o;
    return port == that.port && host.equals(that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public String toString() {
    return "HostAddress{" + "host='" + host + '\'' + ", port=" + port + '}';
  }
}
