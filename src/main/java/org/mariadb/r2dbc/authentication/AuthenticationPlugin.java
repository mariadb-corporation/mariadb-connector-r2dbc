// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.authentication;

import io.r2dbc.spi.R2dbcException;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.message.AuthMoreData;
import org.mariadb.r2dbc.message.AuthSwitch;
import org.mariadb.r2dbc.message.ClientMessage;

public interface AuthenticationPlugin {

  String type();

  AuthenticationPlugin create();

  ClientMessage next(
      MariadbConnectionConfiguration configuration,
      AuthSwitch authSwitch,
      AuthMoreData authMoreData)
      throws R2dbcException;
}
