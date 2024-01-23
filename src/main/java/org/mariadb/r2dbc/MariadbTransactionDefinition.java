// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mariadb.r2dbc.util.Assert;

public final class MariadbTransactionDefinition implements TransactionDefinition {

  public static final MariadbTransactionDefinition EMPTY =
      new MariadbTransactionDefinition(Collections.emptyMap());

  public static Option<Boolean> WITH_CONSISTENT_SNAPSHOT =
      Option.valueOf("WITH CONSISTENT SNAPSHOT");
  public static MariadbTransactionDefinition WITH_CONSISTENT_SNAPSHOT_READ_WRITE =
      EMPTY.consistent().readWrite();
  public static MariadbTransactionDefinition WITH_CONSISTENT_SNAPSHOT_READ_ONLY =
      EMPTY.consistent().readOnly();
  public static MariadbTransactionDefinition READ_WRITE = EMPTY.readWrite();
  public static MariadbTransactionDefinition READ_ONLY = EMPTY.readOnly();
  private final Map<Option<?>, Object> options;

  private MariadbTransactionDefinition(Map<Option<?>, Object> options) {
    this.options = options;
  }

  static MariadbTransactionDefinition mutability(boolean readWrite) {
    return readWrite ? EMPTY.readWrite() : EMPTY.readOnly();
  }

  static MariadbTransactionDefinition from(IsolationLevel isolationLevel) {
    return MariadbTransactionDefinition.EMPTY.isolationLevel(isolationLevel);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getAttribute(Option<T> option) {
    return (T) this.options.get(option);
  }

  public MariadbTransactionDefinition with(Option<?> option, Object value) {

    Map<Option<?>, Object> options = new HashMap<>(this.options);
    options.put(
        Assert.requireNonNull(option, "option must not be null"),
        Assert.requireNonNull(value, "value must not be null"));

    return new MariadbTransactionDefinition(options);
  }

  public MariadbTransactionDefinition isolationLevel(IsolationLevel isolationLevel) {
    return with(TransactionDefinition.ISOLATION_LEVEL, isolationLevel);
  }

  public MariadbTransactionDefinition readOnly() {
    return with(TransactionDefinition.READ_ONLY, true);
  }

  public MariadbTransactionDefinition readWrite() {
    return with(TransactionDefinition.READ_ONLY, false);
  }

  public MariadbTransactionDefinition consistent() {
    return with(MariadbTransactionDefinition.WITH_CONSISTENT_SNAPSHOT, true);
  }

  public MariadbTransactionDefinition notConsistent() {
    return with(MariadbTransactionDefinition.WITH_CONSISTENT_SNAPSHOT, false);
  }
}
