// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc;

import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;

public enum MariadbTransactionDefinition implements TransactionDefinition {
  WITH_CONSISTENT_SNAPSHOT_READ_WRITE {
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
      if (READ_ONLY.equals(option)) return (T) Boolean.FALSE;
      if (WITH_CONSISTENT_SNAPSHOT.equals(option)) return (T) Boolean.TRUE;
      return null;
    }
  },
  WITH_CONSISTENT_SNAPSHOT_READ_ONLY {
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
      if (READ_ONLY.equals(option)) return (T) Boolean.TRUE;
      if (WITH_CONSISTENT_SNAPSHOT.equals(option)) return (T) Boolean.TRUE;
      return null;
    }
  },
  READ_WRITE {
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
      if (READ_ONLY.equals(option)) return (T) Boolean.FALSE;
      if (WITH_CONSISTENT_SNAPSHOT.equals(option)) return (T) Boolean.FALSE;
      return null;
    }
  },
  READ_ONLY {
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttribute(Option<T> option) {
      if (READ_ONLY.equals(option)) return (T) Boolean.TRUE;
      if (WITH_CONSISTENT_SNAPSHOT.equals(option)) return (T) Boolean.FALSE;
      return null;
    }
  };

  public static Option<Boolean> WITH_CONSISTENT_SNAPSHOT =
      Option.valueOf("WITH CONSISTENT SNAPSHOT");
}
