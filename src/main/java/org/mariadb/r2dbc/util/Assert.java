// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import reactor.util.annotation.Nullable;

public final class Assert {

  private Assert() {}

  public static <T> T requireNonNull(@Nullable T t, String message) {
    if (t == null) {
      throw new IllegalArgumentException(message);
    }
    return t;
  }
}
