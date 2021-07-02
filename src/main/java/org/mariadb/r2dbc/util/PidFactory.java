// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import java.lang.reflect.Method;
import java.util.function.Supplier;

public final class PidFactory {
  private static Supplier<String> instance;

  static {
    try {
      // if java 9+
      Class<?> processHandle = Class.forName("java.lang.ProcessHandle");
      instance =
          () -> {
            try {
              Method currentProcessMethod = processHandle.getMethod("current");
              Object currentProcess = currentProcessMethod.invoke(null);
              Method pidMethod = processHandle.getMethod("pid");
              return String.valueOf(pidMethod.invoke(currentProcess));
            } catch (Throwable throwable) {
              return null;
            }
          };
    } catch (Throwable cle) {
      instance = () -> null;
    }
  }

  public static Supplier<String> getInstance() {
    return instance;
  }
}
