/*
 * Copyright 2020 MariaDB Ab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
