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

package org.mariadb.r2dbc.util.constants;

public class ColumnFlags {
  public static final short NOT_NULL = 1;
  public static final short PRIMARY_KEY = 2;
  public static final short UNIQUE_KEY = 4;
  public static final short MULTIPLE_KEY = 8;
  public static final short BLOB = 16;
  public static final short UNSIGNED = 32;
  public static final short ZEROFILL = 64;
  public static final short BINARY_COLLATION = 128;
  public static final short ENUM = 256;
  public static final short AUTO_INCREMENT = 512;
  public static final short TIMESTAMP = 1024;
  public static final short SET = 2048;
}
