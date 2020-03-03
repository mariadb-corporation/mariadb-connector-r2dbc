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

package org.mariadb.r2dbc.codec;

public enum DataType {
  OLDDECIMAL(0),
  TINYINT(1),
  SMALLINT(2),
  INTEGER(3),
  FLOAT(4),
  DOUBLE(5),
  NULL(6),
  TIMESTAMP(7),
  BIGINT(8),
  MEDIUMINT(9),
  DATE(10),
  TIME(11),
  DATETIME(12),
  YEAR(13),
  NEWDATE(14),
  VARCHAR(15),
  BIT(16),
  JSON(245),
  DECIMAL(246),
  ENUM(247),
  SET(248),
  TINYBLOB(249),
  MEDIUMBLOB(250),
  LONGBLOB(251),
  BLOB(252),
  VARSTRING(253),
  STRING(254),
  GEOMETRY(255);

  static final DataType[] typeMap;

  static {
    typeMap = new DataType[256];
    for (DataType v : values()) {
      typeMap[v.mariadbType] = v;
    }
  }

  private final short mariadbType;

  DataType(int mariadbType) {
    this.mariadbType = (short) mariadbType;
  }

  /**
   * Convert server Type to server type.
   *
   * @param typeValue type value
   * @param charsetNumber charset
   * @return MariaDb type
   */
  public static DataType fromServer(int typeValue, int charsetNumber) {

    DataType dataType = typeMap[typeValue];

    if (dataType == null) {
      // Potential fallback for types that are not implemented.
      // Should not be normally used.
      dataType = BLOB;
    }

    if (charsetNumber != 63 && typeValue >= 249 && typeValue <= 252) {
      // MariaDB Text dataType
      return DataType.VARCHAR;
    }

    return dataType;
  }
}
