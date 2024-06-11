// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2024 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.OutParameterMetadata;
import java.nio.charset.StandardCharsets;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.ServerMessage;
import org.mariadb.r2dbc.util.CharsetEncodingLength;
import org.mariadb.r2dbc.util.MariadbType;
import org.mariadb.r2dbc.util.constants.ColumnFlags;

public final class ColumnDefinitionPacket
    implements ServerMessage, ColumnMetadata, OutParameterMetadata {
  private final byte[] meta;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private final boolean ending;
  private final MariadbConnectionConfiguration conf;

  private ColumnDefinitionPacket(
      byte[] meta,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      boolean ending,
      MariadbConnectionConfiguration conf) {
    this.meta = meta;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.ending = ending;
    this.conf = conf;
  }

  private ColumnDefinitionPacket(String name, MariadbConnectionConfiguration conf) {
    byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
    byte[] arr = new byte[6 + 2 * nameBytes.length];
    int pos = 0;

    // lenenc_str     catalog
    // lenenc_str     schema
    // lenenc_str     table
    // lenenc_str     org_table
    for (int i = 0; i < 4; i++) {
      arr[pos++] = 0;
    }

    // lenenc_str     name
    // lenenc_str     org_name
    for (int i = 0; i < 2; i++) {
      arr[pos++] = (byte) nameBytes.length;
      System.arraycopy(nameBytes, 0, arr, pos, nameBytes.length);
      pos += nameBytes.length;
    }

    this.meta = arr;
    this.charset = 33;
    this.length = 8;
    this.dataType = DataType.BIGINT;
    this.decimals = 0;
    this.flags = ColumnFlags.PRIMARY_KEY;
    this.ending = false;
    this.conf = conf;
  }

  public static ColumnDefinitionPacket decode(
      ByteBuf buf, boolean ending, MariadbConnectionConfiguration conf) {
    byte[] meta = new byte[buf.readableBytes() - 12];
    buf.readBytes(meta);
    int charset = buf.readUnsignedShortLE();
    long length = buf.readUnsignedIntLE();
    DataType dataType = DataType.fromServer(buf.readUnsignedByte(), charset);
    int flags = buf.readUnsignedShortLE();
    byte decimals = buf.readByte();
    return new ColumnDefinitionPacket(
        meta, charset, length, dataType, decimals, flags, ending, conf);
  }

  public static ColumnDefinitionPacket fromGeneratedId(
      String name, MariadbConnectionConfiguration conf) {
    return new ColumnDefinitionPacket(name, conf);
  }

  private String getString(int idx) {
    int pos = 0;
    for (int i = 0; i < idx; i++) {
      // maximum length of 64 characters.
      // so length encode is just encoded on one byte
      int len = this.meta[pos++] & 0xff;
      pos += len;
    }
    int length = this.meta[pos++] & 0xff;
    return new String(this.meta, pos, length, StandardCharsets.UTF_8);
  }

  public String getSchema() {
    return this.getString(1);
  }

  public String getTableAlias() {
    return this.getString(2);
  }

  public String getTable() {
    return this.getString(3);
  }

  @Override
  public String getName() {
    return this.getString(4);
  }

  public String getColumn() {
    return this.getString(5);
  }

  public int getCharset() {
    return charset;
  }

  public long getLength() {
    return length;
  }

  public DataType getDataType() {
    return dataType;
  }

  public byte getDecimals() {
    return decimals;
  }

  public boolean isSigned() {
    return ((flags & ColumnFlags.UNSIGNED) == 0);
  }

  public int getDisplaySize() {
    if (dataType == DataType.TEXT
        || dataType == DataType.JSON
        || dataType == DataType.ENUM
        || dataType == DataType.SET
        || dataType == DataType.VARSTRING
        || dataType == DataType.STRING) {
      return (int)
          (length
              / (CharsetEncodingLength.maxCharlen.get(charset) == 0
                  ? 1
                  : CharsetEncodingLength.maxCharlen.get(charset)));
    }
    return (int) length;
  }

  public Nullability getNullability() {
    return (flags & ColumnFlags.NOT_NULL) > 0 ? Nullability.NON_NULL : Nullability.NULLABLE;
  }

  public boolean isPrimaryKey() {
    return ((this.flags & ColumnFlags.PRIMARY_KEY) > 0);
  }

  public boolean isUniqueKey() {
    return ((this.flags & ColumnFlags.UNIQUE_KEY) > 0);
  }

  public boolean isMultipleKey() {
    return ((this.flags & ColumnFlags.MULTIPLE_KEY) > 0);
  }

  public boolean isBlob() {
    return ((this.flags & ColumnFlags.BLOB) > 0);
  }

  public boolean isZeroFill() {
    return ((this.flags & ColumnFlags.ZEROFILL) > 0);
  }

  // doesn't use & 128 bit filter, because char binary and varchar binary are not binary (handle
  // like string), but have the binary flag
  public boolean isBinary() {
    return (charset == 63);
  }

  public MariadbType getType() {
    switch (dataType) {
      case TINYINT:
        // TINYINT(1) are considered as boolean
        if (length == 1 && conf.tinyInt1isBit()) return MariadbType.BOOLEAN;
        return isSigned() ? MariadbType.TINYINT : MariadbType.UNSIGNED_TINYINT;
      case YEAR:
        return MariadbType.SMALLINT;
      case SMALLINT:
        return isSigned() ? MariadbType.SMALLINT : MariadbType.UNSIGNED_SMALLINT;
      case INTEGER:
        return isSigned() ? MariadbType.INTEGER : MariadbType.UNSIGNED_INTEGER;
      case FLOAT:
        return MariadbType.FLOAT;
      case DOUBLE:
        return MariadbType.DOUBLE;
      case TIMESTAMP:
      case DATETIME:
        return MariadbType.TIMESTAMP;
      case BIGINT:
        return isSigned() ? MariadbType.BIGINT : MariadbType.UNSIGNED_BIGINT;
      case MEDIUMINT:
        return MariadbType.INTEGER;
      case DATE:
      case NEWDATE:
        return MariadbType.DATE;
      case TIME:
        return MariadbType.TIME;
      case JSON:
      case ENUM:
      case SET:
      case STRING:
      case VARSTRING:
      case NULL:
        return isBinary() ? MariadbType.BYTES : MariadbType.VARCHAR;
      case TEXT:
        return MariadbType.CLOB;
      case OLDDECIMAL:
      case DECIMAL:
        return MariadbType.DECIMAL;
      case BIT:
        // BIT(1) are considered as boolean
        if (length == 1 && conf.tinyInt1isBit()) return MariadbType.BOOLEAN;
        return MariadbType.BIT;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return MariadbType.BLOB;

      default:
        return null;
    }
  }

  @Override
  public Integer getPrecision() {
    switch (dataType) {
      case OLDDECIMAL:
      case DECIMAL:
        // DECIMAL and OLDDECIMAL are  "exact" fixed-point number.
        // so :
        // - if can be signed, 1 byte is saved for sign
        // - if decimal > 0, one byte more for dot
        if (isSigned()) {
          return (int) (length - ((getDecimals() > 0) ? 2 : 1));
        } else {
          return (int) (length - ((decimals > 0) ? 1 : 0));
        }
      default:
        return (int) length;
    }
  }

  @Override
  public Integer getScale() {
    switch (dataType) {
      case OLDDECIMAL:
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case FLOAT:
      case DOUBLE:
      case BIGINT:
      case MEDIUMINT:
      case BIT:
      case DECIMAL:
        return (int) decimals;
      default:
        return 0;
    }
  }

  @Override
  public Class<?> getJavaType() {
    return getType().getJavaType();
  }

  public ColumnDefinitionPacket getNativeTypeMetadata() {
    return this;
  }

  @Override
  public boolean ending() {
    return this.ending;
  }
}
