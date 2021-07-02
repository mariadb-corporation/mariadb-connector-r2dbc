// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.message.server;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Nullability;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.BitSet;
import org.mariadb.r2dbc.MariadbConnectionConfiguration;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.codec.list.*;
import org.mariadb.r2dbc.util.constants.ColumnFlags;
import reactor.util.Logger;
import reactor.util.Loggers;

public final class ColumnDefinitionPacket implements ServerMessage {
  private static final Logger logger = Loggers.getLogger(ColumnDefinitionPacket.class);

  // This array stored character length for every collation id up to collation id 256
  // It is generated from the information schema using
  // "select  id, maxlen from information_schema.character_sets, information_schema.collations
  // where character_sets.character_set_name = collations.character_set_name order by id"
  private static final int[] maxCharlen = {
    0, 2, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 3, 2, 1, 1,
    1, 0, 1, 2, 1, 1, 1, 1,
    2, 1, 1, 1, 2, 1, 1, 1,
    1, 3, 1, 2, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 4, 4, 1,
    1, 1, 1, 1, 1, 1, 4, 4,
    0, 1, 1, 1, 4, 4, 0, 1,
    1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 0, 1, 1, 1,
    1, 1, 1, 3, 2, 2, 2, 2,
    2, 1, 2, 3, 1, 1, 1, 2,
    2, 3, 3, 1, 0, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 0, 0, 0, 0, 0, 0, 0,
    2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 0, 2, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 2,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0,
    3, 3, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 3, 3, 3, 3, 3,
    3, 3, 3, 3, 0, 3, 4, 4,
    0, 0, 0, 0, 0, 0, 0, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 4, 4, 4, 4,
    4, 4, 4, 4, 0, 4, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0
  };
  private final byte[] meta;
  private final int charset;
  private final long length;
  private final DataType dataType;
  private final byte decimals;
  private final int flags;
  private boolean ending;

  private ColumnDefinitionPacket(
      byte[] meta,
      int charset,
      long length,
      DataType dataType,
      byte decimals,
      int flags,
      boolean ending) {
    this.meta = meta;
    this.charset = charset;
    this.length = length;
    this.dataType = dataType;
    this.decimals = decimals;
    this.flags = flags;
    this.ending = ending;
  }

  private ColumnDefinitionPacket(String name) {
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
  }

  public static ColumnDefinitionPacket decode(
      Sequencer sequencer, ByteBuf buf, Context context, boolean ending) {
    byte[] meta = new byte[buf.readableBytes() - 12];
    buf.readBytes(meta);
    int charset = buf.readUnsignedShortLE();
    long length = buf.readUnsignedIntLE();
    DataType dataType = DataType.fromServer(buf.readUnsignedByte(), charset);
    int flags = buf.readUnsignedShortLE();
    byte decimals = buf.readByte();
    return new ColumnDefinitionPacket(meta, charset, length, dataType, decimals, flags, ending);
  }

  public static ColumnDefinitionPacket fromGeneratedId(String name) {
    return new ColumnDefinitionPacket(name);
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

  public String getColumnAlias() {
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

  public DataType getType() {
    return dataType;
  }

  public byte getDecimals() {
    return decimals;
  }

  public boolean isSigned() {
    return ((flags & ColumnFlags.UNSIGNED) == 0);
  }

  public int getDisplaySize() {
    if (dataType == DataType.VARCHAR
        || dataType == DataType.JSON
        || dataType == DataType.ENUM
        || dataType == DataType.SET
        || dataType == DataType.VARSTRING
        || dataType == DataType.STRING) {
      return (int) (length / (maxCharlen[charset] == 0 ? 1 : maxCharlen[charset]));
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

  public Class<?> getJavaClass() {
    switch (dataType) {
      case TINYINT:
        return isSigned() ? Byte.class : Short.class;
      case SMALLINT:
        return isSigned() ? Short.class : Integer.class;
      case INTEGER:
        return isSigned() ? Integer.class : Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case TIMESTAMP:
      case DATETIME:
        return LocalDateTime.class;
      case BIGINT:
        return isSigned() ? Long.class : BigInteger.class;
      case MEDIUMINT:
        return Integer.class;
      case DATE:
      case NEWDATE:
        return LocalDate.class;
      case TIME:
        return Duration.class;
      case YEAR:
        return Short.class;
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteBuffer.class : String.class;
      case OLDDECIMAL:
      case DECIMAL:
        return BigDecimal.class;
      case BIT:
        return BitSet.class;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return Blob.class;

      default:
        return null;
    }
  }

  public Codec<?> getDefaultCodec(MariadbConnectionConfiguration conf) {
    switch (dataType) {
      case VARCHAR:
      case JSON:
      case ENUM:
      case SET:
      case VARSTRING:
      case STRING:
        return isBinary() ? ByteArrayCodec.INSTANCE : StringCodec.INSTANCE;
      case TINYINT:
        // TINYINT(1) are considered as boolean
        if (length == 1 && conf.tinyInt1isBit()) return BooleanCodec.INSTANCE;
        return isSigned() ? ByteCodec.INSTANCE : ShortCodec.INSTANCE;
      case SMALLINT:
        return isSigned() ? ShortCodec.INSTANCE : IntCodec.INSTANCE;
      case INTEGER:
        return isSigned() ? IntCodec.INSTANCE : LongCodec.INSTANCE;
      case FLOAT:
        return FloatCodec.INSTANCE;
      case DOUBLE:
        return DoubleCodec.INSTANCE;
      case TIMESTAMP:
      case DATETIME:
        return LocalDateTimeCodec.INSTANCE;
      case BIGINT:
        return isSigned() ? LongCodec.INSTANCE : BigIntegerCodec.INSTANCE;
      case MEDIUMINT:
        return IntCodec.INSTANCE;
      case DATE:
      case NEWDATE:
        return LocalDateCodec.INSTANCE;
      case TIME:
        return DurationCodec.INSTANCE;
      case YEAR:
        return ShortCodec.INSTANCE;
      case OLDDECIMAL:
      case DECIMAL:
        return BigDecimalCodec.INSTANCE;
      case BIT:
        // BIT(1) are considered as boolean
        if (length == 1 && conf.tinyInt1isBit()) return BooleanCodec.INSTANCE;
        return BitSetCodec.INSTANCE;
      case GEOMETRY:
        return ByteArrayCodec.INSTANCE;
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
        return BlobCodec.INSTANCE;
      default:
        return null;
    }
  }

  @Override
  public boolean ending() {
    return this.ending;
  }
}
