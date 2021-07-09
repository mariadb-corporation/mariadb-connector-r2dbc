// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2021 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec;

import java.util.HashMap;
import java.util.Map;
import org.mariadb.r2dbc.codec.list.*;

public final class Codecs {

  public static final Codec<?>[] LIST =
      new Codec<?>[] {
        BigDecimalCodec.INSTANCE,
        BigIntegerCodec.INSTANCE,
        BooleanCodec.INSTANCE,
        BlobCodec.INSTANCE,
        ByteArrayCodec.INSTANCE,
        ByteCodec.INSTANCE,
        BitSetCodec.INSTANCE,
        ClobCodec.INSTANCE,
        DoubleCodec.INSTANCE,
        LongCodec.INSTANCE,
        FloatCodec.INSTANCE,
        IntCodec.INSTANCE,
        LocalDateCodec.INSTANCE,
        LocalDateTimeCodec.INSTANCE,
        LocalTimeCodec.INSTANCE,
        DurationCodec.INSTANCE,
        ShortCodec.INSTANCE,
        StreamCodec.INSTANCE,
        StringCodec.INSTANCE
      };

  // association with enum, since doesn't supporting generics in enum :(
  public static final Map<DataType, Codec<?>> CODEC_LIST = new HashMap<>();

  static {
    CODEC_LIST.put(DataType.OLDDECIMAL, BigDecimalCodec.INSTANCE);
    CODEC_LIST.put(DataType.TINYINT, ByteCodec.INSTANCE);
    CODEC_LIST.put(DataType.SMALLINT, ShortCodec.INSTANCE);
    CODEC_LIST.put(DataType.INTEGER, IntCodec.INSTANCE);
    CODEC_LIST.put(DataType.DOUBLE, DoubleCodec.INSTANCE);
    CODEC_LIST.put(DataType.NULL, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.TIMESTAMP, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.BIGINT, LongCodec.INSTANCE);
    CODEC_LIST.put(DataType.DATE, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.TIME, DurationCodec.INSTANCE);
    CODEC_LIST.put(DataType.DATETIME, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.YEAR, ShortCodec.INSTANCE);
    CODEC_LIST.put(DataType.NEWDATE, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.TEXT, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.BIT, BitSetCodec.INSTANCE);
    CODEC_LIST.put(DataType.JSON, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.DECIMAL, BigDecimalCodec.INSTANCE);
    CODEC_LIST.put(DataType.ENUM, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.SET, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.TINYBLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.MEDIUMBLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.LONGBLOB, BlobCodec.INSTANCE);
    CODEC_LIST.put(DataType.BLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.VARSTRING, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.STRING, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.GEOMETRY, ByteArrayCodec.INSTANCE);
  }

  public static Codec<?> codecByClass(Class<?> value, int index) {
    for (Codec<?> codec : Codecs.LIST) {
      if (codec.canEncode(value)) {
        return codec;
      }
    }
    throw new IllegalArgumentException(
        String.format("No encoder for class %s (parameter at index %s) ", value.getName(), index));
  }
}
