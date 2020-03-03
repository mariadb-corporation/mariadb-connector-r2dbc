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

import org.mariadb.r2dbc.codec.list.*;

import java.util.HashMap;
import java.util.Map;

public class Codecs {

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
        MediumCodec.INSTANCE,
        ShortCodec.INSTANCE,
        StreamCodec.INSTANCE,
        StringCodec.INSTANCE,
        TinyIntCodec.INSTANCE
      };

  // association with enum, since doesn't supporting generics in enum :(
  public static final Map<DataType, Codec<?>> CODEC_LIST = new HashMap<>();

  static {
    CODEC_LIST.put(DataType.OLDDECIMAL, BigDecimalCodec.INSTANCE);
    CODEC_LIST.put(DataType.TINYINT, TinyIntCodec.INSTANCE);
    CODEC_LIST.put(DataType.SMALLINT, ShortCodec.INSTANCE);
    CODEC_LIST.put(DataType.INTEGER, IntCodec.INSTANCE);
    CODEC_LIST.put(DataType.DOUBLE, DoubleCodec.INSTANCE);
    CODEC_LIST.put(DataType.NULL, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.TIMESTAMP, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.BIGINT, LongCodec.INSTANCE);
    CODEC_LIST.put(DataType.MEDIUMINT, MediumCodec.INSTANCE);
    CODEC_LIST.put(DataType.DATE, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.TIME, DurationCodec.INSTANCE);
    CODEC_LIST.put(DataType.DATETIME, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.YEAR, ShortCodec.INSTANCE);
    CODEC_LIST.put(DataType.NEWDATE, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.VARCHAR, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.BIT, BitSetCodec.INSTANCE);
    CODEC_LIST.put(DataType.JSON, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.DECIMAL, BigDecimalCodec.INSTANCE);
    CODEC_LIST.put(DataType.ENUM, LocalDateTimeCodec.INSTANCE);
    CODEC_LIST.put(DataType.SET, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.TINYBLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.MEDIUMBLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.LONGBLOB, BlobCodec.INSTANCE);
    CODEC_LIST.put(DataType.BLOB, ByteArrayCodec.INSTANCE);
    CODEC_LIST.put(DataType.VARSTRING, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.STRING, StringCodec.INSTANCE);
    CODEC_LIST.put(DataType.GEOMETRY, ByteArrayCodec.INSTANCE);
  }

  public static Codec<?> from(DataType dataType) {
    return CODEC_LIST.get(dataType);
  }
}
