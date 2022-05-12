package org.mariadb.r2dbc.util;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.list.*;

public enum MariadbType implements Type {
  TINYINT(R2dbcType.TINYINT.name(), Byte.class, ByteCodec.INSTANCE),
  UNSIGNED_TINYINT(R2dbcType.TINYINT.name(), Short.class, ShortCodec.INSTANCE),
  SMALLINT(R2dbcType.SMALLINT.name(), Short.class, ShortCodec.INSTANCE),
  UNSIGNED_SMALLINT(R2dbcType.SMALLINT.name(), Integer.class, IntCodec.INSTANCE),
  INTEGER(R2dbcType.INTEGER.name(), Integer.class, IntCodec.INSTANCE),
  UNSIGNED_INTEGER(R2dbcType.INTEGER.name(), Long.class, LongCodec.INSTANCE),
  FLOAT(R2dbcType.FLOAT.name(), Float.class, FloatCodec.INSTANCE),
  DOUBLE(R2dbcType.DOUBLE.name(), Double.class, DoubleCodec.INSTANCE),
  BIGINT(R2dbcType.BIGINT.name(), Long.class, LongCodec.INSTANCE),
  UNSIGNED_BIGINT(R2dbcType.BIGINT.name(), BigInteger.class, BigIntegerCodec.INSTANCE),
  TIME(R2dbcType.TIME.name(), LocalTime.class, LocalTimeCodec.INSTANCE),
  TIMESTAMP(R2dbcType.TIMESTAMP.name(), LocalDateTime.class, LocalDateTimeCodec.INSTANCE),
  DATE(R2dbcType.DATE.name(), LocalDate.class, LocalDateCodec.INSTANCE),
  BIT("BIT", BitSet.class, BitSetCodec.INSTANCE),
  BOOLEAN(R2dbcType.BOOLEAN.getName(), Boolean.class, BooleanCodec.INSTANCE),
  BYTES("BYTES", byte[].class, ByteArrayCodec.INSTANCE),
  BLOB(R2dbcType.BLOB.getName(), ByteBuffer.class, ByteBufferCodec.INSTANCE),
  VARCHAR(R2dbcType.VARCHAR.getName(), String.class, StringCodec.INSTANCE),
  CLOB(R2dbcType.CLOB.getName(), String.class, StringCodec.INSTANCE),
  BINARY(R2dbcType.BINARY.getName(), Blob.class, BlobCodec.INSTANCE),
  DECIMAL(R2dbcType.DECIMAL.getName(), BigDecimal.class, BigDecimalCodec.INSTANCE);

  private final String typeName;
  private final Class<?> classType;
  private final Codec<?> defaultCodec;

  <T> MariadbType(String typeName, Class<T> classType, Codec<T> defaultCodec) {
    this.typeName = typeName;
    this.classType = classType;
    this.defaultCodec = defaultCodec;
  }

  @Override
  public Class<?> getJavaType() {
    return classType;
  }

  @Override
  public String getName() {
    return typeName;
  }

  public Codec<?> getDefaultCodec() {
    return defaultCodec;
  }
}
