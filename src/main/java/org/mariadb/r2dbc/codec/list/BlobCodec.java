// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.codec.list;

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import org.mariadb.r2dbc.ExceptionFactory;
import org.mariadb.r2dbc.codec.Codec;
import org.mariadb.r2dbc.codec.DataType;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.message.server.ColumnDefinitionPacket;
import org.mariadb.r2dbc.util.BufferUtils;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class BlobCodec implements Codec<Blob> {

  public static final BlobCodec INSTANCE = new BlobCodec();

  private static final EnumSet<DataType> COMPATIBLE_TYPES =
      EnumSet.of(
          DataType.BIT,
          DataType.BLOB,
          DataType.TINYBLOB,
          DataType.MEDIUMBLOB,
          DataType.LONGBLOB,
          DataType.STRING,
          DataType.VARSTRING,
          DataType.TEXT);

  public boolean canDecode(ColumnDefinitionPacket column, Class<?> type) {
    return COMPATIBLE_TYPES.contains(column.getDataType()) && type.isAssignableFrom(Blob.class);
  }

  @Override
  public Blob decodeText(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Blob> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case STRING:
      case TEXT:
      case VARSTRING:
        if (!column.isBinary()) {
          buf.skipBytes(length);
          throw factory.createParsingException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getDataType()));
        }
        return new MariaDbBlob(buf.readRetainedSlice(length));

      default:
        // BIT, TINYBLOB, MEDIUMBLOB, LONGBLOB, BLOB, GEOMETRY
        return new MariaDbBlob(buf.readRetainedSlice(length));
    }
  }

  @Override
  public Blob decodeBinary(
      ByteBuf buf,
      int length,
      ColumnDefinitionPacket column,
      Class<? extends Blob> type,
      ExceptionFactory factory) {
    switch (column.getDataType()) {
      case BIT:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
      case BLOB:
      case GEOMETRY:
        return new MariaDbBlob(buf.readRetainedSlice(length));

      default:
        // STRING, VARCHAR, VARSTRING:
        if (!column.isBinary()) {
          buf.skipBytes(length);
          throw factory.createParsingException(
              String.format(
                  "Data type %s (not binary) cannot be decoded as Blob", column.getDataType()));
        }
        return new MariaDbBlob(buf.readRetainedSlice(length));
    }
  }

  public boolean canEncode(Class<?> value) {
    return Blob.class.isAssignableFrom(value);
  }

  @Override
  public void encodeText(ByteBuf buf, Context context, Object value, ExceptionFactory factory) {
    buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
    Flux.from(((Blob) value).stream())
        .handle(
            (tempVal, sync) -> {
              if (tempVal.hasArray()) {
                BufferUtils.writeEscaped(
                    buf, tempVal.array(), tempVal.arrayOffset(), tempVal.remaining(), context);
              } else {
                byte[] intermediaryBuf = new byte[tempVal.remaining()];
                tempVal.get(intermediaryBuf);
                BufferUtils.writeEscaped(buf, intermediaryBuf, 0, intermediaryBuf.length, context);
              }
              sync.next(buf);
            })
        .doOnComplete(
            () -> {
              buf.writeByte((byte) '\'');
            })
        .subscribe();
  }

  @Override
  public void encodeBinary(ByteBuf buf, Context context, Object value, ExceptionFactory factory) {
    Blob b = (Blob) value;
    buf.writeByte(0xfe);
    int initialPos = buf.writerIndex();
    buf.writerIndex(buf.writerIndex() + 8); // reserve length encoded length bytes

    Flux.from(b.stream())
        .handle(
            (tempVal, sync) -> {
              buf.writeBytes(tempVal);
              sync.next(buf);
            })
        .doOnComplete(
            () -> {
              // Write length
              int endPos = buf.writerIndex();
              buf.writerIndex(initialPos);
              buf.writeLongLE(endPos - (initialPos + 8));
              buf.writerIndex(endPos);
            })
        .subscribe();
  }

  private class MariaDbBlob implements Blob {
    private ByteBuf data;

    public MariaDbBlob(ByteBuf data) {
      this.data = data;
    }

    @Override
    public Publisher<ByteBuffer> stream() {
      return Mono.just(this.data.nioBuffer()).doAfterTerminate(this::discard);
    }

    @Override
    public Publisher<Void> discard() {
      if (data != null) {
        this.data.release();
        this.data = null;
      }
      return Mono.empty();
    }
  }

  public DataType getBinaryEncodeType() {
    return DataType.BLOB;
  }
}
