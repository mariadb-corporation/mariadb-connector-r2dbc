// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.util.constants.ServerStatus;

public final class BufferUtils {

  private static final byte QUOTE = (byte) '\'';
  private static final byte DBL_QUOTE = (byte) '"';
  private static final byte ZERO_BYTE = (byte) '\0';
  private static final byte BACKSLASH = (byte) '\\';
  public static final byte[] BINARY_PREFIX = {'_', 'b', 'i', 'n', 'a', 'r', 'y', ' ', '\''};
  public static final byte[] STRING_PREFIX = {'\''};

  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static void skipLengthEncode(ByteBuf buf) {
    short type = buf.readUnsignedByte();
    switch (type) {
      case 252:
        buf.skipBytes(buf.readUnsignedShortLE());
        return;
      case 253:
        buf.skipBytes(buf.readUnsignedMediumLE());
        return;
      case 254:
        buf.skipBytes((int) buf.readLongLE());
        return;
      default:
        buf.skipBytes(type);
    }
  }

  public static long readLengthEncodedInt(ByteBuf buf) {
    short type = buf.readUnsignedByte();
    switch (type) {
      case 251:
        return -1;
      case 252:
        return buf.readUnsignedShortLE();
      case 253:
        return buf.readUnsignedMediumLE();
      case 254:
        return buf.readLongLE();
      default:
        return type;
    }
  }

  public static String readLengthEncodedString(ByteBuf buf) {
    int length = (int) readLengthEncodedInt(buf);
    if (length == -1) return null;
    return buf.readCharSequence(length, StandardCharsets.UTF_8).toString();
  }

  public static ByteBuf readLengthEncodedBuffer(ByteBuf buf) {
    int length = (int) readLengthEncodedInt(buf);
    if (length == -1) return null;
    buf.skipBytes(length);
    return buf.slice(buf.readerIndex() - length, length);
  }

  public static byte[] encodeLength(int length) {
    if (length < 251) {
      return new byte[] {(byte) length};
    }

    if (length < 65536) {
      return new byte[] {(byte) 0xfc, (byte) length, (byte) (length >>> 8)};
    }

    if (length < 16777216) {
      return new byte[] {(byte) 0xfd, (byte) length, (byte) (length >>> 8), (byte) (length >>> 16)};
    }

    return new byte[] {
      (byte) 0xfe,
      (byte) length,
      (byte) (length >>> 8),
      (byte) (length >>> 16),
      (byte) (length >>> 24),
      (byte) (length >>> 32),
      (byte) (length >>> 40),
      (byte) (length >>> 48),
      (byte) (length >>> 54)
    };
  }

  public static void writeLengthEncode(String val, ByteBuf buf) {
    byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
    buf.writeBytes(encodeLength(bytes.length));
    buf.writeBytes(bytes);
  }

  public static ByteBuf encodeByte(ByteBufAllocator allocator, int value) {
    ByteBuf byteBuf = allocator.buffer();
    byteBuf.writeByte(value);
    return byteBuf;
  }

  public static ByteBuf encodeAscii(ByteBufAllocator allocator, String value) {
    ByteBuf byteBuf = allocator.buffer();
    byteBuf.writeCharSequence(value, StandardCharsets.US_ASCII);
    return byteBuf;
  }

  public static ByteBuf encodeLengthAscii(ByteBufAllocator allocator, String value) {
    int len = value.length();
    ByteBuf byteBuf = allocator.buffer(len + 9);
    byteBuf.writeBytes(encodeLength(value.length()));
    byteBuf.writeCharSequence(value, StandardCharsets.US_ASCII);
    return byteBuf;
  }

  public static ByteBuf encodeLengthUtf8(ByteBufAllocator allocator, String value) {
    byte[] b = value.getBytes(StandardCharsets.UTF_8);
    CompositeByteBuf byteBuf = allocator.compositeBuffer();
    byteBuf.addComponent(true, Unpooled.wrappedBuffer(encodeLength(b.length)));
    byteBuf.addComponent(true, Unpooled.wrappedBuffer(b));
    return byteBuf;
  }

  public static ByteBuf encodeLengthBytes(ByteBufAllocator allocator, byte[] value) {
    CompositeByteBuf byteBuf = allocator.compositeBuffer();
    byteBuf.addComponent(true, Unpooled.wrappedBuffer(encodeLength(value.length)));
    byteBuf.addComponent(true, Unpooled.wrappedBuffer(value));
    return byteBuf;
  }

  public static ByteBuf encodeEscapedBuffer(
      ByteBufAllocator allocator, ByteBuf value, Context context) {
    ByteBuf buf = allocator.buffer(value.readableBytes() * 2);
    buf.writeBytes(BINARY_PREFIX);
    boolean noBackslashEscapes =
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;

    int fromIndex = value.readerIndex();
    int toIndex = value.writerIndex();
    if (noBackslashEscapes) {
      while (true) {
        int nextPos = value.indexOf(fromIndex, toIndex, QUOTE);
        if (nextPos >= 0) {
          buf.writeBytes(value, fromIndex, nextPos - fromIndex);
          buf.writeByte(QUOTE);
          buf.writeByte(QUOTE);
          fromIndex = nextPos + 1;
        } else {
          buf.writeBytes(value, fromIndex, toIndex);
          break;
        }
      }
    } else {
      ByteProcessor processor =
          b -> (b != QUOTE && b != BACKSLASH && b != (byte) '"' && b != ZERO_BYTE);

      while (true) {
        int nextPos = value.forEachByte(fromIndex, toIndex - fromIndex, processor);
        if (nextPos == -1) {
          buf.writeBytes(value, fromIndex, toIndex - fromIndex);
          break;
        }
        buf.writeBytes(value, fromIndex, nextPos - fromIndex);
        buf.writeByte(BACKSLASH);
        buf.writeByte(value.getByte(nextPos));
        fromIndex = nextPos + 1;
      }
    }
    buf.writeByte('\'');
    return buf;
  }

  public static ByteBuf encodeEscapedBytes(
      ByteBufAllocator allocator, byte[] prefix, byte[] value, Context context) {
    ByteBuf stBuf = Unpooled.wrappedBuffer(value);
    ByteBuf buf = allocator.buffer(stBuf.readableBytes() + 10);
    buf.writeBytes(prefix);
    escapedBytes(buf, value, value.length, context);
    buf.writeByte('\'');
    return buf;
  }

  public static void escapedBytes(ByteBuf buf, byte[] value, int len, Context context) {
    ByteBuf stBuf = Unpooled.wrappedBuffer(value, 0, len);
    boolean noBackslashEscapes =
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;

    int fromIndex = stBuf.readerIndex();
    int toIndex = stBuf.writerIndex();
    if (noBackslashEscapes) {
      while (true) {
        int nextPos = stBuf.indexOf(fromIndex, toIndex, QUOTE);
        if (nextPos >= 0) {
          buf.writeBytes(stBuf, fromIndex, nextPos - fromIndex);
          buf.writeByte(QUOTE);
          buf.writeByte(QUOTE);
          fromIndex = nextPos + 1;
        } else {
          buf.writeBytes(stBuf, fromIndex, toIndex - fromIndex);
          break;
        }
      }
    } else {
      ByteProcessor processor =
          b -> (b != QUOTE && b != BACKSLASH && b != (byte) '"' && b != ZERO_BYTE);

      while (true) {
        int nextPos = stBuf.forEachByte(fromIndex, toIndex - fromIndex, processor);
        if (nextPos == -1) {
          buf.writeBytes(stBuf, fromIndex, toIndex - fromIndex);
          break;
        }
        buf.writeBytes(stBuf, fromIndex, nextPos - fromIndex);
        buf.writeByte(BACKSLASH);
        buf.writeByte(stBuf.getByte(nextPos));
        fromIndex = nextPos + 1;
      }
    }
  }

  public static String toString(ByteBuf packet) {
    char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[packet.readableBytes() * 2];
    int j = 0;
    while (packet.readableBytes() > 0) {
      int v = packet.readByte() & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
      j++;
    }
    return new String(hexChars);
  }
}
