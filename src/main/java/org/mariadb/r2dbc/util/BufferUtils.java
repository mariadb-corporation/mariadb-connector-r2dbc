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

import io.netty.buffer.ByteBuf;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.mariadb.r2dbc.client.ConnectionContext;
import org.mariadb.r2dbc.util.constants.ServerStatus;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.BitSet;

public class BufferUtils {

  private static final byte QUOTE = (byte) '\'';
  private static final byte DBL_QUOTE = (byte) '"';
  private static final byte ZERO_BYTE = (byte) '\0';
  private static final byte BACKSLASH = (byte) '\\';
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
  private static final DateTimeFormatter TIMESTAMP_FORMAT_NO_FRACTIONAL =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  public static int skipLengthEncode(byte[] buf, int pos) {
    int type = buf[pos++] & 0xff;
    switch (type) {
      case 251:
        break;
      case 252:
        pos += 2 + (0xffff & (((buf[pos] & 0xff) + ((buf[pos + 1] & 0xff) << 8))));
        break;
      case 253:
        pos +=
            3
                + (0xffffff
                    & ((buf[pos] & 0xff)
                        + ((buf[pos + 1] & 0xff) << 8)
                        + ((buf[pos + 2] & 0xff) << 16)));
        break;
      case 254:
        pos +=
            8
                + ((buf[pos] & 0xff)
                    + ((long) (buf[pos + 1] & 0xff) << 8)
                    + ((long) (buf[pos + 2] & 0xff) << 16)
                    + ((long) (buf[pos + 3] & 0xff) << 24)
                    + ((long) (buf[pos + 4] & 0xff) << 32)
                    + ((long) (buf[pos + 5] & 0xff) << 40)
                    + ((long) (buf[pos + 6] & 0xff) << 48)
                    + ((long) (buf[pos + 7] & 0xff) << 56));
        break;
      default:
        pos += type;
        break;
    }
    return pos;
  }

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
        return;
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

  public static String readLengthEncodedString(byte[] buf, int pos) {
    int length;
    int type = buf[pos++] & 0xff;
    switch (type) {
      case 251:
        return null;
      case 252:
        length = (0xffff & (((buf[pos++] & 0xff) + ((buf[pos++] & 0xff) << 8))));
        break;
      case 253:
        length =
            (0xffffff
                & ((buf[pos] & 0xff)
                    + ((buf[pos + 1] & 0xff) << 8)
                    + ((buf[pos + 2] & 0xff) << 16)));
        length += 3;
        break;
      case 254:
        length =
            (int)
                (+((buf[pos] & 0xff)
                    + ((long) (buf[pos + 1] & 0xff) << 8)
                    + ((long) (buf[pos + 2] & 0xff) << 16)
                    + ((long) (buf[pos + 3] & 0xff) << 24)
                    + ((long) (buf[pos + 4] & 0xff) << 32)
                    + ((long) (buf[pos + 5] & 0xff) << 40)
                    + ((long) (buf[pos + 6] & 0xff) << 48)
                    + ((long) (buf[pos + 7] & 0xff) << 56)));
        pos += 8;
        break;
      default:
        length = type;
    }

    return new String(buf, pos, length, StandardCharsets.UTF_8);
  }

  public static ByteBuf readLengthEncodedBuffer(ByteBuf buf) {
    int length = (int) readLengthEncodedInt(buf);
    if (length == -1) return null;
    buf.skipBytes(length);
    return buf.slice(buf.readerIndex() - length, length);
  }

  public static void writeLengthEncode(int length, ByteBuf buf) {
    if (length < 251) {
      buf.writeByte((byte) length);
      return;
    }

    if (length < 65536) {
      buf.writeByte((byte) 0xfc);
      buf.writeByte((byte) length);
      buf.writeByte((byte) (length >>> 8));
      return;
    }

    if (length < 16777216) {
      buf.writeByte((byte) 0xfd);
      buf.writeByte((byte) length);
      buf.writeByte((byte) (length >>> 8));
      buf.writeByte((byte) (length >>> 16));
      return;
    }

    buf.writeByte((byte) 0xfe);
    buf.writeByte((byte) length);
    buf.writeByte((byte) (length >>> 8));
    buf.writeByte((byte) (length >>> 16));
    buf.writeByte((byte) (length >>> 24));
    buf.writeByte((byte) (length >>> 32));
    buf.writeByte((byte) (length >>> 40));
    buf.writeByte((byte) (length >>> 48));
    buf.writeByte((byte) (length >>> 54));
  }

  public static void writeLengthEncode(String val, ByteBuf buf) {
    byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
    writeLengthEncode(bytes.length, buf);
    buf.writeBytes(bytes);
  }

  public static void writeAscii(ByteBuf buf, String str) {
    buf.writeCharSequence(str, StandardCharsets.US_ASCII);
  }

  public static void write(ByteBuf buf, LocalDate val) {
    buf.writeByte(QUOTE);
    buf.writeCharSequence(val.format(DateTimeFormatter.ISO_LOCAL_DATE), StandardCharsets.US_ASCII);
    buf.writeByte(QUOTE);
  }

  public static void write(ByteBuf buf, Duration val) {
    long s = val.getSeconds();
    long microSecond = val.getNano() / 1000;
    buf.writeByte(QUOTE);
    if (microSecond != 0) {
      buf.writeCharSequence(
          String.format("%d:%02d:%02d.%06d", s / 3600, (s % 3600) / 60, (s % 60), microSecond),
          StandardCharsets.US_ASCII);
    } else {
      buf.writeCharSequence(
          String.format("%d:%02d:%02d", s / 3600, (s % 3600) / 60, (s % 60)),
          StandardCharsets.US_ASCII);
    }
    buf.writeByte(QUOTE);
  }

  public static void write(ByteBuf buf, LocalTime val) {
    StringBuilder dateString = new StringBuilder(15);
    dateString
        .append(val.getHour() < 10 ? "0" : "")
        .append(val.getHour())
        .append(val.getMinute() < 10 ? ":0" : ":")
        .append(val.getMinute())
        .append(val.getSecond() < 10 ? ":0" : ":")
        .append(val.getSecond());

    int microseconds = val.getNano() / 1000;
    if (microseconds > 0) {
      dateString.append(".");
      if (microseconds % 1000 == 0) {
        dateString.append(Integer.toString(microseconds / 1000 + 1000).substring(1));
      } else {
        dateString.append(Integer.toString(microseconds + 1000000).substring(1));
      }
    }

    buf.ensureWritable(17);
    buf.writeByte(QUOTE);
    buf.writeCharSequence(dateString.toString(), StandardCharsets.US_ASCII);
    buf.writeByte(QUOTE);
  }

  public static void write(ByteBuf buf, LocalDateTime val) {
    buf.writeByte(QUOTE);
    buf.writeCharSequence(
        val.format(val.getNano() != 0 ? TIMESTAMP_FORMAT : TIMESTAMP_FORMAT_NO_FRACTIONAL),
        StandardCharsets.US_ASCII);
    buf.writeByte(QUOTE);
  }

  public static void write(ByteBuf buf, BitSet val) {
    StringBuilder sb = new StringBuilder(val.length() * 8 + 3);
    sb.append("b'");
    int i = val.length();
    while (i-- > 0) {
      sb.append(val.get(i) ? "1" : "0");
    }
    sb.append("'");
    buf.writeCharSequence(sb.toString(), StandardCharsets.US_ASCII);
  }

  public static void write(ByteBuf buf, byte[] val, ConnectionContext context) {
    buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
    writeEscaped(buf, val, 0, val.length, context);
    buf.writeByte(QUOTE);
  }

  public static void write(ByteBuf buffer, Clob val, ConnectionContext context) {
    buffer.writeByte(QUOTE);
    Flux.from(val.stream())
        .handle(
            (tempVal, sync) -> {
              write(buffer, tempVal.toString(), false, true, context);
              sync.next(buffer);
            })
        .subscribe();
    buffer.writeByte(QUOTE);
  }

  public static void write(ByteBuf buffer, Blob val, ConnectionContext context) {
    buffer.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
    Flux.from(val.stream())
        .handle(
            (tempVal, sync) -> {
              if (tempVal.hasArray()) {
                writeEscaped(
                    buffer, tempVal.array(), tempVal.arrayOffset(), tempVal.remaining(), context);
              } else {
                byte[] intermediaryBuf = new byte[tempVal.remaining()];
                tempVal.get(intermediaryBuf);
                writeEscaped(buffer, intermediaryBuf, 0, intermediaryBuf.length, context);
              }
              sync.next(buffer);
            })
        .doOnComplete(
            () -> {
              buffer.writeByte((byte) '\'');
            })
        .subscribe();
  }

  public static void write(ByteBuf buf, InputStream val, ConnectionContext context) {
    try {
      buf.writeBytes("_binary '".getBytes(StandardCharsets.US_ASCII));
      BufferUtils.write(buf, val, true, context);
      buf.writeByte(QUOTE);
    } catch (IOException ioe) {
      throw new R2dbcNonTransientResourceException("Failed to read InputStream", ioe);
    }
  }

  private static void write(ByteBuf buf, InputStream is, boolean escape, ConnectionContext context)
      throws IOException {
    byte[] array = new byte[4096];
    int len;
    if (escape) {
      while ((len = is.read(array)) > 0) {
        writeEscaped(buf, array, 0, len, context);
      }
    } else {
      while ((len = is.read(array)) > 0) {
        buf.writeBytes(array, 0, len);
      }
    }
  }

  public static void writeEscaped(
      ByteBuf buf, byte[] bytes, int offset, int len, ConnectionContext context) {
    buf.ensureWritable(len * 2);
    boolean noBackslashEscapes =
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;

    if (noBackslashEscapes) {
      for (int i = offset; i < len + offset; i++) {
        if (QUOTE == bytes[i]) {
          buf.writeByte(QUOTE);
        }
        buf.writeByte(bytes[i]);
      }
    } else {
      for (int i = offset; i < len + offset; i++) {
        if (bytes[i] == QUOTE
            || bytes[i] == BACKSLASH
            || bytes[i] == '"'
            || bytes[i] == ZERO_BYTE) {
          buf.writeByte(BACKSLASH);
        }
        buf.writeByte(bytes[i]);
      }
    }
  }

  public static ByteBuf write(
      ByteBuf buf, String str, boolean quote, boolean escape, ConnectionContext context) {

    int charsLength = str.length();
    buf.ensureWritable(charsLength * 3 + 2);

    // create UTF-8 byte array
    // since java char are internally using UTF-16 using surrogate's pattern, 4 bytes unicode
    // characters will
    // represent 2 characters : example "\uD83C\uDFA4" = 🎤 unicode 8 "no microphones"
    // so max size is 3 * charLength
    // (escape characters are 1 byte encoded, so length might only be 2 when escape)
    // + 2 for the quotes for text protocol
    int charsOffset = 0;
    char currChar;
    boolean noBackslashEscapes =
        (context.getServerStatus() & ServerStatus.NO_BACKSLASH_ESCAPES) > 0;
    // quick loop if only ASCII chars for faster escape
    if (escape) {
      if (quote) buf.writeByte(QUOTE);
      if (noBackslashEscapes) {
        for (;
            charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
            charsOffset++) {
          if (currChar == QUOTE) {
            buf.writeByte(QUOTE);
          }
          buf.writeByte((byte) currChar);
        }
      } else {
        for (;
            charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
            charsOffset++) {
          if (currChar == BACKSLASH
              || currChar == QUOTE
              || currChar == 0
              || currChar == DBL_QUOTE) {
            buf.writeByte(BACKSLASH);
          }
          buf.writeByte((byte) currChar);
        }
      }
    } else {
      for (;
          charsOffset < charsLength && (currChar = str.charAt(charsOffset)) < 0x80;
          charsOffset++) {
        buf.writeByte((byte) currChar);
      }
    }

    // if quick loop not finished
    while (charsOffset < charsLength) {
      currChar = str.charAt(charsOffset++);
      if (currChar < 0x80) {
        if (escape) {
          if (noBackslashEscapes) {
            if (currChar == QUOTE) {
              buf.writeByte(QUOTE);
            }
          } else if (currChar == BACKSLASH
              || currChar == QUOTE
              || currChar == ZERO_BYTE
              || currChar == DBL_QUOTE) {
            buf.writeByte(BACKSLASH);
          }
        }
        buf.writeByte((byte) currChar);
      } else if (currChar < 0x800) {
        buf.writeByte((byte) (0xc0 | (currChar >> 6)));
        buf.writeByte((byte) (0x80 | (currChar & 0x3f)));
      } else if (currChar >= 0xD800 && currChar < 0xE000) {
        // reserved for surrogate - see https://en.wikipedia.org/wiki/UTF-16
        if (currChar < 0xDC00) {
          // is high surrogate
          if (charsOffset + 1 > charsLength) {
            buf.writeByte((byte) 0x63);
          } else {
            char nextChar = str.charAt(charsOffset);
            if (nextChar >= 0xDC00 && nextChar < 0xE000) {
              // is low surrogate
              int surrogatePairs =
                  ((currChar << 10) + nextChar) + (0x010000 - (0xD800 << 10) - 0xDC00);
              buf.writeByte((byte) (0xf0 | ((surrogatePairs >> 18))));
              buf.writeByte((byte) (0x80 | ((surrogatePairs >> 12) & 0x3f)));
              buf.writeByte((byte) (0x80 | ((surrogatePairs >> 6) & 0x3f)));
              buf.writeByte((byte) (0x80 | (surrogatePairs & 0x3f)));
              charsOffset++;
            } else {
              // must have low surrogate
              buf.writeByte((byte) 0x3f);
            }
          }
        } else {
          // low surrogate without high surrogate before
          buf.writeByte((byte) 0x3f);
        }
      } else {
        buf.writeByte((byte) (0xe0 | ((currChar >> 12))));
        buf.writeByte((byte) (0x80 | ((currChar >> 6) & 0x3f)));
        buf.writeByte((byte) (0x80 | (currChar & 0x3f)));
      }
    }
    if (quote) {
      buf.writeByte(QUOTE);
    }
    return buf;
  }
}
