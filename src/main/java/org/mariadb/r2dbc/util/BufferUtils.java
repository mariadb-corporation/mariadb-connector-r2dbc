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
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import org.mariadb.r2dbc.client.Context;
import org.mariadb.r2dbc.util.constants.ServerStatus;

public final class BufferUtils {

  private static final byte QUOTE = (byte) '\'';
  private static final byte DBL_QUOTE = (byte) '"';
  private static final byte ZERO_BYTE = (byte) '\0';
  private static final byte BACKSLASH = (byte) '\\';
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

  public static void writeEscaped(ByteBuf buf, byte[] bytes, int offset, int len, Context context) {
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

  public static ByteBuf write(ByteBuf buf, String str, boolean quote, Context context) {

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
        if (currChar == BACKSLASH || currChar == QUOTE || currChar == 0 || currChar == DBL_QUOTE) {
          buf.writeByte(BACKSLASH);
        }
        buf.writeByte((byte) currChar);
      }
    }

    // if quick loop not finished
    while (charsOffset < charsLength) {
      currChar = str.charAt(charsOffset++);
      if (currChar < 0x80) {
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
