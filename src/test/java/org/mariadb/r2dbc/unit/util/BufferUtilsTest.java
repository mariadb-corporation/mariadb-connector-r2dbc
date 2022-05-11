// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2020-2022 MariaDB Corporation Ab

package org.mariadb.r2dbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.r2dbc.spi.IsolationLevel;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.client.SimpleContext;
import org.mariadb.r2dbc.message.Context;
import org.mariadb.r2dbc.util.BufferUtils;
import org.mariadb.r2dbc.util.constants.ServerStatus;

class BufferUtilsTest {

  ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;

  @Test
  void skipLengthEncode() {
    ByteBuf buf = allocator.buffer(1000);
    buf.setBytes(0, new byte[] {0, 1});
    buf.writerIndex(1000);
    BufferUtils.skipLengthEncode(buf);
    assertEquals(1, buf.readByte());

    byte[] b = new byte[500];
    b[0] = (byte) 252;
    b[1] = 1;
    b[2] = 0;
    b[3] = 2;
    b[4] = 3;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    BufferUtils.skipLengthEncode(buf);
    assertEquals(4, buf.readerIndex());

    b = new byte[500];
    b[0] = (byte) 253;
    b[1] = 1;
    b[2] = 0;
    b[3] = 0;
    b[4] = 1;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    BufferUtils.skipLengthEncode(buf);
    assertEquals(5, buf.readerIndex());

    b = new byte[500];
    b[0] = (byte) 254;
    b[1] = 1;
    b[2] = 0;
    b[3] = 0;
    b[4] = 0;
    b[5] = 0;
    b[6] = 0;
    b[7] = 0;
    b[8] = 0;
    b[9] = 1;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    BufferUtils.skipLengthEncode(buf);
    assertEquals(10, buf.readerIndex());
    buf.release();
  }

  @Test
  void readLengthEncodedInt() {
    ByteBuf buf = allocator.buffer(1000);
    buf.setBytes(0, new byte[] {0, 1});
    buf.writerIndex(1000);
    BufferUtils.skipLengthEncode(buf);
    assertEquals(1, buf.readByte());

    byte[] b = new byte[500];
    b[0] = (byte) 252;
    b[1] = 1;
    b[2] = 0;
    b[3] = 2;
    b[4] = 3;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    assertEquals(1, BufferUtils.readLengthEncodedInt(buf));

    b = new byte[500];
    b[0] = (byte) 253;
    b[1] = 1;
    b[2] = 0;
    b[3] = 0;
    b[4] = 1;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    assertEquals(1, BufferUtils.readLengthEncodedInt(buf));

    b = new byte[500];
    b[0] = (byte) 254;
    b[1] = 1;
    b[2] = 0;
    b[3] = 0;
    b[4] = 0;
    b[5] = 0;
    b[6] = 0;
    b[7] = 0;
    b[8] = 0;
    b[9] = 1;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    assertEquals(1, BufferUtils.readLengthEncodedInt(buf));

    b = new byte[500];
    b[0] = (byte) 251;

    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);

    assertEquals(-1, BufferUtils.readLengthEncodedInt(buf));
    buf.release();
  }

  @Test
  void readLengthEncodedString() {
    ByteBuf buf = allocator.buffer(1000);
    buf.setBytes(0, new byte[] {0, 1});
    buf.writerIndex(1000);
    BufferUtils.skipLengthEncode(buf);
    assertEquals(1, buf.readByte());

    byte[] b = new byte[500];
    b[0] = (byte) 251;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);
    assertNull(BufferUtils.readLengthEncodedString(buf));

    b = new byte[500];
    b[0] = (byte) 2;
    b[1] = (byte) 65;
    b[2] = (byte) 66;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);
    assertEquals("AB", BufferUtils.readLengthEncodedString(buf));
    buf.release();
  }

  @Test
  void readLengthEncodedBuffer() {
    ByteBuf buf = allocator.buffer(1000);
    buf.setBytes(0, new byte[] {0, 1});
    buf.writerIndex(1000);
    BufferUtils.skipLengthEncode(buf);
    assertEquals(1, buf.readByte());

    byte[] b = new byte[500];
    b[0] = (byte) 251;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);
    assertNull(BufferUtils.readLengthEncodedBuffer(buf));

    b = new byte[500];
    b[0] = (byte) 2;
    b[1] = (byte) 65;
    b[2] = (byte) 66;
    buf.resetReaderIndex();
    buf.setBytes(0, b);
    buf.writerIndex(1000);
    ByteBuf bb = BufferUtils.readLengthEncodedBuffer(buf);
    byte[] res = new byte[2];
    bb.getBytes(0, res);
    assertArrayEquals("AB".getBytes(StandardCharsets.UTF_8), res);
    buf.release();
  }

  @Test
  void write() {
    Context ctxNoBackSlash =
        new SimpleContext(
            "10.5.5-mariadb",
            1,
            1,
            ServerStatus.NO_BACKSLASH_ESCAPES,
            true,
            1,
            "testr2",
            null,
            IsolationLevel.REPEATABLE_READ);
    Context ctx =
        new SimpleContext(
            "10.5.5-mariadb",
            1,
            1,
            (short) 0,
            true,
            1,
            "testr2",
            null,
            IsolationLevel.REPEATABLE_READ);

    ByteBuf buf = allocator.buffer(1000);
    buf.writerIndex(0);
    byte[] val = "A'\"\0\\€'\"\0\\".getBytes(StandardCharsets.UTF_8);
    BufferUtils.escapedBytes(buf, val, val.length, ctxNoBackSlash);
    byte[] res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals("A''\"\0\\€''\"\0\\".getBytes(StandardCharsets.UTF_8), res);

    buf.writerIndex(0);
    BufferUtils.escapedBytes(buf, val, val.length, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals("A\\'\\\"\\\0\\\\€\\'\\\"\\\0\\\\".getBytes(StandardCharsets.UTF_8), res);

    final byte[] utf8Wrong2bytes = new byte[] {0x08, (byte) 0xFF, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong3bytes =
        new byte[] {0x07, (byte) 0x0a, (byte) 0xff, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong4bytes =
        new byte[] {0x10, (byte) 0x20, (byte) 0x0a, (byte) 0xff, (byte) 0x6F, (byte) 0x6F};
    final byte[] utf8Wrong4bytes2 = new byte[] {-16, (byte) -97, (byte) -103};

    buf.writerIndex(0);
    BufferUtils.escapedBytes(buf, utf8Wrong2bytes, utf8Wrong2bytes.length, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(utf8Wrong2bytes, res);

    buf.writerIndex(0);
    BufferUtils.escapedBytes(buf, utf8Wrong3bytes, utf8Wrong3bytes.length, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(utf8Wrong3bytes, res);

    buf.writerIndex(0);
    BufferUtils.escapedBytes(buf, utf8Wrong4bytes, utf8Wrong4bytes.length, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(utf8Wrong4bytes, res);

    buf.writerIndex(0);
    BufferUtils.escapedBytes(buf, utf8Wrong4bytes2, utf8Wrong4bytes2.length, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(utf8Wrong4bytes2, res);
    buf.release();
  }

  @Test
  void toStringBuf() {
    ByteBuf buf = allocator.buffer(1000);
    buf.setBytes(
        0,
        new byte[] {
          0x6d, 0x00, 0x00, 0x00, 0x0a, 0x35, 0x2e, 0x35, 0x2e, 0x35, 0x2d, 0x31, 0x30, 0x2e, 0x36,
          0x2e
        });
    buf.readerIndex(0);
    buf.writerIndex(16);
    Assertions.assertEquals("6D0000000A352E352E352D31302E362E", BufferUtils.toString(buf));
    buf.release();
  }
}
