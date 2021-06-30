package org.mariadb.r2dbc.unit.util;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mariadb.r2dbc.client.Context;
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
  }

  @Test
  void write() {
    Context ctxNoBackSlash =
        new Context("10.5.5-mariadb", 1, null, 1, ServerStatus.NO_BACKSLASH_ESCAPES, true);
    Context ctx = new Context("10.5.5-mariadb", 1, null, 1, (short) 0, true);

    ByteBuf buf = allocator.buffer(1000);
    buf.writerIndex(0);
    BufferUtils.write(buf, "A'\"\0\\€'\"\0\\", false, ctxNoBackSlash);
    byte[] res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals("A''\"\0\\€''\"\0\\".getBytes(StandardCharsets.UTF_8), res);

    buf.writerIndex(0);
    BufferUtils.write(buf, "A'\"\0\\€'\"\0\\", false, ctx);
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
    BufferUtils.write(buf, new String(utf8Wrong2bytes, StandardCharsets.UTF_8), false, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(new byte[] {8, -17, -65, -67, 111, 111}, res);

    buf.writerIndex(0);
    BufferUtils.write(buf, new String(utf8Wrong3bytes, StandardCharsets.UTF_8), false, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(new byte[] {7, 10, -17, -65, -67, 111, 111}, res);

    buf.writerIndex(0);
    BufferUtils.write(buf, new String(utf8Wrong4bytes, StandardCharsets.UTF_8), false, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(new byte[] {16, 32, 10, -17, -65, -67, 111, 111}, res);

    buf.writerIndex(0);
    BufferUtils.write(buf, new String(utf8Wrong4bytes2, StandardCharsets.UTF_8), false, ctx);
    res = new byte[buf.writerIndex()];
    buf.getBytes(0, res);
    assertArrayEquals(new byte[] {-17, -65, -67}, res);
  }
}
