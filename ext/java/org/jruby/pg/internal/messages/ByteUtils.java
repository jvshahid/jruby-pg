package org.jruby.pg.internal.messages;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteUtils {
  public static void writeInt4(OutputStream out, int value) throws IOException {
    out.write(value >> 24);
    out.write(value >> 16);
    out.write(value >> 8);
    out.write(value);
  }

  public static void writeInt2(OutputStream out, int value) throws IOException {
    out.write(value >> 8);
    out.write(value);
  }

  public static ByteBuffer getNullTerminatedBytes(ByteBuffer buffer) {
    return getNullTerminatedBytes(buffer, 0);
  }

  public static ByteBuffer getNullTerminatedBytes(ByteBuffer buffer, int offset) {
    ByteBuffer newBuffer = buffer.duplicate();
    byte[] bytes = newBuffer.array();
    int i;
    for (i = offset; i < newBuffer.limit(); i++)
      if (bytes[i] == '\0')
        break;
    newBuffer.position(offset);
    newBuffer.limit(i);
    buffer.position(i + 1);	// skip the null byte
    return newBuffer;
  }

  public static String byteBufferToString(ByteBuffer buffer) {
    int position = buffer.position();
    int limit = buffer.limit();
    return new String(buffer.array(), position, limit - position);
  }

  public static void fixLength(byte[] bytes) {
    fixLength(bytes, 1);
  }

  public static void fixLength(byte[] bytes, int offset) {
    bytes[offset]     = (byte) (bytes.length >> 24);
    bytes[offset + 1] = (byte) (bytes.length >> 16);
    bytes[offset + 2] = (byte) (bytes.length >> 8);
    bytes[offset + 3] = (byte) (bytes.length);
  }

  public static void writeString(OutputStream out, String name) throws IOException {
    out.write(name.getBytes());
    out.write('\0');
  }
}
