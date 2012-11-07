package org.jruby.pg.internal.messages;

import java.nio.ByteBuffer;

public class CopyData extends ProtocolMessage {
  private final byte[] bytes;

  public CopyData(byte[] bytes) {
    this.bytes = new byte[bytes.length + 5];
    this.bytes[0] = 'd';
    System.arraycopy(bytes, 0, this.bytes, 5, bytes.length);
    ByteUtils.fixLength(this.bytes);
  }

  @Override
  public int getLength() {
    return bytes.length - 1;
  }

  @Override
  public MessageType getType() {
    return MessageType.CopyData;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(bytes);
  }
}
