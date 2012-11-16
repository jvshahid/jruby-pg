package org.jruby.pg.internal.messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class CopyData extends ProtocolMessage {
  private final ByteBuffer value;

  public CopyData(ByteBuffer value) {
    this.value = value;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public MessageType getType() {
    return MessageType.CopyData;
  }

  @Override
  public ByteBuffer toBytes() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      out.write('d');
      out.write(getValue().remaining() + 4);
      out.write(getValue().array(), getValue().arrayOffset() + getValue().position(), getValue().remaining());
    } catch (Exception e) {
      // we cannot be here
    }
    return ByteBuffer.wrap(out.toByteArray());
  }

  public ByteBuffer getValue() {
    return value;
  }
}
