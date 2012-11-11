package org.jruby.pg.internal.messsages;

import java.nio.ByteBuffer;

import org.jruby.pg.internal.messages.ProtocolMessage;

public class Sync extends ProtocolMessage {
  private final byte[] bytes = {'S', 0, 0, 0, 4};

  @Override
  public int getLength() {
    return 4;
  }

  @Override
  public MessageType getType() {
    return MessageType.Sync;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(bytes);
  }
}
