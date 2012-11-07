package org.jruby.pg.internal.messages;

import java.util.Map;

public class ErrorResponse extends BackendMessage {
  private final Map<Byte, String> fields;
  private final int length;

  // the first byte of each array element is the code followed by the value
  public ErrorResponse(Map<Byte, String> fields, int length) {
    this.fields = fields;
    this.length = length;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public MessageType getType() {
    return MessageType.ErrorResponse;
  }

  public Map<Byte, String> getFields() {
    return fields;
  }
}
