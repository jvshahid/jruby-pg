package org.jruby.pg.internal.messages;

public class CommandComplete extends BackendMessage {

  private final String tag;

  public CommandComplete(String tag) {
    this.tag = tag;
  }

  @Override
  public int getLength() {
    return 4 + tag.getBytes().length;
  }

  @Override
  public MessageType getType() {
    return MessageType.CommandComplete;
  }

  public String getTag() {
    return tag;
  }
}
