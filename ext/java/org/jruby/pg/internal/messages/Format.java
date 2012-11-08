package org.jruby.pg.internal.messages;

public enum Format {
  Binary, Text;

  public int getValue() {
    switch(this) {
    case Binary:
      return 1;
    case Text:
      return 0;
    default:
      throw new IllegalArgumentException("unkonwn format: " + name());
    }
  }
}
