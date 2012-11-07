package org.jruby.pg.internal.messages;

public class AuthenticationMD5Password extends BackendMessage {
  public AuthenticationMD5Password(byte[] salt) {
    if (salt.length != 4)
      throw new IllegalArgumentException("Salt must be a 4 byte array");
  }

  @Override
  public int getLength() {
    return 12;
  }

  @Override
  public MessageType getType() {
    return MessageType.AuthenticationMD5Password;
  }
}
