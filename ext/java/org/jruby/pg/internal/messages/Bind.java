package org.jruby.pg.internal.messages;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class Bind extends ProtocolMessage {
  private final String destinationPortal;
  private final String sourceStatement;
  private final Parameter[] params;
  private final Format format;
  private final byte[] bytes;
  private final int length;

  public Bind(String destinationPortal, String sourceStatement, Parameter[] params, Format format) {
    this.destinationPortal = destinationPortal;
    this.sourceStatement = sourceStatement;
    this.params = params;
    this.format = format;

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      out.write('b');
      out.write(new byte[4]);
      ByteUtils.writeString(out, destinationPortal);
      ByteUtils.writeString(out, sourceStatement);
      ByteUtils.writeInt2(out, params.length);
      for (Parameter parameter : params) {
        parameter.writeFormat(out);
      }
      ByteUtils.writeInt2(out, params.length);
      for (Parameter parameter : params) {
        parameter.writeValue(out);
      }
      ByteUtils.writeInt2(out, 1);
      out.write(format.getValue());
    } catch (Exception ex) {
      // we cannot be here
    }

    byte[] bytes = out.toByteArray();
    ByteUtils.fixLength(bytes);
    this.bytes = bytes;
    this.length = bytes.length - 1;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public MessageType getType() {
    return MessageType.Bind;
  }

  @Override
  public ByteBuffer toBytes() {
    return ByteBuffer.wrap(bytes);
  }

  public String getDestinationPortal() {
    return destinationPortal;
  }

  public String getSourceStatement() {
    return sourceStatement;
  }

  public Parameter[] getParams() {
    return params;
  }

  public Format getFormat() {
    return format;
  }
}
