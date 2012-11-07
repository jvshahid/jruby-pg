package org.jruby.pg.internal.messages;

import java.io.OutputStream;

public abstract class Parameter {
  public abstract void writeValue(OutputStream out);
  public abstract void writeFormat(OutputStream out);
}
