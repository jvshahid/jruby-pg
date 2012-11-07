package org.jruby.pg.internal;

import java.util.List;

import org.jruby.pg.internal.messages.DataRow;
import org.jruby.pg.internal.messages.ErrorResponse;
import org.jruby.pg.internal.messages.RowDescription;

public class ResultSet {
  public static ResultSet EMPTY;
  private RowDescription descrption;
  private List<DataRow> rows;
  private ErrorResponse error;

  public void setDescription(RowDescription descrption) {
    this.descrption = descrption;
  }

  public void appendRow(DataRow row) {
    rows.add(row);
  }

  public void setErrorResponse(ErrorResponse error) {
    this.error = error;
  }
}
