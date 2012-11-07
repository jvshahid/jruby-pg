package org.jruby.pg.internal;

public enum ConnectionState {
  CONNECTION_OK,
  CONNECTION_BAD,
  PGRES_POLLING_READING,
  PGRES_POLLING_WRITING,
  PGRES_POLLING_OK,
  PGRES_POLLING_FAILED,

  // Connecting,
  SendingStartup,
  ReadingAuthentication,
  SendingAuthentication,
  ReadingAuthenticationResponse,
  ReadingParameterStatus,
  ReadingBackendData,
  ReadyForQuery,
  Failed,
  SendingQuery,
  ReadingQueryResponse;
}
