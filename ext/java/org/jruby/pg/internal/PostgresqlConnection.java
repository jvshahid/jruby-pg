 package org.jruby.pg.internal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.jruby.pg.internal.messages.BackendKeyData;
import org.jruby.pg.internal.messages.DataRow;
import org.jruby.pg.internal.messages.ErrorResponse;
import org.jruby.pg.internal.messages.ParameterStatus;
import org.jruby.pg.internal.messages.ProtocolMessage;
import org.jruby.pg.internal.messages.ProtocolMessageBuffer;
import org.jruby.pg.internal.messages.Query;
import org.jruby.pg.internal.messages.RowDescription;
import org.jruby.pg.internal.messages.Startup;

import static org.jruby.pg.internal.PostgresqlConnectionUtils.*;

public class PostgresqlConnection {

  /** Static API (create a new connection) **/
  public static PostgresqlConnection connectDb(Properties props) throws Exception {
    PostgresqlConnection connection = new PostgresqlConnection(props);
    connection.connect();
    return connection;
  }

  public static PostgresqlConnection connectStart(Properties props) throws IOException {
    PostgresqlConnection connection = new PostgresqlConnection(props);
    connection.connectAsync();
    return connection;
  }

  /** Public API (execute, close, etc.) **/

  public ConnectionState status() {
    return translateState();
  }

  public void consumeInput() throws IOException {
    connectPoll();
  }

  public ConnectionState connectPoll() throws IOException {
    System.out.println("state: " + state.name());
    try {
      switch(state) {
      case SendingStartup:
      case SendingAuthentication:
      case SendingQuery:
        flush();
        break;
      case ReadingAuthentication:
      case ReadingAuthenticationResponse:
      case ReadingBackendData:
      case ReadingParameterStatus:
        socket.read(currentInMessage.getBuffer());
        break;
      case ReadyForQuery:
        break;
      default:
        throw new IllegalStateException("Unknown state: " + state.name());
      }
    } catch (IOException e) {
      state = ConnectionState.Failed;
      throw e;
    }
    changeState();
    return translateStateForPolling();
  }

  public ResultSet exec(String query) throws IOException {
    sendQuery(query);
    ResultSet prevResult, result;
    prevResult = result = null;
    while ((result = getResult()) != null) {
      prevResult = result;
    }
    return prevResult;
  }

  public void sendQuery(String query) throws IOException {
    if (state != ConnectionState.ReadyForQuery)
      throw new IllegalStateException("Connection isn't ready for a new query");

    currentOutMessage = new Query(query);
    currentOutBuffer = currentInMessage.getBuffer();
    state = ConnectionState.SendingQuery;

    socket.configureBlocking(nonBlocking);
    socket.write(currentOutBuffer);
    changeState();
  }

  public SocketChannel getSocket() {
    return socket;
  }

  public void close() {
  }

  public void setNonBlocking(boolean nonBlocking) {
    this.nonBlocking = nonBlocking;
  }

  public boolean isNonBlocking() {
    return nonBlocking;
  }

  public boolean isBusy() {
    if (state == ConnectionState.SendingQuery ||
         state == ConnectionState.ReadingQueryResponse) {
      return lastResultSet == null;
    }
    return false;
  }

  public ResultSet getResult() throws IOException {
    if (state == ConnectionState.ReadyForQuery)
      return null;

    if (!isBusy()) {
      ResultSet temp = lastResultSet;
      lastResultSet = null;
      return temp;
    }

    // send all the data in the output buffer
    Selector selector = Selector.open();
    socket.register(selector, SelectionKey.OP_WRITE);
    while (!flush()) {
      selector.select();
    }
    selector.keys().clear();

    socket.register(selector, SelectionKey.OP_READ);
    while (isBusy()) {
      selector.select();
      consumeInput();
    }
    return getResult();
  }

  public boolean flush() throws IOException {
    if (currentOutBuffer.remaining() == 0)
      return true;
    socket.write(currentOutBuffer);
    return currentInMessage.remaining() == 0;
  }

  /** the shitty implementation the makes the connection ticks **/

  private PostgresqlConnection(Properties props) {
    this.host = host(props);
    this.port = port(props);
    this.user = user(props);
    this.dbname = dbname(props);
  }

  private void connect() throws Exception {
    connectAsync();
    Selector selector = Selector.open();
    while (status() != ConnectionState.CONNECTION_BAD &&
        status() != ConnectionState.CONNECTION_OK) {
      // do connection poll
      ConnectionState pollState = connectPoll();
      if (pollState == ConnectionState.PGRES_POLLING_WRITING) {
        socket.register(selector, SelectionKey.OP_WRITE);
        selector.select();
      } else {
        socket.register(selector, SelectionKey.OP_READ);
        selector.select();
      }
      selector.keys().clear();
    }
  }

  private void connectAsync() throws IOException {
    try {
      socket = SocketChannel.open();
      socket.configureBlocking(true);
      socket.connect(new InetSocketAddress(host, port));

      // send startup message
      socket.configureBlocking(false);
      state = ConnectionState.SendingStartup;
      currentOutMessage = new Startup(user, dbname, new Properties());
      currentOutBuffer = currentOutMessage.toBytes();
      currentInMessage = new ProtocolMessageBuffer();
    } catch (IOException e) {
      state = ConnectionState.Failed;
      throw e;
    }
  }

  private void changeState() {
    System.out.println("remaining: " + currentInMessage.remaining());
    switch (state) {
    case SendingStartup:
      if (currentOutBuffer.remaining() == 0)
        state = ConnectionState.ReadingAuthentication;
    case SendingAuthentication:
      if (currentOutBuffer.remaining() == 0)
        state = ConnectionState.ReadingAuthenticationResponse;
      break;
    case SendingQuery:
      if (currentOutBuffer.remaining() == 0)
        state = ConnectionState.ReadingQueryResponse;
      break;
    case ReadingAuthentication:
    case ReadingAuthenticationResponse:
      if (currentInMessage.remaining() == 0) {
        processAuthentication();
        currentInMessage = new ProtocolMessageBuffer();
      }
      break;
    case ReadingBackendData:
    case ReadingParameterStatus:
      if (currentInMessage.remaining() == 0) {
        processParameterStatusAndBackend();
        currentInMessage = new ProtocolMessageBuffer();
      }
      break;
    case ReadingQueryResponse:
      if (currentInMessage.remaining() == 0) {
        processQueryResponse();
        currentInMessage = new ProtocolMessageBuffer();
      }
    default:
      // don't do anything
      break;
    }
  }

  private void processQueryResponse() {
    ProtocolMessage message = currentInMessage.getMessage();
    switch(message.getType()) {
    case CopyInResponse:
    case CopyOutResponse:
      throw new UnsupportedOperationException("Copy operations isn't supported yet");
    case CommandComplete:
      lastResultSet = inProgress;
      inProgress = null;
      break;
    case EmptyQueryResponse:
      lastResultSet = ResultSet.EMPTY;
      inProgress = null;
      break;
    case RowDescription:
      inProgress = new ResultSet();
      // fetch the row description
      RowDescription description = (RowDescription) message;
      inProgress.setDescription(description);
      break;
    case DataRow:
      DataRow dataRow = (DataRow) message;
      inProgress.appendRow(dataRow);
      break;
    case NoticeResponse:
      break;
    case ErrorResponse:
      ErrorResponse error = (ErrorResponse) message;
      inProgress.setErrorResponse(error);
      lastResultSet = inProgress;
      inProgress = null;
      break;
    case ReadyForQuery:
      state = ConnectionState.ReadyForQuery;
      break;
    }
  }

  private void processParameterStatusAndBackend() {
    ProtocolMessage message = currentInMessage.getMessage();
    switch(message.getType()) {
    case ErrorResponse:
      state = ConnectionState.Failed;
      break;
    case ParameterStatus:
      ParameterStatus parameterStatus = (ParameterStatus) message;
      parameterValues.put(parameterStatus.getName(), parameterStatus.getValue());
      System.out.println("current parameter values: " + parameterValues);
      break;
    case BackendKeyData:
      backendKeyData = (BackendKeyData) message;
      System.out.println("pid: " + backendKeyData.getPid() + ", secret: " + backendKeyData.getSecret());
    case ReadyForQuery:
      state = ConnectionState.ReadyForQuery;
      break;
    default:
      throw new IllegalArgumentException("Received : " + message.getType().name());
    }
  }

  private void processAuthentication() {
    ProtocolMessage message = currentInMessage.getMessage();
    switch (message.getType()) {
    case ErrorResponse:
      state = ConnectionState.Failed;
      break;
    case AuthenticationOk:
      state = ConnectionState.ReadingBackendData;
      break;
    default:
      throw new IllegalArgumentException("Received : " + message.getType().name());
    }
  }

  private ConnectionState translateStateForPolling() {
    switch(state) {
    case SendingStartup:
    case SendingAuthentication:
      return ConnectionState.PGRES_POLLING_WRITING;
    case ReadingAuthentication:
    case ReadingAuthenticationResponse:
    case ReadingBackendData:
    case ReadingParameterStatus:
      return ConnectionState.PGRES_POLLING_READING;
    case ReadyForQuery:
      return ConnectionState.PGRES_POLLING_OK;
    case Failed:
      return ConnectionState.PGRES_POLLING_FAILED;
    default:
      throw new IllegalStateException("Unkown state: " + state.name());
    }
  }

  private ConnectionState translateState() {
    System.out.println("current state: " + state.name());
    switch(state) {
    case Failed:
      return ConnectionState.CONNECTION_BAD;
    default:
      return ConnectionState.CONNECTION_OK;
    }
  }

  private final String host;
  private final String dbname;
  private final int port;
  private final String user;
  private boolean nonBlocking = false;

  private ResultSet inProgress;
  private ResultSet lastResultSet;
  private SocketChannel socket;
  private ConnectionState state = ConnectionState.CONNECTION_BAD;
  private ProtocolMessage currentOutMessage;
  private ByteBuffer currentOutBuffer;
  private ProtocolMessageBuffer currentInMessage;
  private final Map<String, String> parameterValues = new HashMap<String, String>();
  private BackendKeyData backendKeyData;
}
