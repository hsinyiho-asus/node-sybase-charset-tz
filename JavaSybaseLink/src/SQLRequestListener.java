import java.util.HashMap;

/**
 *
 * @author rod
 */
interface SQLRequestListener {
  void connect(ConnectRequest request);
  void sqlRequest(SQLRequest request);
  void close(int msgId);
}
