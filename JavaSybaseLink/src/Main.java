/*
 * The idea is to recive json messages in containing
 * { "msgId" : 1, "sql" : "select * from blar"}   on standard in.
 *
 * Then on standard out send
 * { "msgId" : 1, "rows" : [{},{}]}  back on standard out where the msgId matches the sent message.
 */
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class Main implements SQLRequestListener {

  SybaseDB db;
  StdInputReader input;

  public static void main(String[] args) {
    Main m = new Main();
  }

  public Main() {
    input = new StdInputReader();
    input.addListener(this);

    // blocking call don't do anything under here.
    input.startReadLoop();
  }

  public void connect(ConnectRequest request) {
    JSONObject response = new JSONObject();
    response.put("msgId", request.msgId);
    if (db != null) {
      response.put("result", "previous connection not closed");
    } else {
      MyProperties props = new MyProperties("sybaseConfig.properties");
      db =
        new SybaseDB(
          request.host,
          request.port,
          request.dbname,
          request.username,
          request.password,
          request.charset,
          request.timezone,
          props.properties
        );

      if (!db.connect()) {
        response.put("result", "connect failed");
      } else {
        response.put("result", "connected");
      }
    }
    response.put("javaStartTime", request.javaStartTime);
    long beforeParse = System.currentTimeMillis();
    response.put("javaEndTime", beforeParse);

    System.out.println(response.toJSONString());
  }

  public void sqlRequest(SQLRequest request) {
    db.execSQL(request);
    //System.out.println(result);
  }

  public void close(int msgId) {
    db.close();
    JSONObject response = new JSONObject();
    response.put("msgId", msgId);
    response.put("result", "closed");
    System.out.println(response.toJSONString());
    db = null;
  }
}
