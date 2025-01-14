import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

/**
 *
 * @author rod
 */
public class ExecSQLCallable implements Callable<String> {

  private Connection conn;
  //private Gson gson;
  private DateFormat df;
  private SQLRequest request;

  public ExecSQLCallable(Connection conn, DateFormat df, SQLRequest request) {
    this.conn = conn;
    this.df = df;
    this.request = request;
    //gson = new GsonBuilder().create();
  }

  public String call() throws Exception {
    String result = execSQLJsonSimple();
    System.out.println(result);
    //safePrintln(result);
    return result;
  }

  public String execSQLJsonSimple() {
    JSONObject response = new JSONObject();
    response.put("msgId", request.msgId);
    response.put("dbId", request.dbId);
    JSONArray rss = new JSONArray();
    response.put("result", rss);

    try {
      Statement stmt = conn.createStatement();
      boolean isRS = stmt.execute(request.sql);
      while (isRS || (stmt.getUpdateCount() != -1)) {
        if (!isRS) {
          isRS = stmt.getMoreResults();
          continue;
        }
        ResultSet rs = stmt.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();

        // get column names;
        int colCount = meta.getColumnCount();
        String[] columns = new String[colCount + 1];
        for (int c = 1; c < colCount + 1; c++) {
          String col = meta.getColumnLabel(c);
          if (col.equals("")) {
            col = "col" + c;
          }
          columns[c] = col;
        }

        JSONArray jsonRS = new JSONArray();
        rss.add(jsonRS);
        while (rs.next()) {
          Map row = new LinkedHashMap();
          for (int c = 1; c < colCount + 1; c++) {
            // Object val = rs.getObject(c);
            // if (val == null) continue;

            int dataType = meta.getColumnType(c);
            switch (dataType) {
              case SybaseDB.TYPE_TIME_STAMP:
              case SybaseDB.TYPE_DATE:
                Timestamp sqlTimestamp = rs.getTimestamp(c);
                if (sqlTimestamp != null) {
                  String my8601formattedDate = df.format(
                    new Date(rs.getTimestamp(c).getTime())
                  );
                  row.put(columns[c], my8601formattedDate);
                } else {
                  row.put(columns[c], null);
                }
                break;
              default:
                row.put(columns[c], rs.getObject(c));
            }
            //System.out.println(columns[c] + ": " + dataType);
          }
          jsonRS.add(row);
        }
        rs.close();
        isRS = stmt.getMoreResults();
      }
      stmt.close();
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      response.put("error", ex.getMessage());
    }

    response.put("javaStartTime", request.javaStartTime);
    long beforeParse = System.currentTimeMillis();
    response.put("javaEndTime", beforeParse);

    String jsonResult = response.toJSONString();
    //System.out.println("parse time: " + (System.currentTimeMillis() - beforeParse));
    return jsonResult;
  }

  public void safePrintln(String s) {
    synchronized (System.out) {
      System.out.println(s);
    }
  }
}
