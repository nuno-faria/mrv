import java.io.PrintWriter;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Monitors the total number of nodes
 */
public class MonitorWorker implements Runnable {

    Config config;
    Connection connection;
    Map<String, List<String>> primaryKeys;


    public MonitorWorker(Config config) throws SQLException {
        this.config = config;
        connection = DriverManager.getConnection(config.connectionString);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        PreparedStatement ps = connection.prepareStatement(
                "SELECT a.attname " +
                "FROM   pg_index i " +
                "JOIN   pg_attribute a ON a.attrelid = i.indrelid " +
                "                     AND a.attnum = ANY(i.indkey) " +
                "WHERE  i.indrelid = ?::regclass " +
                "AND    i.indisprimary;");
        primaryKeys = new HashMap<>();
        for (String table: config.monitorTables) {
            primaryKeys.put(table, new ArrayList<>());
            ps.setString(1, table);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String column = rs.getString(1);
                if (!column.equals("rk")) {
                    primaryKeys.get(table).add(column);
                }
            }
        }
        (new Thread(this)).start();
    }


    String tablePks(String table) {
        return primaryKeys.get(table).stream().collect(Collectors.joining(","));
    }


    public void run() {
        try {
            PrintWriter out = new PrintWriter("monitor" + LocalDateTime.now().toString().replaceAll("[\\-:.]", "") + ".csv");
            out.println("time,table,size,nodes");
            long begin = System.currentTimeMillis();
            Statement s = connection.createStatement();
            while (true) {
                try {
                    for (String table : config.monitorTables) {
                        String pks = tablePks(table);
                        ResultSet rs = s.executeQuery(
                            "SELECT COUNT(*), SUM(t.count) FROM (SELECT COUNT(*) FROM " + table + " GROUP BY " + pks + ") as t");
                        while (rs.next()) {
                            int size = rs.getInt(1);
                            int nodes = rs.getInt(2);
                            out.println(System.currentTimeMillis() - begin + "," + table + "," + size + "," + nodes);
                        }
                    }
                    out.flush();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(config.monitorDelta);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
