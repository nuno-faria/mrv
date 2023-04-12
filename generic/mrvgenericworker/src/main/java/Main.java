import java.sql.*;


/**
 * Main
 */
public class Main {

    static Config config;

    static void init() {
        try {
            Connection connection = DriverManager.getConnection(config.connectionString);
            Statement s = connection.createStatement();
            s.execute("CREATE TABLE " + config.statusTable + " (table_name varchar, column_name varchar, pk varchar, commits int, aborts int, last_updated timestamp, pk_sql varchar, primary key(table_name, column_name, pk))");
            connection.close();
        }
        catch (Exception ignored) { }
    }


    public static void main(String[] args) throws Exception {
        config = Config.readConfig("src/main/resources/config.yml");
        init();
        if (config.adjustWorkers > 0) {
            new AdjustWorker(config);
        }
        if (config.balanceWorkers > 0) {
            new BalanceWorker(config);
        }
        if (config.monitor) {
            new MonitorWorker(config);
        }
    }
}
