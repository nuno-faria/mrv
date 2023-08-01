package transactions.mrv;

import transactions.Transactions;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Multiple nodes, each with zero or more stock units
 */
public class TransactionsMrvSQL implements Transactions {

    private Connection connection;
    private static int maxNodes = 128;
    private Random rand;
    private PreparedStatement decrementStock;
    private PreparedStatement incrementStock;
    private PreparedStatement getStock;
    private int id;


    public TransactionsMrvSQL() {
        rand = new Random();
    }


    @Override
    public void setId(int id) {
        this.id = id;
    }


    /**
     * Creates the schema
     */
    private void createSchema(Connection connection, String dbms) {
        try {
            Statement st = connection.createStatement();
            st.execute("CREATE TABLE IF NOT EXISTS Product_Orig (pId varchar(255) PRIMARY KEY)");
            st.execute("CREATE TABLE IF NOT EXISTS Product_Stock (pId varchar(255), rk smallint, stock int, PRIMARY KEY (pId, rk))");
            st.execute("CREATE OR REPLACE VIEW Total_Stock AS SELECT pid, SUM(stock) as total FROM Product_Stock GROUP BY pid");
            if (dbms.equals("postgresql")) {
                st.execute("CREATE TABLE IF NOT EXISTS Product_Tx (pid varchar(255), commits int, aborts int, last_updated timestamp, PRIMARY KEY(pid))");
            }
            else {
                st.execute("CREATE TABLE IF NOT EXISTS Product_Tx (pid varchar(255), commits int, aborts int, last_updated timestamp(3), PRIMARY KEY(pid))");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Creates the procedures
     */
    private void createProcedures(Connection connection, String dbms, int distributeAddsSize) {
        try {
            Statement st = connection.createStatement();
            if (dbms.equals("postgresql")) {
                st.executeUpdate(
                    "CREATE OR REPLACE FUNCTION remove_stock_mrv(amount_ int, pid_ varchar(255), rk_ int) RETURNS bool " +
                    "AS $$ " +
                    "DECLARE node_rk int; " +
                    "        node_stock int; " +
                    "        done bool = False; " +
                    "        cur CURSOR FOR " +
                    "            (SELECT rk, stock " +
                    "            FROM Product_Stock " +
                    "            WHERE pid = pid_ AND rk >= rk_ " +
                    "            ORDER BY RK) " +
                    "            UNION ALL " +
                    "            (SELECT rk, stock " +
                    "            FROM Product_Stock " +
                    "            WHERE pid = pid_ AND rk < rk_ " +
                    "            ORDER BY RK); " +
                    "BEGIN " +
                    "    OPEN cur; " +
                    "    WHILE NOT done AND amount_ > 0 LOOP " +
                    "        FETCH cur INTO node_rk, node_stock; " +
                    "        IF NOT FOUND THEN " +
                    "            done = TRUE; " +
                    "        ELSE " +
                    "            IF node_stock > 0 THEN " +
                    "                UPDATE Product_Stock  " +
                    "                SET stock = stock - LEAST(stock, amount_)  " +
                    "                WHERE pid = pid_ AND rk = node_rk;  " +
                    "                amount_ = amount_ - LEAST(node_stock, amount_);  " +
                    "            END IF; " +
                    "        END IF; " +
                    "    END LOOP; " +
                    "    CLOSE cur; " +
                    "    RETURN amount_ = 0; " +
                    "END " +
                    "$$ LANGUAGE plpgsql;"
                );

                if (distributeAddsSize == 0) {
                    st.executeUpdate(
                        "CREATE OR REPLACE FUNCTION add_stock_mrv(amount_ int, pid_ varchar(255), rk_ int) RETURNS bool " +
                        "AS $$ " +
                        "DECLARE node_rk int; " +
                        "        done bool = FALSE; " +
                        "        cur CURSOR FOR " +
                        "            (SELECT rk " +
                        "            FROM Product_Stock " +
                        "            WHERE pid = pid_ AND rk >= rk_ " +
                        "            ORDER BY RK) " +
                        "            UNION ALL " +
                        "            (SELECT rk " +
                        "            FROM Product_Stock " +
                        "            WHERE pid = pid_ AND rk < rk_ " +
                        "            ORDER BY RK) " +
                        "            LIMIT 1; " +
                        "BEGIN " +
                        "    OPEN cur; " +
                        "    FETCH cur INTO node_rk; " +
                        "    UPDATE Product_Stock " +
                        "    SET stock = stock + amount_ " +
                        "    WHERE pid = pid_ " +
                        "        AND rk = node_rk; " +
                        "    CLOSE cur; " +
                        "    RETURN TRUE; " +
                        "END " +
                        "$$ LANGUAGE plpgsql;"
                    );
                }
                else {
                    // rk not used here
                    st.executeUpdate(
                        "CREATE OR REPLACE FUNCTION add_stock_mrv(amount_ int, pid_ varchar(255), rk_ int) RETURNS bool " +
                        "AS $$ " +
                        "DECLARE delta int; " +
                        "        rks int[]; " +
                        "BEGIN " +
                        "    delta = amount_ / " + distributeAddsSize + "; " +
                        "    SELECT array(( " +
                        "        SELECT rk " +
                        "        FROM Product_Stock " +
                        "        WHERE pid = pid_ " +
                        "        ORDER BY stock, rk ASC " +
                        "        LIMIT " + distributeAddsSize + ")" +
                        "    ) INTO rks; " +
                        "" +
                        "    UPDATE Product_Stock " +
                        "    SET stock = stock + delta " +
                        "    WHERE pid = pid_ " +
                        "        AND rk = ANY(rks); " +
                        "" +
                        "    IF array_length(rks, 1) * delta < amount_ THEN " +
                        "        UPDATE Product_Stock " +
                        "        SET stock = stock + (amount_ - array_length(rks, 1) * delta) " +
                        "        WHERE pid = pid_ " +
                        "            AND rk = rks[1]; " +
                        "    END IF; " +
                        "" +
                        "    RETURN TRUE;  " +
                        "END  " +
                        "$$ LANGUAGE plpgsql;"
                    );
                }
            }
            else if (dbms.equals("mysql") || dbms.equals("mariadb")) {
                st.executeUpdate(
                    "CREATE FUNCTION remove_stock_mrv(amount_ int, pid_ varchar(255), rk_ int) RETURNS bool DETERMINISTIC  " +
                    "BEGIN  " +
                    "    DECLARE done BOOL DEFAULT FALSE; " +
                    "    DECLARE node_rk int; " +
                    "    DECLARE node_stock int; " +
                    "    DECLARE cur CURSOR FOR " +
                    "        (SELECT rk, stock " +
                    "        FROM Product_Stock " +
                    "        WHERE pid = pid_ AND rk >= rk_ " +
                    "        ORDER BY RK) " +
                    "        UNION ALL " +
                    "        (SELECT rk, stock " +
                    "        FROM Product_Stock " +
                    "        WHERE pid = pid_ AND rk < rk_ " +
                    "        ORDER BY RK); " +
                    "    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE; " +
                    "    OPEN cur; " +
                    "    WHILE NOT done AND amount_ > 0 DO " +
                    "        FETCH cur INTO node_rk, node_stock; " +
                    "        IF NOT done THEN " +
                    "           IF node_stock > 0 THEN " +
                    "               UPDATE Product_Stock  " +
                    "               SET stock = stock - LEAST(stock, amount_)  " +
                    "               WHERE pid = pid_ AND rk = node_rk;  " +
                    "               SET amount_ = amount_ - LEAST(node_stock, amount_);  " +
                    "           END IF; " +
                    "        END IF; " +
                    "    END WHILE; " +
                    "    CLOSE cur; " +
                    "    RETURN amount_ = 0; " +
                    "END;"
                );

                st.executeUpdate(
                    "CREATE FUNCTION add_stock_mrv(amount_ int, pid_ varchar(255), rk_ int) RETURNS bool DETERMINISTIC  " +
                    "BEGIN  " +
                    "    DECLARE node_rk int; " +
                    "    DECLARE cur CURSOR FOR " +
                    "        (SELECT rk " +
                    "        FROM Product_Stock " +
                    "        WHERE pid = pid_ AND rk >= rk_ " +
                    "        ORDER BY RK) " +
                    "        UNION ALL " +
                    "        (SELECT rk " +
                    "        FROM Product_Stock " +
                    "        WHERE pid = pid_ AND rk < rk_ " +
                    "        ORDER BY RK) " +
                    "        LIMIT 1; " +
                    "    OPEN cur; " +
                    "    FETCH cur INTO node_rk; " +
                    "    UPDATE Product_Stock  " +
                    "    SET stock = stock + amount_  " +
                    "    WHERE pid = pid_  " +
                    "        AND rk = node_rk; " +
                    "    CLOSE cur; " +
                    "    RETURN true;  " +
                    "END;"
                );
            }
        }
        catch (SQLException e) {
            // procedures already exist
        }
    }


    /**
     * Cleans the database
     */
    private void clean(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        st.execute("DELETE FROM Product_Orig");
        st.execute("DELETE FROM Product_Stock");
        st.execute("DELETE FROM Product_Tx");
    }


    // initialNodes / maxNodes should be an int number
    public void populate(String connectionString, String dbms, int pidLimit, int initialStock,
                         Map<String, Object> extraConfigs) {
        try {
            TransactionsMrvSQL.maxNodes = (int) extraConfigs.get("maxNodes");
            int initialNodes = Math.min((int) extraConfigs.get("initialNodes"), maxNodes);
            int zeroNodesPercentage = (int) extraConfigs.get("zeroNodesPercentage");
            int nodesWithStock = Math.max(1, initialNodes - initialNodes * zeroNodesPercentage / 100);
            int stockPerNode = initialStock / nodesWithStock;
            Connection connection = DriverManager.getConnection(connectionString);
            connection.setAutoCommit(true);

            createSchema(connection, dbms);
            createProcedures(connection, dbms, (int) extraConfigs.get("distributeAddsSize"));
            clean(connection);

            PreparedStatement ps1 = connection.prepareStatement("INSERT INTO Product_Orig (pId) VALUES(?)");
            PreparedStatement ps2 = connection.prepareStatement("INSERT INTO Product_Stock (pid, rk, stock) VALUES(?, ?, ?)");
            connection.setAutoCommit(false);
            Random rand = new Random();

            for (int i = 0; i < pidLimit ; i++) {
                ps1.setString(1, "p" + i);
                ps1.addBatch();
                ps2.setString(1, "p" + i); //pk
                List<Integer> range = IntStream.range(0, maxNodes).boxed().collect(Collectors.toList());
                for (int j = 0; j < initialNodes; j++) {
                ps2.setInt(2, range.remove(rand.nextInt(range.size()))); //rk
                ps2.setInt(3, j < nodesWithStock ? stockPerNode : 0); //stock
                ps2.addBatch();
            }
        }

            ps1.executeBatch();
            ps2.executeBatch();
            connection.commit();

            if (dbms.equals("postgresql")) {
                connection.setAutoCommit(true);
                connection.createStatement().execute("VACUUM ANALYZE");
                connection.setAutoCommit(false);
            }

            connection.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public String getType() {
        return "mrv";
    }


    @Override
    public void setConnection(String connectionString, String dbms, int isolation) throws Exception {
        connection = DriverManager.getConnection(connectionString);
        connection.setTransactionIsolation(isolation);
        decrementStock = connection.prepareStatement("SELECT remove_stock_mrv(?, ?, ?)");
        incrementStock = connection.prepareStatement("SELECT add_stock_mrv(?, ?, ?)");
        getStock = connection.prepareStatement("SELECT total from Total_Stock WHERE pid = ?");
        if (dbms.equals("postgresql")) {
            Statement s = connection.createStatement();
            s.execute("set random_page_cost = 0");
            s.close();
        }
        connection.setAutoCommit(false);
    }


    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private void endTransaction(String pId, boolean committed) {
        MrvWorkersSQL.addTxResult(pId, committed);
    }


    private void endTransaction(List<String> pIds, boolean committed) {
        for (String pId: pIds) {
            MrvWorkersSQL.addTxResult(pId, committed);
        }
    }


    @Override
    public int decrementStock(List<String> pIds, int amount) {
        String p = pIds.get(0);
        try {
            for (String pId: pIds) {
                p = pId;
                int rk = rand.nextInt(maxNodes);
                decrementStock.setInt(1, amount);
                decrementStock.setString(2, pId);
                decrementStock.setInt(3, rk);
                ResultSet rs = decrementStock.executeQuery();
                rs.next();
                boolean result = rs.getBoolean(1);

                if (!result) {
                    connection.rollback();
                    return -1;
                }
            }
            connection.commit();
            endTransaction(pIds, true);
            return 1;
        }
        catch (SQLException e) {
            try {
                connection.rollback();
                endTransaction(p, false);
                return 0;
            }
            catch (SQLException ex) {
                ex.printStackTrace();
                return 0;
            }
        }
    }


    @Override
    public boolean incrementStock(List<String> pIds, int amount) {
        String p = pIds.get(0);
        try {
            for (String pId: pIds) {
                p = pId;
                int rk = rand.nextInt(maxNodes);
                incrementStock.setInt(1, amount);
                incrementStock.setString(2, pId);
                incrementStock.setInt(3, rk);
                incrementStock.execute();
            }
            connection.commit();
            endTransaction(pIds, true);
            return true;
        }
        catch (Exception e) {
            try {
                connection.rollback();
                endTransaction(p, false);
                return false;
            }
            catch (SQLException ex) {
                ex.printStackTrace();
                return false;
            }
        }
    }


    @Override
    public int getStock(String pId) {
        try {
            getStock.setString(1, pId);
            ResultSet rs = getStock.executeQuery();
            rs.next();
            endTransaction(pId, true);
            return rs.getInt(1);
        } 
        catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
