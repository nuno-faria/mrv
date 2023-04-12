package transactions.phaseReconciliation;

import transactions.Transactions;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * One entry per product
 */
public class TransactionsPhaseReconciliation implements Transactions {

    private Connection connection;
    private PreparedStatement updateStockJoined;
    private PreparedStatement updateStockSplit;
    private PreparedStatement getStock;
    private int id;

    @Override
    public void setId(int id) {
        this.id = id;
        try {
            updateStockSplit.setInt(3, id);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }


    /**
     * Creates the schema
     */
    private void createSchema(Connection connection, String dbms) {
        try {
            Statement st = connection.createStatement();
            st.execute("CREATE TABLE IF NOT EXISTS Product (pId varchar(255) PRIMARY KEY, stock int)");
            st.execute("CREATE TABLE IF NOT EXISTS Product_Split_Stock (pId varchar(255), core_id int, stock int, PRIMARY KEY (pId, core_id))");
            st.execute("CREATE TABLE IF NOT EXISTS Product_Stats (pid varchar(255) PRIMARY KEY, commits int, aborts int, no_stock int)");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Create Procedures (mysql and mariadb don't support update with returns)
     */
    private void createProcedures(Connection connection, String dbms) {
        try {
            Statement st = connection.createStatement();
            if (dbms.equals("mysql") || dbms.equals("mariadb")) {
                st.executeUpdate("CREATE PROCEDURE update_stock_joined(amount int, pId varchar(255)) " + "BEGIN "
                        + "UPDATE Product " + "SET stock = @stock := stock + amount " + "WHERE Product.pId = pId; "
                        + "SELECT @stock; " + "END;");
                st.executeUpdate("CREATE PROCEDURE update_stock_split(amount int, core_id int, pId varchar(255)) " + "BEGIN "
                        + "UPDATE Product_Split_Stock " + "SET stock = @stock := stock + amount "
                        + "WHERE Product_Split_Stock.pId = pId AND Product_Split_Stock.core_id = core_id; "
                        + "SELECT @stock; " + "END;");
            }
        } catch (Exception e) {
            // procedure already exists
        }
    }


    /**
     * Cleans the database
     */
    private void clean(Connection connection) throws SQLException {
        Statement st = connection.createStatement();
        st.execute("DELETE FROM Product");
        st.execute("DELETE FROM Product_Split_Stock");
        st.execute("DELETE FROM Product_Stats");
    }


    @Override
    public void populate(String connectionString, String dbms, int pidLimit, int initialStock,
                         Map<String, Object> extraConfigs) {
        try {
            int cores = (int) extraConfigs.get("cores");
            Connection connection = DriverManager.getConnection(connectionString);
            connection.setAutoCommit(true);

            createSchema(connection, dbms);
            createProcedures(connection, dbms);
            clean(connection);

            PreparedStatement ps1 = connection.prepareStatement("INSERT INTO Product (pId, stock) VALUES(?, ?)");
            PreparedStatement ps2 = connection.prepareStatement("INSERT INTO Product_Split_Stock (pId, core_id, stock) VALUES(?, ?, ?)");
            connection.setAutoCommit(false);

            for (int i = 0; i < pidLimit; i++) {
                ps1.setString(1, "p" + i);
                ps1.setInt(2, initialStock);
                ps1.addBatch();
                for (int j = 0; j < cores; j++) {
                    ps2.setString(1, "p" + i);
                    ps2.setInt(2, j);
                    ps2.setInt(3, 0);
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public String getType() {
        return "phaseReconciliation";
    }


    @Override
    public void setConnection(String connectionString, String dbms, int isolation) throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        connection.setTransactionIsolation(isolation);

        if (dbms.equals("postgresql")) {
            updateStockJoined = connection.prepareStatement(
                    "UPDATE Product SET stock = stock + ? WHERE pid = ? RETURNING stock");
            updateStockSplit = connection.prepareStatement(
                    "UPDATE Product_Split_Stock SET stock = stock + ? WHERE pid = ? AND core_id = ? RETURNING stock");
        }
        else if (dbms.equals("mysql") || dbms.equals("mariadb")) {
            updateStockJoined = connection.prepareStatement("CALL update_stock_joined(?, ?)");
            updateStockSplit = connection.prepareStatement("CALL update_stock_split(?, ?, ?)");
        }

        getStock = connection.prepareStatement("SELECT stock FROM Product WHERE pid = ?");

        connection.setAutoCommit(false);
    }


    @Override
    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public int decrementStock(List<String> pIds, int amount) {
        String pid = pIds.get(0);
        try {
            for (String pId: pIds) {
                pid = pId;
                boolean isJoinedPhase;
                PhaseReconciliationCoordinator.coordinator.handlePhaseChanges(pId, this.id);
                ResultSet rs;

                if (PhaseReconciliationCoordinator.coordinator.isJoinedPhase(pId)) {
                    isJoinedPhase = true;
                    updateStockJoined.setInt(1, -amount);
                    updateStockJoined.setString(2, pId);
                    rs = updateStockJoined.executeQuery();
                }
                else {
                    isJoinedPhase = false;
                    updateStockSplit.setInt(1, -amount);
                    updateStockSplit.setString(2, pId);
                    rs = updateStockSplit.executeQuery();
                }

                rs.next();
                int finalStock = rs.getInt(1);
                if (finalStock < 0) {
                    AddStatusWorker.addStatusWorker.incrementAbortsNoStock(pid);
                    connection.rollback();
                    // if we are in the joined phase and abort due to no stock, then we know that all the stock is depleted
                    if (isJoinedPhase) {
                        return -1;
                    }
                    // else, we are in the split phase and we do not know for certain if the other cores have stock reserved,
                    // so we must wait for the joined phase and retry
                    else {
                        return 0;
                    }
                }
            }
            connection.commit();
            pIds.forEach(x -> AddStatusWorker.addStatusWorker.incrementCommits(x));
            return 1;
        }
        catch (Exception e) {
            try {
                connection.rollback();
                AddStatusWorker.addStatusWorker.incrementAborts(pid);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return 0;
        }
    }


    @Override
    public boolean incrementStock(List<String> pIds, int amount) {
        String pid = pIds.get(0);
        try {
            for (String pId: pIds) {
                pid = pId;
                PhaseReconciliationCoordinator.coordinator.handlePhaseChanges(pId, this.id);

                if (PhaseReconciliationCoordinator.coordinator.isJoinedPhase(pId)) {
                    updateStockJoined.setInt(1, amount);
                    updateStockJoined.setString(2, pId);
                    updateStockJoined.execute();
                }
                else {
                    updateStockSplit.setInt(1, amount);
                    updateStockSplit.setString(2, pId);
                    updateStockSplit.execute();
                }
            }
            connection.commit();
            pIds.forEach(x -> AddStatusWorker.addStatusWorker.incrementCommits(x));
            return true;
        } catch (Exception e) {
            try {
                connection.rollback();
                AddStatusWorker.addStatusWorker.incrementAborts(pid);
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return false;
        }
    }


    @Override
    public int getStock(String pId) {
        try {
            PhaseReconciliationCoordinator.coordinator.handlePhaseChanges(pId, this.id);
            PhaseReconciliationCoordinator.coordinator.waitForJoinedPhase(pId, this.id);

            getStock.setString(1, pId);
            ResultSet rs = getStock.executeQuery();
            rs.next();
            AddStatusWorker.addStatusWorker.incrementCommits(pId);
            return rs.getInt(1);
        }
        catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
