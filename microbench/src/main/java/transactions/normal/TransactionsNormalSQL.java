package transactions.normal;

import transactions.Transactions;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * One entry per product
 */
public class TransactionsNormalSQL implements Transactions {

    private Connection connection;
    private PreparedStatement updateStock;
    private PreparedStatement getStock;
    private int id;


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
            st.execute("CREATE TABLE IF NOT EXISTS Product (pId varchar(255) PRIMARY KEY, stock int)");
        } catch (Exception e) {
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
                st.executeUpdate("CREATE PROCEDURE update_stock(amount int, pId varchar(255)) " + "BEGIN "
                        + "UPDATE Product " + "SET stock = @stock := stock + amount " + "WHERE Product.pId = pId; "
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
    }


    @Override
    public void populate(String connectionString, String dbms, int pidLimit, int initialStock,
            Map<String, Object> extraConfigs) {
        try {
            Connection connection = DriverManager.getConnection(connectionString);
            connection.setAutoCommit(true);

            createSchema(connection, dbms);
            createProcedures(connection, dbms);
            clean(connection);

            PreparedStatement ps = connection.prepareStatement("INSERT INTO Product (pId, stock) VALUES(?, ?)");
            connection.setAutoCommit(false);

            for (int i = 0; i < pidLimit; i++) {
                ps.setString(1, "p" + i);
                ps.setInt(2, initialStock);
                ps.addBatch();
            }

            ps.executeBatch();
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
        return "normal";
    }


    @Override
    public void setConnection(String connectionString, String dbms, int isolation) throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        connection.setTransactionIsolation(isolation);

        if (dbms.equals("postgresql")) {
            updateStock = connection.prepareStatement(
                "UPDATE Product SET stock = stock + ? WHERE pid = ? RETURNING stock");
        } 
        else if (dbms.equals("mysql") || dbms.equals("mariadb")) {
            updateStock = connection.prepareStatement("CALL update_stock(?, ?)");
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
        try {
            for (String pId: pIds) {
                updateStock.setInt(1, -amount);
                updateStock.setString(2, pId);
                ResultSet rs = updateStock.executeQuery();
                rs.next();
                int finalStock = rs.getInt(1);
                if (finalStock < 0) {
                    connection.rollback();
                    return -1;
                }
            }
            connection.commit();
            return 1;
        }
        catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return 0;
        }
    }


    @Override
    public boolean incrementStock(List<String> pIds, int amount) {
        try {
            for (String pId: pIds) {
                updateStock.setInt(1, amount);
                updateStock.setString(2, pId);
                updateStock.execute();
            }
            connection.commit();
            return true;
        } catch (Exception e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
            return false;
        }
    }


    @Override
    public int getStock(String pId) {
        try {
            getStock.setString(1, pId);
            ResultSet rs = getStock.executeQuery();
            rs.next();
            return rs.getInt(1);
        } 
        catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
