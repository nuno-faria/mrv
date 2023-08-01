package transactions.mrv;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MrvWorkersSQL extends MrvWorkers {


    public MrvWorkersSQL(String connectionString, String dbms, int balanceDelta, String balanceAlgorithm,
                         int balanceMinmaxK, int balanceMinmaxKRatio, int balanceMinDiff, int balanceWindow,
                         String adjustAlgorithm, int adjustDelta, int adjustWindow, int maxNodes, int minNodes,
                         double arGoal, double arMin, int monitorDelta, int minAverageAmountPerNode, String workers) {
        super();
        try {
            switch (workers) {
                case "all":
                    new BalanceNodes(connectionString, dbms, balanceAlgorithm, balanceDelta, balanceMinmaxK,
                                     balanceMinmaxKRatio, balanceMinDiff, balanceWindow);
                    new AddStatusWorker(connectionString, dbms);
                    new AdjustNodes(connectionString, dbms, adjustAlgorithm, adjustDelta, adjustWindow, maxNodes, minNodes,
                            arGoal, arMin, minAverageAmountPerNode);
                    new Monitor(connectionString, monitorDelta);
                    new MonitorVariation(connectionString);
                    break;
                case "balance":
                    new BalanceNodes(connectionString, dbms, balanceAlgorithm, balanceDelta, balanceMinmaxK,
                                     balanceMinmaxKRatio, balanceMinDiff, balanceWindow);
                    new MonitorVariation(connectionString);
                    new AddStatusWorker(connectionString, dbms);
                    break;
                case "adjust":
                    new AddStatusWorker(connectionString, dbms);
                    new AdjustNodes(connectionString, dbms, adjustAlgorithm, adjustDelta, adjustWindow, maxNodes, minNodes,
                            arGoal, arMin, minAverageAmountPerNode);
                    new Monitor(connectionString, monitorDelta);
                    break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public class AddStatusWorker extends MrvWorkers.AddStatusWorker {
        private Connection connection;
        private PreparedStatement addTxStatus;


        public AddStatusWorker(String connectionString, String dbms) throws Exception {
            super();
            connection = DriverManager.getConnection(connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(true);
            if (dbms.equals("postgresql")) {
                addTxStatus = connection.prepareStatement(
                        "INSERT INTO Product_Tx VALUES (?, ?, ?, now()) ON CONFLICT (pid) DO UPDATE SET " +
                        "commits = Product_Tx.commits + EXCLUDED.commits, aborts = Product_Tx.aborts + EXCLUDED.aborts, " +
                        "last_updated = EXCLUDED.last_updated"
                );
            }
            else {
                addTxStatus = connection.prepareStatement(
                        "INSERT INTO Product_Tx VALUES (?, ?, ?, now(3)) ON DUPLICATE KEY UPDATE " +
                        "commits = commits + VALUES(commits), aborts = aborts + VALUES(aborts), " +
                        "last_updated = VALUES(last_updated);"
                );
            }
        }


        @Override
        void addStatusDB(Map<String, BatchedStatus> status) {
            int i = 0;
            int size = status.values().size();
            for (BatchedStatus s: status.values()) {
                try {
                    addTxStatus.setString(1, s.pid);
                    addTxStatus.setInt(2, s.commits);
                    addTxStatus.setInt(3, s.aborts);
                    addTxStatus.addBatch();

                    if (i % 100 == 0 || i == size - 1) {
                        boolean done = false;
                        while (!done) {
                            try {
                                addTxStatus.executeBatch();
                                done = true;
                            }
                            catch (Exception e) { }
                        }
                    }
                    i++;
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


        @Override
        void closeConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public class BalanceNodes extends MrvWorkers.BalanceNodes {
        private Connection connection;
        private PreparedStatement allProducts;
        private PreparedStatement countNodes;
        private PreparedStatement twoRandomNodes;
        private PreparedStatement maxMinNodes;
        private PreparedStatement allNodes;
        private PreparedStatement addStock;
        private PreparedStatement updateStock;
        private PreparedStatement updateAll;
        private Random random;
        private int minmaxK;
        private int minmaxKRatio;
        private int minDiff;
        private String dbms;


        private BalanceNodes(String connectionString, String dbms, String balanceAlgorithm, int delta,
                             int minmaxK, int minmaxKRatio, int minDiff, int window) throws Exception {
            super(delta, balanceAlgorithm);

            this.minmaxK = Math.max(minmaxK, 1);
            this.minmaxKRatio = minmaxKRatio;

            String windowBegin;
            if (dbms.equals("postgresql")) {
                windowBegin = "now() - interval '" + (delta * window / 100) + " milliseconds'";
            }
            else {
                windowBegin = "now(3) - interval " + ((double) delta * window / 100000) + " second";
            }

            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            if (dbms.equals("postgresql")) {
                Statement s = this.connection.createStatement();
                s.execute("set random_page_cost = 0");
                s.close();
            }
            this.allProducts = connection.prepareStatement(
                    "SELECT pid FROM Product_Tx " +
                    "WHERE last_updated >= " + windowBegin
            );
            this.countNodes = connection.prepareStatement("SELECT COUNT(*) FROM Product_Stock WHERE pid = ?");
            this.twoRandomNodes = connection.prepareStatement(
                    "(SELECT rk, stock FROM Product_Stock WHERE pid = ? OFFSET ? LIMIT 1) " +
                    "UNION ALL " +
                    "(SELECT rk, stock FROM Product_Stock WHERE pid = ? OFFSET ? LIMIT 1)"
            );
            if (dbms.equals("postgresql")) {
                String minmaxLimitCondition;
                if (minmaxKRatio > 0) {
                    minmaxLimitCondition =
                        " (SELECT least(greatest(count(*) /" + minmaxKRatio + ", 1), 32) FROM T) ";
                }
                else {
                    minmaxLimitCondition = " " + minmaxK;
                }
                this.maxMinNodes = connection.prepareStatement(
                    "WITH T AS (  " +
                        "SELECT rk, stock, row_number() over (order by stock desc, random()) " +
                        "FROM Product_Stock " +
                        "WHERE pid = ? " +
                    ")  " +
                    "(SELECT rk, stock FROM T LIMIT + " + minmaxLimitCondition + ") " +
                    "UNION ALL " +
                    "(SELECT rk, stock FROM T ORDER BY row_number DESC LIMIT " + minmaxLimitCondition + "); ");
            }
            else {
                connection.createStatement().execute("DROP PROCEDURE IF EXISTS minmaxNodes");
                if (minmaxKRatio > 0) {
                    connection.createStatement().execute(
                        "CREATE PROCEDURE minmaxNodes(pid_ varchar(255)) " +
                        "BEGIN  " +
                            "DECLARE limit_ int;" +
                            "SELECT least(greatest(count(*) / " + minmaxKRatio + ", 1), 32) FROM Product_Stock WHERE pid = pid_ INTO limit_; " +
                            "(SELECT rk, stock FROM Product_Stock WHERE pid = pid_ ORDER BY stock DESC LIMIT limit_) " +
                            "UNION ALL " +
                            "(SELECT rk, stock FROM Product_Stock WHERE pid = pid_ ORDER BY stock ASC LIMIT limit_); " +
                        "END;"
                    );
                }
                else {
                    connection.createStatement().execute(
                        "CREATE PROCEDURE minmaxNodes(pid_ varchar(255)) " +
                            "BEGIN  " +
                            "(SELECT rk, stock FROM Product_Stock WHERE pid = pid_ ORDER BY stock DESC LIMIT " + minmaxK + ") " +
                            "UNION ALL " +
                            "(SELECT rk, stock FROM Product_Stock WHERE pid = pid_ ORDER BY stock ASC LIMIT " + minmaxK + "); " +
                        "END;"
                    );
                }

                this.maxMinNodes = connection.prepareStatement("call minmaxNodes(?)");
            }
            this.allNodes = connection.prepareStatement("SELECT rk, stock FROM Product_Stock WHERE pid = ?");
            this.addStock = connection.prepareStatement("UPDATE Product_Stock SET stock = stock + ? WHERE pid = ? AND rk = ?");
            if (dbms.equals("postgresql")) {
                this.updateStock = connection.prepareStatement(
                        "UPDATE Product_Stock SET stock = ? WHERE pid = ? AND rk = any(?)");
            }
            else {
                this.updateStock = connection.prepareStatement(
                        "UPDATE Product_Stock SET stock = ? WHERE pid = ? AND find_in_set(rk, ?)");
            }
            this.updateAll = connection.prepareStatement("UPDATE Product_Stock SET stock = ? WHERE pid = ?");
            this.minDiff = minDiff;
            this.dbms = dbms;
            this.random = new Random();
        }


        @Override
        void beginTransaction() {
            try {
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        @Override
        void commitTransaction() throws Exception {
            connection.commit();
        }


        @Override
        void abortTransaction() {
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        @Override
        List<String> allProducts() throws Exception {
            ResultSet rs = allProducts.executeQuery();
            List<String> pids = new ArrayList<>();
            while (rs.next()) {
                pids.add(rs.getString(1));
            }
            return pids;
        }


        @Override
        Map<Integer, Integer> twoRandomNodes(String pid) throws Exception {
            countNodes.setString(1, pid);
            ResultSet rs = countNodes.executeQuery();
            rs.next();
            int size = rs.getInt(1);
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

            if (size >= 2) {
                List<Integer> l = IntStream.range(0, size).boxed().collect(Collectors.toList());
                Map<Integer, Integer> results = new HashMap<>();
                twoRandomNodes.setString(1, pid);
                twoRandomNodes.setInt(2, l.remove(random.nextInt(l.size())));
                twoRandomNodes.setString(3, pid);
                twoRandomNodes.setInt(4, l.remove(random.nextInt(l.size())));
                rs = twoRandomNodes.executeQuery();

                while (rs.next()) {
                    results.put(rs.getInt(1), rs.getInt(2));
                    max = Math.max(max, rs.getInt(2));
                    min = Math.min(min, rs.getInt(2));
                }

                double diffPercentage = (max - min) / (double) (max + min) * 100;
                if (results.size() >= 2 && diffPercentage >= minDiff) {
                    return results;
                }
                else {
                    return null;
                }
            }
            else {
                return null;
            }
        }


        @Override
        Map<Integer, Integer> maxMinNodes(String pid) throws Exception {
            maxMinNodes.setString(1, pid);
            ResultSet rs = maxMinNodes.executeQuery();
            Map<Integer, Integer> results = new HashMap<>();
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

            while (rs.next()) {
                results.put(rs.getInt(1), rs.getInt(2));
                min = Math.min(min, rs.getInt(2));
                max = Math.max(max, rs.getInt(2));
            }

            double diffPercentage = (max - min) / (double) (max + min) * 100;
            if (results.size() >= 2 && diffPercentage >= minDiff) {
                return results;
            }
            else {
                return null;
            }
        }


        @Override
        Map<Integer, Integer> allNodes(String pid) throws Exception {
            Map<Integer, Integer> nodes = new HashMap<>();
            allNodes.setString(1, pid);
            ResultSet rs = allNodes.executeQuery();
            while (rs.next()) {
                nodes.put(rs.getInt(1), rs.getInt(2));
            }
            return nodes;
        }


        @Override
        void addStock(String pid, int rk, int amount) throws Exception {
            addStock.setInt(1, amount);
            addStock.setString(2, pid);
            addStock.setInt(3, rk);
            addStock.executeUpdate();
        }


        @Override
        void updateNodes(String pid, List<Integer> nodes, int newValue) throws Exception {
            updateStock.setInt(1, newValue);
            updateStock.setString(2, pid);
            /*
            int i = 3;
            for (int rk: nodes) {
                updateStock.setInt(i, rk);
                i += 1;
            }
            // fill empty parameters in the 'IN' statement with the first rk (to prevent sql errors)
            while (i < minmaxK * 2 + 3) {
                updateStock.setInt(i, nodes.get(0));
                i += 1;
            }
            */
            if (dbms.equals("postgresql")) {
                updateStock.setArray(3, connection.createArrayOf("integer", nodes.toArray()));
            }
            else {
                updateStock.setString(3, String.join(",", nodes.stream().map(x -> x.toString()).collect(Collectors.toList())));
            }
            updateStock.executeUpdate();
        }


        @Override
        void updateAll(String pid, int newValue) throws Exception {
            updateAll.setInt(1, newValue);
            updateAll.setString(2, pid);
            updateAll.executeUpdate();
        }


        @Override
        void closeConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    public class AdjustNodes extends MrvWorkers.AdjustNodes {

        private Connection connection;
        // returns the total value, number of records, number of commits and aborts for all products in the window
        private PreparedStatement valuesAndStats;
        private PreparedStatement productNodesRks;
        private PreparedStatement productNodes;
        private PreparedStatement addNode;
        private PreparedStatement removeNode;
        private PreparedStatement updateNode;
        private PreparedStatement clearStats;
        private Random rand;
        private int maxNodes;


        public AdjustNodes(String connectionString, String dbms, String algorithm, int delta, int window, int maxNodes,
                           int minNodes, double arGoal, double arMin, int minAverageAmountPerNode) throws SQLException {
            super(algorithm, delta, maxNodes, minNodes, arGoal, arMin, minAverageAmountPerNode);
            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            String windowBegin;
            if (dbms.equals("postgresql")) {
                windowBegin = "now() - interval '" + ((double) delta * window / 100) + " milliseconds'";
            }
            else {
                windowBegin = "now(3) - interval " + ((double) delta * window / 100000) + " second";
            }

            this.valuesAndStats = connection.prepareStatement(
                    "SELECT ps.pid, sum(ps.stock), count(ps.pid), pt.commits, pt.aborts " +
                    "FROM Product_Stock AS ps " +
                    "JOIN Product_Tx AS pt ON ps.pid = pt.pid " +
                    "WHERE pt.last_updated >= "+ windowBegin + " " +
                    "GROUP BY ps.pid, pt.commits, pt.aborts"
            );
            this.productNodesRks = connection.prepareStatement("SELECT rk FROM Product_Stock WHERE pid = ?");
            this.productNodes = connection.prepareStatement("SELECT rk, stock FROM Product_Stock WHERE pid = ?");
            this.addNode = connection.prepareStatement("INSERT INTO Product_Stock (pid, rk, stock) VALUES(?, ?, 0)");
            this.removeNode = connection.prepareStatement("DELETE FROM Product_Stock WHERE pid = ? AND rk = ?");
            this.updateNode = connection.prepareStatement("UPDATE Product_Stock SET stock = stock + ? WHERE pid = ? AND rk = ?");
            this.clearStats = connection.prepareStatement("DELETE FROM Product_Tx");
            this.rand = new Random();
            this.maxNodes = maxNodes;
        }


        @Override
        void beginTransaction() {
            try {
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        @Override
        void commitTransaction() throws Exception {
            connection.commit();
        }


        @Override
        void abortTransaction() {
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


        @Override
        Map<String, int[]> getValuesAndStats() throws Exception {
            ResultSet rs = valuesAndStats.executeQuery();
            Map<String, int[]> m = new HashMap<>();
            while (rs.next()) {
                m.put(rs.getString(1), new int[]{rs.getInt(2), rs.getInt(3), rs.getInt(4), rs.getInt(5)});
            }
            return m;
        }


        @Override
        void clearTxStats() throws Exception {
            clearStats.executeUpdate();
        }


        @Override
        void addNode(String pid) throws Exception {
            productNodesRks.setString(1, pid);
            ResultSet rs = productNodesRks.executeQuery();
            List<Integer> rks = new ArrayList<>();

            while (rs.next()) {
                rks.add(rs.getInt(1));
            }

            List<Integer> availableRks = IntStream.range(0, maxNodes).boxed().collect(Collectors.toList());
            availableRks.removeAll(rks);
            int newRk = availableRks.get(rand.nextInt(availableRks.size()));

            addNode.setString(1, pid);
            addNode.setInt(2, newRk);
            addNode.executeUpdate();
        }


        @Override
        void removeNode(String pid) throws Exception {
            productNodes.setString(1, pid);
            ResultSet rs = productNodes.executeQuery();
            //using lists instead of map to easily get a random
            List<Integer> rks = new ArrayList<>();
            List<Integer> stocks = new ArrayList<>();

            while (rs.next()) {
                rks.add(rs.getInt(1));
                stocks.add(rs.getInt(2));
            }

            int r = rand.nextInt(rks.size());
            int rkToRemove = rks.remove(r);
            int amount = stocks.get(r);
            int rkToUpdate = rks.get(rand.nextInt(rks.size()));

            removeNode.setString(1, pid);
            removeNode.setInt(2, rkToRemove);
            removeNode.executeUpdate();

            updateNode.setInt(1, amount);
            updateNode.setString(2, pid);
            updateNode.setInt(3, rkToUpdate);
            updateNode.executeUpdate();
        }


        @Override
        void closeConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }



    public class Monitor extends MrvWorkers.Monitor {

        private Connection connection;
        private PreparedStatement totalNodesAndAbortRate;


        public Monitor(String connectionString, int delta) throws SQLException {
            super(delta);
            this.connection = DriverManager.getConnection(connectionString);
            this.totalNodesAndAbortRate = connection.prepareStatement(
                    "SELECT sum(ps.count), sum(aborts) / (CASE WHEN sum(aborts) + sum(commits) = 0 " +
                            "THEN 1 ELSE cast(sum(aborts) + sum(commits) AS decimal) END) " +
                            "FROM (SELECT pid, count(*) as count FROM Product_Stock GROUP BY pid) ps " +
                            "LEFT JOIN Product_Tx ptx ON ptx.pid = ps.pid;");
        }


        @Override
        double[] totalNodesAndAbortRate() {
            try {
                ResultSet rs = totalNodesAndAbortRate.executeQuery();
                rs.next();
                return new double[]{rs.getInt(1), rs.getDouble(2)};
            }
            catch (Exception e) {
                e.printStackTrace();
                return new double[]{0, 0};
            }
        }

        @Override
        void closeConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }



    public class MonitorVariation extends MrvWorkers.MonitorVariation {

        private Connection connection;
        private PreparedStatement variation;


        public MonitorVariation(String connectionString) throws SQLException {
            super();
            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setAutoCommit(true);
            this.variation = connection.prepareStatement(
                    "SELECT t1.pid, t1.variation, coalesce(t2.zeros, 0) AS zeros " +
                    "FROM (SELECT pid, stddev_pop(stock) / (CASE WHEN avg(stock) = 0 THEN 1 ELSE avg(stock) END) AS variation " +
                    "FROM Product_Stock GROUP BY pid) AS t1 " +
                    "LEFT JOIN (SELECT pid, coalesce(count(*), 0) AS zeros " +
                    "FROM Product_Stock WHERE stock = 0 GROUP BY pid) AS t2 ON t1.pid = t2.pid;"
            );
        }


        Map<String, VariationAndZeros> getVariationsAndZeros() throws Exception {
            Map<String, VariationAndZeros> variations = new HashMap<>();
            ResultSet rs = variation.executeQuery();
            while (rs.next()) {
                variations.put(rs.getString(1), new VariationAndZeros(rs.getDouble(2), rs.getInt(3)));
            }
            return variations;
        }
    }
}
