import java.sql.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;


/**
 * Balances the amount between records
 */
public class BalanceWorker implements Runnable {

    Config config;
    List<BlockingQueue<BalanceJob>> queues;
    PreparedStatement txStatusQuery;


    public BalanceWorker(Config config) throws SQLException {
        this.config = config;
        queues = new ArrayList<>();
        for (int i = 0; i < config.balanceWorkers; i++) {
            LinkedBlockingQueue<BalanceJob> queue = new LinkedBlockingQueue<>();
            queues.add(queue);
            new Worker(queue);
        }
        (new Thread(this)).start();
    }


    private List<TxStatus> getTxStatus() throws SQLException {
        List<TxStatus> l = new ArrayList<>();
        ResultSet rs = txStatusQuery.executeQuery();
        while (rs.next()) {
            l.add(new TxStatus(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4), rs.getInt(5)));
        }
        return l;
    }


    public void run() {
        try {
            Connection connection = DriverManager.getConnection(config.connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            Random rand = new Random();
            double windowBegin = ((double) config.balanceDelta * config.balanceWindow / 100);
            txStatusQuery = connection.prepareStatement(
                    "SELECT * FROM " + config.statusTable +
                    " WHERE last_updated >= now() - interval '" + windowBegin + " milliseconds' " +
                    "   AND mrv_size(table_name, column_name, pk_sql) > 1");

            while (true) {
                try {
                    for (TxStatus status : getTxStatus()) {
                        BalanceJob job = new BalanceJob(status.tableName + "_" + status.columnName, status.pkCond, status.columnName);
                        int randomWorker = rand.nextInt(queues.size());
                        queues.get(randomWorker).add(job);
                    }
                    Thread.sleep(config.balanceDelta);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    class BalanceJob {
        public String table;
        public String pkCond;
        public String valueColumn;

        BalanceJob(String table, String pkCond, String valueColumn) {
            this.table = table;
            this.pkCond = pkCond;
            this.valueColumn = valueColumn;
        }
    }


    class Worker implements Runnable {
        private BlockingQueue<BalanceJob> queue;
        private Connection connection;
        private String getNodesQuery;
        private String updateSingleNodeQuery;
        private String updateMultipleNodesQuery;
        private Random rand;
        private Statement statement;


        public Worker(BlockingQueue<BalanceJob> queue) throws SQLException {
            connection = DriverManager.getConnection(config.connectionString);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            this.queue = queue;
            String minmaxLimitCondition;
            if (config.balanceMinmaxKRatio > 0) {
                minmaxLimitCondition =
                        " (SELECT least(greatest(count(*) /" + config.balanceMinmaxKRatio + ", 1), 32) FROM %s WHERE %s) ";
            }
            else {
                minmaxLimitCondition = " " + Math.max(1, config.balanceMinmaxK);
            }
            if (config.balanceAlgorithm.equals("minmax")) {
                getNodesQuery = "(SELECT rk, %s " +
                        "FROM %s " +
                        "WHERE %s " +
                        "ORDER BY %s DESC " +
                        "LIMIT " + minmaxLimitCondition + ") " +
                        "UNION ALL " +
                        "(SELECT rk, %s " +
                        "FROM %s " +
                        "WHERE %s " +
                        "ORDER BY %s ASC " +
                        "LIMIT " + minmaxLimitCondition + ")";
            }
            else {
                getNodesQuery = "(SELECT rk, %s " +
                        "FROM %s " +
                        "WHERE %s " +
                        "    AND (rk >= %s " +
                        "        OR NOT EXISTS( " +
                        "            SELECT 1 " +
                        "            FROM %s " +
                        "            WHERE %s AND rk >= %s " +
                        "            LIMIT 1) " +
                        "    ) " +
                        "ORDER BY rk " +
                        "LIMIT 1) " +
                        "UNION ALL " +
                        "(SELECT rk, %s " +
                        "FROM %s " +
                        "WHERE %s" +
                        "    AND (rk >= %s " +
                        "        OR NOT EXISTS( " +
                        "            SELECT 1 " +
                        "            FROM %s " +
                        "            WHERE %s AND rk >= %s " +
                        "            LIMIT 1) " +
                        "    ) " +
                        "ORDER BY rk " +
                        "LIMIT 1)";
            }
            updateSingleNodeQuery = "UPDATE %s " +
                    "SET %s = %d " +
                    "WHERE %s AND rk = %s";
            updateMultipleNodesQuery = "UPDATE %s " +
                    "SET %s = %d " +
                    "WHERE %s AND rk IN (%s)";
            rand = new Random();
            (new Thread(this)).start();
        }


        private Map<Integer, Integer> getNodes(BalanceJob job) throws SQLException {
            Map<Integer, Integer> m = new HashMap<>();
            ResultSet rs;
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;

            if (config.balanceAlgorithm.equals("minmax")) {
                if (config.balanceMinmaxKRatio > 0) {
                    rs = statement.executeQuery(String.format(getNodesQuery,
                            job.valueColumn, job.table, job.pkCond, job.valueColumn, job.table, job.pkCond,
                            job.valueColumn, job.table, job.pkCond, job.valueColumn, job.table, job.pkCond));
                }
                else {
                    rs = statement.executeQuery(String.format(getNodesQuery,
                            job.valueColumn, job.table, job.pkCond, job.valueColumn,
                            job.valueColumn, job.table, job.pkCond, job.valueColumn));
                }
            }
            else {
                int rk1 = rand.nextInt(config.maxNodes);
                int rk2 = rand.nextInt(config.maxNodes);
                rs = statement.executeQuery(String.format(getNodesQuery,
                        job.valueColumn, job.table, job.pkCond, rk1, job.table, job.pkCond, rk1,
                        job.valueColumn, job.table, job.pkCond, rk2, job.table, job.pkCond, rk2));
            }

            while (rs.next()) {
                m.put(rs.getInt(1), rs.getInt(2));
                min = Math.min(min, rs.getInt(2));
                max = Math.max(max, rs.getInt(2));
            }

            double diffPercentage = (max - min) / (double) (max + min) * 100;
            if (m.size() >= 2 && diffPercentage >= config.balanceMinDiff) {
                return m;
            }
            else {
                return null;
            }
        }


        private void updateNode(BalanceJob job, int rk, int amount) throws SQLException {
            String query = String.format(updateSingleNodeQuery, job.table, job.valueColumn, amount, job.pkCond, rk);
            statement.executeUpdate(query);
        }


        private void updateNodes(BalanceJob job, List<Integer> rks, int amount) throws SQLException {
            String rkIn = String.join(",", rks.stream().map(x -> x.toString()).sorted().collect(Collectors.toList()));
            String query = String.format(updateMultipleNodesQuery, job.table, job.valueColumn, amount, job.pkCond, rkIn);
            statement.executeUpdate(query);
        }


        public void run() {
            try {
                Statement s = connection.createStatement();
                while (true) {
                    BalanceJob job = queue.take();
                    boolean done = false;

                    while (!done) {
                        try {
                            Map<Integer, Integer> nodesToBalance = getNodes(job);
                            if (nodesToBalance != null) {
                                int total = nodesToBalance.values().stream().mapToInt(x -> x).sum();
                                int average = total / nodesToBalance.size();
                                int leftover = total - (average * nodesToBalance.size());
                                List<Integer> rks = new ArrayList<>(nodesToBalance.keySet());
                                updateNodes(job, rks, average);

                                if (leftover > 0) {
                                    updateNode(job, rks.get(0), average + leftover);
                                }
                            }
                            connection.commit();
                            done = true;
                        }
                        catch (Exception e) {
                            try {
                                connection.rollback();
                            } catch (SQLException ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
