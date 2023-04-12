package transactions.phaseReconciliation;

import java.sql.*;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages the phase reconciliation algorithm
 */
public class PhaseReconciliationCoordinator implements Runnable {

    public static PhaseReconciliationCoordinator coordinator = null;

    private int phaseDeltaMillis;
    private double abortRateToSplit;
    private double waitingRatioToJoin;
    private double noStockRatioToJoin;
    private Thread thread;
    private AtomicBoolean over;
    private Map<String, PhaseReconciliationAuxiliaryData> auxiliaryData;
    private ReentrantLock auxiliaryDataLock;
    private int totalNumberCores;
    private String connectionString;
    private Connection connection;
    private PreparedStatement getStats;
    private PreparedStatement getTotalCommitsAndNoStock;
    private PreparedStatement cleanStats;
    private Map<Integer, String> waitingForJoinedPhase; // worker id waiting for: product id
    private AtomicInteger totalPhaseChanges;


    /**
     * Stores auxiliary data pertaining to some product, to aid the phase reconciliation execution
     */
    private class PhaseReconciliationAuxiliaryData {
        String pid;
        ReentrantLock phaseLock;
        ReentrantLock waitLock;
        Condition waitCondition;
        Set<Integer> ackedPhaseChange;
        String currentPhase;
        String nextPhase;
        Connection connection;
        PreparedStatement mergeValues;
        PreparedStatement getJoinedValue;
        PreparedStatement updateAllSplitValues;
        PreparedStatement incrementSplitValue;


        public PhaseReconciliationAuxiliaryData(String pid, String connectionString) throws SQLException {
            this.pid = pid;
            this.phaseLock  = new ReentrantLock();
            this.waitLock = new ReentrantLock();
            this.waitCondition = this.waitLock.newCondition();
            this.ackedPhaseChange = ConcurrentHashMap.newKeySet();
            this.currentPhase = "joined";
            this.nextPhase = "joined";
            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setAutoCommit(true);
            this.mergeValues = this.connection.prepareStatement(
                    "UPDATE Product " +
                    "SET stock = (" +
                    "   SELECT sum(stock) " +
                    "   FROM Product_Split_Stock " +
                    "   WHERE pid = ?" +
                    ") " +
                    "WHERE pid = ?");
            this.mergeValues.setString(1, pid);
            this.mergeValues.setString(2, pid);
            this.getJoinedValue = this.connection.prepareStatement(
                    "SELECT stock " +
                    "FROM Product " +
                    "WHERE pid = ?");
            this.getJoinedValue.setString(1, pid);
            this.updateAllSplitValues = this.connection.prepareStatement(
                    "UPDATE Product_Split_Stock " +
                    "SET stock = ? " +
                    "WHERE pid = ?");
            this.updateAllSplitValues.setString(2, pid);
            this.incrementSplitValue = this.connection.prepareStatement(
                    "UPDATE Product_Split_Stock " +
                    "SET stock = stock + 1 " +
                    "WHERE pid = ? and core_id = ?");
            this.incrementSplitValue.setString(1, pid);
        }
    }


    /**
     * PhaseReconciliationCoordinator Constructor
     * @param phaseDeltaMillis Time between considering changing phases for some product
     * @param abortRateToSplit Minimum abort rate to consider changing to the split phase
     * @param waitingRatioToJoin Minimum number of transactions waiting for the joined phase to consider changing from the split phase
     */
    public PhaseReconciliationCoordinator(String connectionString, int phaseDeltaMillis, double abortRateToSplit,
                                          double waitingRatioToJoin, double noStockRatioToJoin, int totalNumberCores) throws SQLException {
        if (PhaseReconciliationCoordinator.coordinator == null) {
            this.phaseDeltaMillis = phaseDeltaMillis;
            this.abortRateToSplit = abortRateToSplit;
            this.waitingRatioToJoin = waitingRatioToJoin;
            this.noStockRatioToJoin = noStockRatioToJoin;
            this.auxiliaryData = new ConcurrentHashMap<>();
            this.auxiliaryDataLock = new ReentrantLock();
            this.totalNumberCores = totalNumberCores;
            this.connectionString = connectionString;
            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setAutoCommit(true);
            this.getStats = this.connection.prepareStatement("SELECT pid, commits, aborts, no_stock FROM Product_Stats");
            this.getTotalCommitsAndNoStock = this.connection.prepareStatement("SELECT sum(commits), sum(no_stock) FROM Product_Stats");
            this.cleanStats = this.connection.prepareStatement(
                    "UPDATE Product_Stats " +
                    "SET commits = 0, aborts = 0, no_stock = 0 " +
                    "WHERE pid IN ( " +
                    "   SELECT pid " +
                    "   FROM Product_Stats " +
                    "   ORDER BY pid " +
                    "   FOR UPDATE)");
            this.over = new AtomicBoolean(false);
            this.waitingForJoinedPhase = new ConcurrentHashMap<>();
            this.totalPhaseChanges = new AtomicInteger(0);
            coordinator = this;
            this.thread = new Thread(this);
        }
    }


    /**
     * Returns the auxiliary data of some particular product, creating a new object if it does not exists yet
     * @param pid Product id
     * @return Auxiliary data
     */
    public PhaseReconciliationAuxiliaryData getAuxiliaryData(String pid) throws SQLException {
        if (!this.auxiliaryData.containsKey(pid)) {
            // the lock is only acquired here to reduce contention, as the add operation is only done once per product per test
            this.auxiliaryDataLock.lock();
            // to prevent multiple auxiliary data for the same product
            if (!this.auxiliaryData.containsKey(pid)) {
                this.auxiliaryData.put(pid, new PhaseReconciliationAuxiliaryData(pid, this.connectionString));
            }
            this.auxiliaryDataLock.unlock();
        }
        return this.auxiliaryData.get(pid);
    }


    /**
     * Check whether the current phase of some particular product is "joined"
     * @param pid Product id
     * @return If the current phase is joined
     */
    public boolean isJoinedPhase(String pid) throws SQLException {
        PhaseReconciliationAuxiliaryData data = getAuxiliaryData(pid);
        return data.currentPhase.equals("joined");
    }


    /**
     * Change the current phase for some product
     * @param pid Product id
     * @param phase Phase to change to
     */
    public void changePhase(String pid, String phase) {
        try {
            PhaseReconciliationAuxiliaryData data = getAuxiliaryData(pid);

            data.phaseLock.lock();

            // already at the target phase
            if (data.currentPhase.equals(phase)) {
                data.phaseLock.unlock();
                return;
            }

            // merge the values in the global record
            if (phase.equals("joined")) {
                data.mergeValues.executeUpdate();
            }
            // split the value into multiple records (one per core)
            else {
                ResultSet rs = data.getJoinedValue.executeQuery();
                rs.next();
                int totalStock = rs.getInt(1);
                int coreAmount = totalStock / totalNumberCores;
                int leftover = totalStock - coreAmount * totalNumberCores;
                data.updateAllSplitValues.setInt(1, coreAmount);
                data.updateAllSplitValues.executeUpdate();
                // add leftover
                for (int i = 0; i < leftover; i++) {
                    data.incrementSplitValue.setInt(2, i);
                    data.incrementSplitValue.executeUpdate();
                }
            }
            data.ackedPhaseChange.clear();
            data.currentPhase = phase;
            totalPhaseChanges.incrementAndGet();

            data.phaseLock.unlock();

            // notify
            data.waitLock.lock();
            data.waitCondition.signalAll();
            data.waitLock.unlock();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Acknowledge phase changes.
     * A worker also executes the phase changing algorithm for some product if it is last ack.
     */
    private void acknowledgePhaseChanges(int id) throws SQLException {
        if (!over.get()) { // do not attempt phase changes at the end of the test
            for (PhaseReconciliationAuxiliaryData data: auxiliaryData.values()) {
                if (!data.currentPhase.equals(data.nextPhase)) {
                    data.ackedPhaseChange.add(id);
                    int nAcks = data.ackedPhaseChange.size();
                    // change phase if it was the last worker to acknowledge
                    if (nAcks == totalNumberCores) {
                        changePhase(data.pid, data.nextPhase);
                    }
                }
            }
        }
    }


    /**
     * Checks if there are phase changes taking place, acknowledging them if there are.
     * If the product currently being accessed is having its phase changed, the worker waits until the phase change finishes
     * @param pid Product currently being accessed by the worker
     */
    public void handlePhaseChanges(String pid, int id) throws SQLException {
        // acknowledge phase changes
        acknowledgePhaseChanges(id);

        // wait if the phase is changing for pid
        PhaseReconciliationAuxiliaryData data = getAuxiliaryData(pid);
        data.waitLock.lock();
        while (!data.nextPhase.equals(data.currentPhase)) {
            try {
                data.waitCondition.await();
                data.waitLock.unlock();
                acknowledgePhaseChanges(id);
                data.waitLock.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        data.waitLock.unlock();
    }


    /**
     * Blocks until the current phase of some product is "joined"
     * @param pid Product id
     */
    public void waitForJoinedPhase(String pid, int id) throws SQLException {
        PhaseReconciliationAuxiliaryData data = getAuxiliaryData(pid);

        if (data.currentPhase.equals("joined")) {
            return;
        }

        int nAcks = data.ackedPhaseChange.size();
        // change to joined phase if all workers are waiting for the same product
        if (nAcks == totalNumberCores) {
            data.phaseLock.lock();
            data.nextPhase = "joined";
            data.phaseLock.unlock();
            changePhase(data.pid, data.nextPhase);
        }

        data.waitLock.lock();
        waitingForJoinedPhase.put(id, pid);
        while (!data.currentPhase.equals("joined")) {
            try {
                data.waitCondition.await();
                data.waitLock.unlock();
                acknowledgePhaseChanges(id);
                data.waitLock.lock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        waitingForJoinedPhase.remove(id, pid);
        data.waitLock.unlock();
    }


    /**
     * Starts the phase reconciliation coordinator
     */
    public void start() {
        thread.start();
    }


    /**
     * Stops the phase reconciliation coordinator
     * @return The total number of phase changes
     */
    public int stop() {
        try {
            over.set(true);
            thread.join();
            Thread.sleep(100); // wait for the last transactions
            auxiliaryData.values().forEach(x -> {
                // change all phases to joined (to prevent dangling workers waiting for the joined phase)
                x.phaseLock.lock();
                x.nextPhase = "joined";
                x.phaseLock.unlock();
                changePhase(x.pid, "joined");
            });
            notifyAllWaiting();
            Thread.sleep(100); // wait for the last transactions

            auxiliaryData.values().forEach(x -> {
                try {
                    x.connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        coordinator = null;
        return totalPhaseChanges.get();
    }


    /**
     * Notifies all workers waiting on a condition
     */
    private void notifyAllWaiting() {
        for (PhaseReconciliationAuxiliaryData data: auxiliaryData.values()) {
            data.waitLock.lock();
            data.waitCondition.signalAll();
            data.waitLock.unlock();
        }
    }


    /**
     * Clears the products statistics
     */
    private void clearStats() {
        boolean clearead = false;
        while (!clearead) {
            try {
                cleanStats.executeUpdate();
                clearead = true;
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @Override
    public void run() {
        while (!over.get()) {
            try {
                double totalNoStockRatio = 0;
                ResultSet rs = getTotalCommitsAndNoStock.executeQuery();
                if (rs.next()) {
                    totalNoStockRatio = rs.getInt(2) / (double) (rs.getInt(1) + rs.getInt(2));
                }

                rs = getStats.executeQuery();
                while (rs.next()) {
                    String pid = rs.getString(1);
                    int commits = rs.getInt(2);
                    int aborts = rs.getInt(3);
                    int noStock = rs.getInt(4);
                    double ar = aborts / (double) (commits + aborts);
                    PhaseReconciliationAuxiliaryData data = getAuxiliaryData(pid);

                    // check if pid phase must be changed
                    data.phaseLock.lock();
                    if (data.currentPhase.equals("joined")) {
                        if (ar > abortRateToSplit) {
                            data.nextPhase = "split";
                        }
                    }
                    else {
                        boolean joinPhaseRequested = waitingForJoinedPhase.values().stream().anyMatch(x -> x.equals(pid));
                        long waitingJoinCount = waitingForJoinedPhase.size();
                        boolean beingWaitedForStock = noStock > 0;
                        if ((beingWaitedForStock && totalNoStockRatio > noStockRatioToJoin) ||
                                (joinPhaseRequested && (ar <= abortRateToSplit ||
                                        (waitingJoinCount > (waitingRatioToJoin * totalNumberCores))))) {
                            data.nextPhase = "joined";
                        }
                    }
                    data.phaseLock.unlock();
                }
                notifyAllWaiting();

                clearStats();

                Thread.sleep(phaseDeltaMillis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            this.connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
