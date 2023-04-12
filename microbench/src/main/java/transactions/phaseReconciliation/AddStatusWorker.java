package transactions.phaseReconciliation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class AddStatusWorker implements Runnable {

    private BlockingQueue<Job> queue;
    public static AddStatusWorker addStatusWorker;
    private Thread thread;
    private AtomicBoolean over;
    private Connection connection;
    private PreparedStatement addTxStatus;


    public AddStatusWorker(String connectionString) throws SQLException {
        if (addStatusWorker == null) {
            this.queue = new LinkedBlockingQueue<>();
            this.connection = DriverManager.getConnection(connectionString);
            this.connection.setAutoCommit(true);
            this.addTxStatus = this.connection.prepareStatement(
                    "INSERT INTO Product_Stats VALUES (?, ?, ?, ?) " +
                    "ON CONFLICT (pid) DO UPDATE SET " +
                    "   commits = Product_Stats.commits + EXCLUDED.commits, " +
                    "   aborts = Product_Stats.aborts + EXCLUDED.aborts, " +
                    "   no_stock = Product_Stats.no_stock + EXCLUDED.no_stock");
            this.over = new AtomicBoolean(false);
            addStatusWorker = this;
            this.thread = new Thread(this);
        }
    }


    private class Job {
        String pid;
        char action; // c - increment commits, a - increment aborts

        public Job(String pid, char action) {
            this.pid = pid;
            this.action = action;
        }
    }


    private class BatchedStatus {
        int commits = 0;
        int aborts = 0;
        int noStock = 0;
    }


    public void incrementAborts(String pid) {
        queue.add(new Job(pid, 'a'));
    }


    public void incrementCommits(String pid) {
        queue.add(new Job(pid, 'c'));
    }


    public void incrementAbortsNoStock(String pid) {
        queue.add(new Job(pid, 's'));
    }


    public void start() {
        this.thread.start();
    }


    public void stop() {
        this.over.set(true);
        try {
            this.thread.join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        addStatusWorker = null;
    }


    private void addStatusDB(Map<String, BatchedStatus> batchedStatus) {
        final AtomicInteger i = new AtomicInteger();
        int size = batchedStatus.values().size();

        batchedStatus.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            try {
                addTxStatus.setString(1, entry.getKey());
                addTxStatus.setInt(2, entry.getValue().commits);
                addTxStatus.setInt(3, entry.getValue().aborts);
                addTxStatus.setInt(4, entry.getValue().noStock);
                addTxStatus.addBatch();

                if (i.get() % 100 == 0 || i.get() == size - 1) {
                    boolean done = false;
                    while (!done) {
                        try {
                            addTxStatus.executeBatch();
                            done = true;
                        }
                        catch (Exception ignored) { }
                    }
                }
                i.incrementAndGet();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


    public void run() {
        try {
            while (!over.get()) {
                List<Job> jobs = new ArrayList<>();
                queue.drainTo(jobs);
                Map<String, BatchedStatus> batchedStatus = new HashMap<>();

                for (Job j: jobs) {
                    batchedStatus.putIfAbsent(j.pid, new BatchedStatus());
                    switch (j.action) {
                        case 'a':
                            batchedStatus.get(j.pid).aborts += 1;
                            break;
                        case 'c':
                            batchedStatus.get(j.pid).commits += 1;
                            break;
                        case 's':
                            batchedStatus.get(j.pid).noStock += 1;
                            break;
                    }
                }
                addStatusDB(batchedStatus);
                Thread.sleep(10);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }
}
