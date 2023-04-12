package transactions.mrv;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Adjust, balance and monitor nodes workers
 * Adjust - controls the number of nodes per product
 * Balance - controls the amount for each product
 * Monitor - counts the number of nodes
 */
public abstract class MrvWorkers {

    private AtomicBoolean over;
    public Map<String, Double> workersStatistics;
    public Map<Long, double[]> monitorMeasurements;
    private static AddStatusWorker addStatusWorker; //singleton
    private List<Thread> threads;


    /*
     * Constructor
     * Classes extending this one should open one database connection per worker
     */
    public MrvWorkers() {
        over = new AtomicBoolean(false);
        workersStatistics = new ConcurrentHashMap<>();
        threads = new Vector<>();
    }


    /**
     * Adds the results of a transaction to the tx status table
     */
    public static void addTxResult(String pId, boolean committed) {
        if (addStatusWorker != null) {
            TxStatus status = new TxStatus(pId, committed, LocalDateTime.now());
            addStatusWorker.addJob(status);
        }
    }


    /**
     * Clears the remaining tx status jobs (to be used when the run ends)
     */
    public static void clearTxStatus() {
        if (addStatusWorker != null) {
            addStatusWorker.clearQueue();
        }
    }


    /**
     * Starts the workers
     */
    public void start() {
        threads.forEach(x -> x.start());
    }


    /**
     * Tells the workers to stop
     */
    public void stop() {
        over.set(true);
        threads.forEach(x -> {
            try {
                x.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }


    public class BatchedStatus {
        public String pid;
        public int commits;
        public int aborts;
        public LocalDateTime lastUpdated;

        public BatchedStatus(String pid, int commits, int aborts, LocalDateTime lastUpdated) {
            this.pid = pid;
            this.commits = commits;
            this.aborts = aborts;
            this.lastUpdated = lastUpdated;
        }
    }



    // ADD STATUS

    /**
     * Handles the async transaction result log
     * (so the adjustNodes worker can decide to add or remove nodes)
     */
    public abstract class AddStatusWorker implements Runnable {

        private BlockingQueue<TxStatus> queue;
        private Random rand;


        public AddStatusWorker() {
            queue = new LinkedBlockingQueue<>();
            rand = new Random();
            addStatusWorker = this;
            Thread t = new Thread(this);
            threads.add(t);
        }


        // inserts the status in the database
        abstract void addStatusDB(Map<String, BatchedStatus> status);

        // close the database connection
        abstract void closeConnection();


        private void addJob(TxStatus status) {
            queue.add(status);
        }


        public void clearQueue() {
            queue.clear();
        }


        public void run() {
            try {
                while (!over.get()) {
                    List<TxStatus> status = new ArrayList<>();
                    queue.drainTo(status);
                    Map<String, BatchedStatus> batchedStatus = new HashMap<>();
                    for (TxStatus s: status) {
                        batchedStatus.putIfAbsent(s.pId, new BatchedStatus(s.pId, 0, 0, s.time));
                        if (s.committed) {
                            batchedStatus.get(s.pId).commits += 1;
                        }
                        else {
                            batchedStatus.get(s.pId).aborts += 1;
                        }
                        batchedStatus.get(s.pId).lastUpdated = LocalDateTime.now();
                    }
                    addStatusDB(batchedStatus);
                    Thread.sleep(25); // set to 0 if testing small windows
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            finally {
                closeConnection();
            }
        }
    }


    // BALANCE

    /**
     * Balances the stock between nodes (selects two nodes at a time)
     */
    public abstract class BalanceNodes implements Runnable {

        private int delta; //millis
        private String balanceAlgorithm;


        public BalanceNodes(int delta, String balanceAlgorithm) {
            this.delta = delta;
            this.balanceAlgorithm = balanceAlgorithm;
            Thread t = new Thread(this);
            threads.add(t);
        }


        // begins a transaction
        abstract void beginTransaction();

        // commits a transaction
        abstract void commitTransaction() throws Exception;

        // aborts a transaction
        abstract void abortTransaction();

        // returns all products
        abstract List<String> allProducts() throws Exception;

        // returns two random nodes [rk1, stock1, rk2, stock2]; returns null if the two selected nodes are the same
        abstract Map<Integer, Integer> twoRandomNodes(String pid) throws Exception;

        // returns the maximum and minimum nodes [rk1, stock1, rk2, stock2]; returns null if the two selected nodes are the same
        abstract Map<Integer, Integer> maxMinNodes(String pid) throws Exception;

        // returns all nodes for some product (rk -> stock)
        abstract Map<Integer, Integer> allNodes(String pid) throws Exception;

        // adds an amount to some node of some product
        abstract void addStock(String pid, int rk, int amount) throws Exception;

        // updates some nodes with a new value (product, rk -> new value)
        abstract void updateNodes(String pid, List<Integer> nodes, int newValue) throws Exception;

        // updates all nodes in a product with a given value
        abstract void updateAll(String pid, int newValue) throws Exception;

        // closes the database connection
        abstract void closeConnection();


        public void run() {
            List<Long> balanceTimes = new ArrayList<>();
            while (!over.get()) {
                try {
                    beginTransaction();
                    List<String> products = allProducts();
                    commitTransaction();

                    long begin = System.currentTimeMillis();
                    for (String product: products) {
                        boolean done = false;
                        while (!done) {
                            try {
                                beginTransaction();
                                if (balanceAlgorithm.equals("random") || balanceAlgorithm.equals("minmax")) {
                                    Map<Integer, Integer> nodes;
                                    if (balanceAlgorithm.equals("random")) {
                                        nodes = twoRandomNodes(product);
                                    }
                                    else {
                                        nodes = maxMinNodes(product);
                                    }
                                    if (nodes != null) {
                                        int total = nodes.values().stream().mapToInt(x -> x).sum();
                                        int average = total / nodes.size();
                                        int leftover = average > 0 ? total % average : total;
                                        List<Integer> rks = new ArrayList<>(nodes.keySet());
                                        updateNodes(product, rks, average);
                                        if (leftover > 0) {
                                            addStock(product, rks.get(0), leftover);
                                        }
                                    }
                                }
                                else {
                                    Map<Integer, Integer> nodes = allNodes(product);
                                    if (nodes.size() >= 2) {
                                        int total = nodes.values().stream().mapToInt(x -> x).sum();
                                        int average = total / nodes.size();
                                        int leftover = average > 0 ? total % average : total;
                                        updateAll(product, average);
                                        if (leftover > 0) {
                                            addStock(product, nodes.keySet().iterator().next(), leftover);
                                        }
                                    }
                                }

                                commitTransaction();
                                done = true;
                            }
                            catch (Exception e) {
                                abortTransaction();
                            }
                        }
                    }
                    balanceTimes.add(System.currentTimeMillis() - begin);
                    Thread.sleep(delta);
                }
                catch (Exception e) {
                    e.printStackTrace();
                    abortTransaction();
                }
            }
            closeConnection();
            workersStatistics.put("balanceTime", balanceTimes.stream().mapToLong(x -> x).average().getAsDouble());
        }
    }



    // ADJUST

    /**
     * Adjust the number of nodes
     */
    public abstract class AdjustNodes implements Runnable {

        private String algorithm;
        private int delta; //millis
        private int maxNodes;
        private int minNodes;
        private double arGoal;
        private double arMin;
        private int minAverageAmountPerNode;


        public AdjustNodes(String algorithm, int delta, int maxNodes, int minNodes, double arGoal, double arMin,
                           int minAverageAmountPerNode) {
            this.algorithm = algorithm;
            this.delta = delta;
            this.maxNodes = maxNodes;
            this.minNodes = minNodes;
            this.arGoal = arGoal;
            this.arMin = arMin;
            this.minAverageAmountPerNode = minAverageAmountPerNode;
            Thread t = new Thread(this);
            threads.add(t);
        }

        // starts a transaction
        abstract void beginTransaction();


        // commits a transaction
        abstract void commitTransaction() throws Exception;


        // aborts a transaction
        abstract void abortTransaction();


        // returns a map with pid -> [total value, total number of nodes, number of commits, number of aborts]
        abstract Map<String, int[]> getValuesAndStats() throws Exception;


        // clears current stats
        abstract void clearTxStats() throws Exception;


        // adds a new node to the product pid
        abstract void addNode(String pid) throws Exception;


        // removes a node from the product pid
        abstract void removeNode(String pid) throws Exception;


        // closes the database connection
        abstract void closeConnection();


        public void run() {
            List<Long> adjustTimes = new ArrayList<>();
            long begin = System.currentTimeMillis();
            while (!over.get()) {
                try {
                    beginTransaction();
                    Map<String, int[]> valuesAndStats = getValuesAndStats();
                    commitTransaction();

                    boolean d = false;
                    while (!d) {
                        try {
                            beginTransaction();
                            clearTxStats();
                            commitTransaction();
                            d = true;
                        }
                        catch (Exception e) {
                            abortTransaction();
                        }
                    }

                    List<String> nodesToAdd = new ArrayList<>();
                    List<String> nodesToRemove = new ArrayList<>();

                    valuesAndStats.forEach((p, v) -> {
                        int currentValue = v[0];
                        int currentNodes = v[1];
                        int commits = v[2];
                        int aborts = v[3];
                        int currentMaxNodes = Math.min(currentValue / minAverageAmountPerNode, maxNodes);

                        if (currentNodes <= currentMaxNodes) {
                            if (commits + aborts > 0) {
                                double ar = (double) aborts / (aborts + commits);
                                int sign = ar > arGoal ? 1 : (ar < arMin ? -1 : 0);
                                int newNodes = 0;

                                switch (algorithm) {
                                    case "binary":
                                        newNodes = sign;
                                        break;

                                    case "linear":
                                        newNodes = (int) (sign * Math.round(1 + currentNodes * ar));
                                        break;

                                    case "quadratic":
                                        newNodes = (int) (sign * Math.round(1 + Math.pow(currentNodes * ar, 2)));
                                        break;
                                }

                                if (newNodes > 0) {
                                    newNodes = Math.min(newNodes, currentMaxNodes - currentNodes);
                                    nodesToAdd.addAll(IntStream.range(0, newNodes).mapToObj(x -> p).collect(Collectors.toList()));
                                }
                                else if (newNodes < 0) {
                                    newNodes = -Math.max(newNodes, minNodes - currentNodes);
                                    nodesToRemove.addAll(IntStream.range(0, newNodes).mapToObj(x -> p).collect(Collectors.toList()));
                                }
                            }
                        }
                        else if (currentNodes > minNodes) {
                            nodesToRemove.add(p);
                        }
                    });

                    //adds can all be done without aborting
                    int i = 0;
                    beginTransaction();
                    for (String s : nodesToAdd) {
                        addNode(s);
                        i += 1;
                        if (i % 15 == 0) {
                            commitTransaction();
                            beginTransaction();
                        }
                    }
                    commitTransaction();

                    for (String s : nodesToRemove) {
                        boolean done = false;
                        while (!done) {
                            try {
                                beginTransaction();
                                removeNode(s);
                                commitTransaction();
                                done = true;
                            } catch (Exception e) {
                                abortTransaction();
                            }
                        }
                    }
                    long end = System.currentTimeMillis() - begin;
                    adjustTimes.add(end);
                    Thread.sleep(delta);
                    begin = System.currentTimeMillis();
                }
                catch (Exception e) {
                    abortTransaction();
                }
            }
            closeConnection();
            workersStatistics.put("adjustTime", adjustTimes.stream().mapToLong(x -> x).average().getAsDouble());
        }
    }



    // MONITOR
    
    /**
     * Builds a time series with the total number of nodes and current abort rate
     */
    public abstract class Monitor implements Runnable {

        private int delta;


        public Monitor(int delta) {
            this.delta = delta;
            monitorMeasurements = new HashMap<>();
            Thread t = new Thread(this);
            threads.add(t);
        }


        // returns the total number of nodes
        abstract double[] totalNodesAndAbortRate();


        // closes the database connection
        abstract void closeConnection();


        public void run() {
            long begin = System.currentTimeMillis();
            while (!over.get()) {
                double[] r = totalNodesAndAbortRate();
                monitorMeasurements.put(System.currentTimeMillis() - begin, r);
                try {
                    Thread.sleep(delta);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            closeConnection();
        }
    }


    public abstract class MonitorVariation implements Runnable {

        class VariationAndZeros {
            public double variation;
            public int zeros;

            VariationAndZeros(double variation, int zeros) {
                this.variation = variation;
                this.zeros = zeros;
            }
        }


        public MonitorVariation() {
            Thread t = new Thread(this);
            threads.add(t);
        }

        private Map<String, List<VariationAndZeros>> allVariations;


        // returns all products and their current variation
        abstract Map<String, VariationAndZeros> getVariationsAndZeros() throws Exception;


        public void run() {
            allVariations = new HashMap<>();
            while (!over.get()) {
                try {
                    Map<String, VariationAndZeros> variations = getVariationsAndZeros();
                    variations.entrySet().forEach(x -> {
                        allVariations.putIfAbsent(x.getKey(), new ArrayList<>());
                        allVariations.get(x.getKey()).add(x.getValue());
                    });
                    Thread.sleep(50);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            workersStatistics.put("variation",
                    allVariations.values().stream().flatMap(x -> x.stream()).mapToDouble(x -> x.variation)
                                 .average().getAsDouble());
            workersStatistics.put("maxAvgVariation",
                    allVariations.values().stream().mapToDouble(x -> x.stream().mapToDouble(y -> y.variation)
                                 .average().getAsDouble()).max().getAsDouble());
            workersStatistics.put("zeros",
                    allVariations.values().stream().flatMap(x -> x.stream()).mapToDouble(x -> x.zeros)
                                 .average().getAsDouble());
        }
    }
}
