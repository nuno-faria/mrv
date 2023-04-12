import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;
import transactions.*;
import transactions.mrv.*;
import transactions.normal.TransactionsNormalMongo;
import transactions.normal.TransactionsNormalSQL;
import transactions.phaseReconciliation.AddStatusWorker;
import transactions.phaseReconciliation.PhaseReconciliationCoordinator;
import transactions.phaseReconciliation.TransactionsPhaseReconciliation;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;


/**
 * Executes the benchmark
 */
public class Main {

    static Config config;


    /**
     * Sleeps and prints the timer
     * @param seconds Time to sleep
     */
    static void timer(int seconds) throws InterruptedException {
        String status = "";
        int timer = 0;
        while (timer < seconds) {
            System.out.print(status.replaceAll(".", "\b"));
            status = timer + "/" + seconds;
            System.out.print(status);
            Thread.sleep(1000);
            timer++;
        }
        System.out.println(status.replaceAll(".", "\b") + "Done");
    }


    /***
     * Builds the object with the transactions logic based on the type and dbms
     * @param type Normal or mrv
     * @return Transactions object
     */
    static Transactions buildTransactionsObject(String type) {
        Transactions transactions;
        if (type.equals("normal")) {
            if (config.dbms.equals("mongodb")) {
                transactions = new TransactionsNormalMongo();
            }
            else {
                transactions = new TransactionsNormalSQL();
            }
        }
        else if (type.equals("mrv")) {
            if (config.dbms.equals("mongodb")) {
                transactions = new TransactionsMrvMongo();
            }
            else {
                transactions = new TransactionsMrvSQL();
            }
        }
        else {
            transactions = new TransactionsPhaseReconciliation();
        }
        return transactions;
    }


    /**
     * Builds the adjust, balance, and monitor workers
     * @return Workers
     */
    static MrvWorkers buildWorkers() {
        MrvWorkers workers = null;
        if (config.dbms.equals("mongodb")) {
            workers = new MrvWorkersMongo(
                config.connectionStrings.get(0), config.balanceDelta, config.balanceAlgorithm,
                config.balanceMinmaxK, config.balanceMinmaxKRatio, config.balanceMinDiff, config.balanceWindow,
                config.adjustAlgorithm, config.adjustDelta, config.adjustWindow, config.maxNodes, config.minNodes,
                config.arGoal, config.arMin, config.monitorDelta, config.minAverageAmountPerNode, config.workers);
        }
        else {
            workers = new MrvWorkersSQL(
                config.connectionStrings.get(0), config.dbms, config.balanceDelta, config.balanceAlgorithm,
                config.balanceMinmaxK, config.balanceMinmaxKRatio, config.balanceMinDiff, config.balanceWindow,
                config.adjustAlgorithm, config.adjustDelta, config.adjustWindow, config.maxNodes, config.minNodes,
                config.arGoal, config.arMin, config.monitorDelta, config.minAverageAmountPerNode, config.workers);
        }
        return workers;
    }


    /***
     * Prints to results to csv and to stdout
     * @param type Type of benchmark
     * @param results List of all transactions results
     * @param workersStatistics Worker-related statistics
     * @param monitor Number of nodes over time (or null if no monitor worker/not mrv)
     */
    static void printResults(String type, List<TxResult> results, Map<String, Double> workersStatistics,
                             Map<Long, double[]> monitor, int totalPhaseChanges) {

        // remove first 5 seconds
        long minTimestamp = results.stream().map(x -> x.timestamp).min(Long::compareTo).get();
        List<TxResult> validResults = results.stream().filter(x -> x.timestamp - minTimestamp >= 5000).collect(Collectors.toList());

        List<TxResult> committedResults = validResults.stream().filter(x -> x.result).collect(Collectors.toList());
        long nCommits = committedResults.size();
        double txAvg = (double) nCommits / (config.time - 5);
        long nCommitsWrite = committedResults.stream().filter(x -> x.type == 'a' || x.type == 's').count();
        long nCommitsRead = committedResults.stream().filter(x -> x.type == 'r').count();
        double txAvgWrite = (double) nCommitsWrite / (config.time - 5);
        double txAvgRead = (double) nCommitsRead / (config.time - 5);
        double rtAvg = committedResults.stream().mapToDouble(x -> x.rt).average().getAsDouble();
        double rtErr = Math.sqrt(committedResults.stream().mapToDouble(x -> Math.pow(x.rt - rtAvg, 2)).sum() / (nCommits - 1));
        double rtP95 = committedResults.stream().map(x -> x.rt).sorted().collect(Collectors.toList()).get((int) (nCommits * 0.95));
        double ar = 1 - ((double) nCommits / validResults.size());
        double rtAdd = committedResults.stream().filter(x -> x.type == 'a').mapToDouble(x -> x.rt).average().orElse(0);
        double rtSub = committedResults.stream().filter(x -> x.type == 's').mapToDouble(x -> x.rt).average().orElse(0);

        // tx std dev based on config.timeBucketErr second intervals
        long beginTime = committedResults.stream().mapToLong(x -> x.timestamp).min().getAsLong();
        Map<Long, Long> txIntervals = new HashMap<>();
        results.stream().filter(x -> x.result).forEach(x -> {
            long relativeTime = (x.timestamp - beginTime) / 1000;
            long interval = relativeTime / config.timeBucketErr;
            if (interval < config.time / config.timeBucketErr) { // ignore transactions that finished after the bench time
                txIntervals.putIfAbsent(interval, 0L);
                txIntervals.put(interval, txIntervals.get(interval) + 1);
            }
        });
        double txErr = Math.sqrt(txIntervals.values().stream()
                .mapToDouble(x -> Math.pow((double) x / config.timeBucketErr - txAvg, 2))
                .sum() / (txIntervals.size() - 1));

        System.out.println("tx/s : " + txAvg);
        System.out.println("ar : " + ar);
        System.out.println("rt : " + rtAvg);
        config.out.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," +
                                         "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                type, config.cli, config.size, config.opDistribution, config.unevenScale, config.accessDistribution,
                config.pAccessed, config.initialStock, config.amountLimit, config.isolation, config.noCollision,
                config.initNodes, config.workers, config.balanceAlgorithm, config.balanceDelta, config.balanceMinmaxK,
                config.balanceMinDiff, config.balanceWindow, config.adjustAlgorithm, config.adjustDelta,
                config.adjustWindow, txAvg, txAvgWrite, txAvgRead, ar, rtAvg, txErr, rtErr, rtP95, rtAdd, rtSub,
                workersStatistics.getOrDefault("balanceTime", 0.0), workersStatistics.getOrDefault("adjustTime", 0.0),
                workersStatistics.getOrDefault("variation", 0.0), workersStatistics.getOrDefault("maxAvgVariation", 0.0),
                workersStatistics.getOrDefault("zeros", 0.0), config.hybridReadRatio, config.abortRateToSplit,
                config.waitingRatioToJoin, config.noStockRatioToJoin, totalPhaseChanges));
        config.out.flush();

        //monitor results
        if (monitor != null) {
            monitor.forEach((t,n) ->
                    config.outMonitor.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                            config.cli, config.size, config.initialStock, config.amountLimit, config.isolation,
                            config.noCollision, config.adjustAlgorithm, t, (int) n[0], n[1])));
            config.outMonitor.flush();
        }
    }


    /**
     * Runs a test
     * @throws Exception
     */
    static void runTest() throws Exception {
        if (config.noCollision) {
            config.size = config.cli;
        }
        System.out.println(config);

        // transactions object
        Transactions transactions = buildTransactionsObject(config.type);
        System.out.println(transactions.getType());
        
        // extra populate configurations (mrv)
        Map<String, Object> extraConfigs = null;
        if (config.type.equals("mrv")) {
            extraConfigs = new HashMap<>();
            if (config.initNodes == 0) {
                config.initNodes = Math.min(config.cli, config.maxNodes);
            }
            else if (config.initNodes == -1) {
                config.initNodes = Math.min(config.cli * 2, config.maxNodes);
            }
            else if (config.initNodes < -1) {
                config.initNodes = Math.min(Math.max((int)Math.ceil((config.cli / (double) config.size) * (-config.initNodes)), config.minNodes), config.maxNodes);
            }
            extraConfigs.put("initialNodes", config.initNodes);
            extraConfigs.put("maxNodes", config.maxNodes);
            extraConfigs.put("distributeAddsSize", config.opDistribution.equals("uneven") ? config.distributeAddsSize : 0);
            extraConfigs.put("zeroNodesPercentage", config.zeroNodesPercentage);
        }
        else if (config.type.equals("phaseReconciliation")) {
            extraConfigs = new HashMap<>();
            extraConfigs.put("cores", config.cli);
        }

        // populate
        System.out.println("populating");
        transactions.populate(config.connectionStrings.get(0), config.dbms, config.size, config.initialStock, extraConfigs);

        // stop flag
        AtomicBoolean over = new AtomicBoolean();
        over.set(false);

        // async add tx result log, balance nodes and adjust nodes workers
        MrvWorkers workers = null;
        if (config.type.equals("mrv") && !config.workers.equals("none") && (config.mode.equals("write") || config.mode.equals("hybrid"))) {
            workers = buildWorkers();
        }

        // phase reconciliation coordinator
        PhaseReconciliationCoordinator phaseReconciliationCoordinator = null;
        AddStatusWorker addStatusWorker = null;
        if (config.type.equals("phaseReconciliation")) {
            phaseReconciliationCoordinator = new PhaseReconciliationCoordinator(config.connectionStrings.get(0),
                    config.phaseDeltaMillis, config.abortRateToSplit, config.waitingRatioToJoin,
                    config.noStockRatioToJoin, config.cli);
            addStatusWorker = new AddStatusWorker(config.connectionStrings.get(0));
        }

        // clients
        List<Client> clientsL = new ArrayList<>();
        for (int i = 0; i < config.cli; i++) {
            clientsL.add(new Client(config.connectionStrings.get(i % config.connectionStrings.size()), config.dbms,
                    config.mode, config.opDistribution, config.unevenScale, config.accessDistribution,
                    over, transactions, config.size, config.amountLimit, config.pAccessed,
                    config.isolation, config.noCollision, config.hybridReadRatio, config.hybridReadRatioUnit, config.cli, i));
        }
        List<Thread> clientsThreads = new ArrayList<>();
        clientsL.forEach(x -> clientsThreads.add(new Thread(x)));

        if (workers != null) {
            workers.start();
        }
        if (phaseReconciliationCoordinator != null) {
            phaseReconciliationCoordinator.start();
            addStatusWorker.start();
        }
        clientsThreads.forEach(Thread::start);

        // timer
        timer(config.time);

        // stop clients and workers
        over.set(true);
        if (workers != null) {
            workers.stop();
        }
        int totalPhaseChanges = 0;
        if (phaseReconciliationCoordinator != null) {
            totalPhaseChanges = phaseReconciliationCoordinator.stop();
            addStatusWorker.stop();
        }
        for (Thread clientThread : clientsThreads) {
            clientThread.join();
        }
        MrvWorkers.clearTxStatus();

        List<TxResult> results = clientsL.stream().flatMap(x -> x.results.stream()).collect(Collectors.toList());
        printResults(transactions.getType(), results, workers != null ? workers.workersStatistics : new HashMap<>(),
                     workers != null ? workers.monitorMeasurements : null, totalPhaseChanges);

        // cooldown
        Thread.sleep(config.cooldown * 1000);
        System.out.println();
    }


    // Runs benchmark
    static void testAll() throws Exception {
        DateTimeFormatter timeStampPattern = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String date = LocalDateTime.now().format(timeStampPattern);
        config.out = new PrintWriter("out-" + config.dbms + "-" + date + ".csv");
        config.out.println("type,clients,size,opDistribution,unevenScale,accessDistribution,productsAccessed,initialStock," +
                "amountLimit,isolation,noCollision,initialNodes,workers,balanceAlgorithm,balanceDelta,balanceMinmaxK," +
                "balanceMinDiff,balanceWindow,adjustAlgorithm,adjustDelta,adjustWindow,tx/s,txWrite/s,txRead/s,ar,rt,tx/s_err,rt_err," +
                "rt_95,rt_add,rt_sub,balance_time,adjust_time,variation,max_avg_variation,zeros,readRatio,abortRateToSplit," +
                "waitingRatioToJoin,noStockRatioToJoin,totalPhaseChanges");
        config.outMonitor = new PrintWriter("out-" + config.dbms + "-monitor-" + date + ".csv");
        config.outMonitor.println("clients,size,initialStock,amountLimit,isolation,noCollisions,adjustAlgorithm,time,nodes,ar");

        //disable mongodb logs
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger("org.mongodb.driver");
        rootLogger.setLevel(Level.OFF);
        rootLogger = loggerContext.getLogger("io.grpc");
        rootLogger.setLevel(Level.OFF);

        while (config.nextBenchmarkConfig()) {
            if (config.type.equals("mrv") && !config.workers.equals("none")) {
                while (config.nextMrvConfig()) {
                    runTest();
                }
            }
            else if (config.type.equals("phaseReconciliation")) {
                while (config.nextPhaseReconciliationConfig()) {
                    runTest();
                }
            }
            else {
                runTest();
            }
        }
    }


    /**
     * Main
     */
    public static void main(String[] args) throws Exception {
        config = Config.readConfig("src/main/resources/config.yaml");
        testAll();
        System.exit(0);
    }
}
