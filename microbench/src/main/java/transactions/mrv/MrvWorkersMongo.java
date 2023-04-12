package transactions.mrv;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;

import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class MrvWorkersMongo extends MrvWorkers {

    private TransactionOptions transactionOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY)
            .build();


    public MrvWorkersMongo(String connectionString, int balanceDelta, String balanceAlgorithm, int balanceMinmaxK,
                           int balanceMinmaxKRatio, int balanceMinDiff, int balanceWindow, String adjustAlgorithm,
                           int adjustDelta, int adjustWindow, int maxNodes, int minNodes, double arGoal, double arMin,
                           int monitorDelta, int minAverageAmountPerNode, String workers) {
        super();
        try {
            switch (workers) {
                case "all":
                    new BalanceNodes(connectionString, balanceAlgorithm, balanceDelta, balanceMinmaxK,
                                     balanceMinmaxKRatio, balanceMinDiff, balanceWindow);
                    new AddStatusWorker(connectionString);
                    new AdjustNodes(connectionString, adjustAlgorithm, adjustDelta, adjustWindow, maxNodes, minNodes,
                                    arGoal, arMin, minAverageAmountPerNode);
                    new Monitor(connectionString, monitorDelta);
                    new MonitorVariation(connectionString);
                    break;
                case "balance":
                    new BalanceNodes(connectionString, balanceAlgorithm, balanceDelta, balanceMinmaxK,
                                     balanceMinmaxKRatio, balanceMinDiff, balanceWindow);
                    new MonitorVariation(connectionString);
                    new AddStatusWorker(connectionString);
                    break;
                case "adjust":
                    new AddStatusWorker(connectionString);
                    new AdjustNodes(connectionString, adjustAlgorithm, adjustDelta, adjustWindow, maxNodes, minNodes,
                                    arGoal, arMin, minAverageAmountPerNode);
                    new Monitor(connectionString, monitorDelta);
                    break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Handles the async transaction result log
     * (so the adjustNodes worker can decide to add or remove nodes)
     */
    public class AddStatusWorker extends MrvWorkers.AddStatusWorker {

        private MongoClient client;
        private ClientSession session;
        private MongoDatabase database;
        private MongoCollection productTx;


        public AddStatusWorker(String connectionString) {
            super();
            client = MongoClients.create(connectionString);
            session = client.startSession();
            database = client.getDatabase("testdb");
            productTx = database.getCollection("product_tx");
        }


        @Override
        void addStatusDB(Map<String, BatchedStatus> status) {
            for (BatchedStatus s: status.values()) {
                boolean done = false;
                try {
                    while (!done) {
                        Document commitsAndAborts = new Document();
                        commitsAndAborts.put("commits", s.commits);
                        commitsAndAborts.put("aborts", s.aborts);
                        Document lastUpdated = new Document("last_updated", LocalDateTime.now());
                        Document updates = new Document();
                        updates.put("$inc", commitsAndAborts);
                        updates.put("$set", lastUpdated);
                        productTx.updateOne(Filters.eq("pid", s.pid), updates, new UpdateOptions().upsert(true));
                        done = true;
                    }
                }
                catch (Exception e) { e.printStackTrace(); }
            }
        }


        @Override
        void closeConnection() {
            client.close();
        }
    }


    public class BalanceNodes extends MrvWorkers.BalanceNodes {

        private MongoClient client;
        private ClientSession session;
        private MongoCollection productStock;
        private MongoCollection productTx;
        private String connectionString;
        private int delta;
        private int minmaxK;
        private int minmaxKRatio;
        private int minDiff;
        private int window;

        private BalanceNodes(String connectionString, String balanceAlgorithm, int delta, int minmaxK, int minmaxKRatio,
                             int minDiff, int window) {
            super(delta, balanceAlgorithm);
            this.connectionString = connectionString;
            this.delta = delta;
            this.minmaxK = minmaxK;
            this.minmaxKRatio = minmaxKRatio;
            this.window = window;
            this.minDiff = minDiff;
        }


        @Override
        void beginTransaction() {
            if (session != null) {
                session.startTransaction(transactionOptions);
            }
            else {
                client = MongoClients.create(connectionString);
                session = client.startSession();
                productStock = client.getDatabase("testdb").getCollection("product_stock");
                productTx = client.getDatabase("testdb").getCollection("product_tx");
                session.startTransaction(transactionOptions);
            }
        }


        @Override
        void commitTransaction() {
            session.commitTransaction();
        }


        @Override
        void abortTransaction() {
            session.abortTransaction();
        }


        @Override
        List<String> allProducts() {
            FindIterable products = productTx.find(Filters.gte("last_updated",
                    LocalDateTime.now().minus(delta * window / 100, ChronoField.MILLI_OF_DAY.getBaseUnit())))
                .projection(Projections.include("pid"));
            List<String> pids = new ArrayList<>();
            for (Object product : products) {
                pids.add(((Document) product).getString("pid"));
            }
            return pids;
        }


        @Override
        Map<Integer, Integer> twoRandomNodes(String pid) {
            Map<Integer, Integer> results = new HashMap<>();
            int max = Integer.MIN_VALUE, min = Integer.MAX_VALUE;
            AggregateIterable aggregate = productStock.aggregate(session, Arrays.asList(
                    //THIS NO-OP IS NEEDED BECAUSE OTHERWISE THE DATABASE
                    //WILL CRASH WITH THE FOLLOWING ERROR:
                    //Invariant failure !_planYielding->getOpCtx()->lockState()->inAWriteUnitOfWork()
                    // src/mongo/db/query/plan_yield_policy.cpp 74
                    Aggregates.match(Filters.ne("pid", "000")),
                    Aggregates.sample(2)
            ));

            for (Object o : aggregate) {
                Document d = ((Document) o);
                results.put(d.getInteger("rk"), d.getInteger("stock"));
                max = Math.max(max, d.getInteger("stock"));
                min = Math.min(min, d.getInteger("stock"));
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
        Map<Integer, Integer> maxMinNodes(String pid) {
            Map<Integer, Integer> results = new HashMap<>();
            int max = Integer.MIN_VALUE, min = Integer.MAX_VALUE;
            int k;
            if (minmaxKRatio > 0) {
                k = Math.min(Math.max((int) (productStock.countDocuments(Filters.eq("pid", pid)) / minmaxKRatio), 1), 32);
            }
            else {
                k = Math.max(minmaxK, 1);
            }

            FindIterable<Document> maxs =
                    productStock.find(Filters.eq("pid", pid)).sort(Sorts.descending("stock")).limit(k);
            FindIterable<Document> mins =
                    productStock.find(Filters.eq("pid", pid)).sort(Sorts.ascending("stock")).limit(k);

            for (Document d : maxs) {
                results.put(d.getInteger("rk"), d.getInteger("stock"));
                max = Math.max(max, d.getInteger("stock"));
            }
            for (Document d : mins) {
                results.put(d.getInteger("rk"), d.getInteger("stock"));
                min = Math.min(min, d.getInteger("stock"));
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
            FindIterable<Document> docs = productStock.find(session, Filters.eq("pid", pid)).projection(Projections.include("rk", "stock"));
            for (Document doc: docs) {
                nodes.put(doc.getInteger("rk"), doc.getInteger("stock"));
            }
            return nodes;
        }


        @Override
        void addStock(String pid, int rk, int amount) {
            Document search = new Document();
            search.put("pid", pid);
            search.put("rk", rk);
            productStock.updateOne(session, search, Updates.inc("stock", amount));
        }


        @Override
        void updateNodes(String pid, List<Integer> rks, int newValue) throws Exception {
            productStock.updateMany(session,
                Filters.and(Filters.eq("pid", pid), Filters.in("rk", rks)),
                Updates.set("stock", newValue));
        }


        @Override
        void updateAll(String pid, int newValue) throws Exception {
            productStock.updateMany(session, Filters.eq("pid", pid), Updates.set("stock", newValue));
        }


        @Override
        void closeConnection() {
            client.close();
        }
    }


    public class AdjustNodes extends MrvWorkers.AdjustNodes {

        private MongoClient client;
        private ClientSession session;
        private MongoCollection productStock;
        private MongoCollection productTx;
        private Random rand;
        private int maxNodes;
        private int minNodes;
        private String connectionString;
        private int delta;
        private int window;


        public AdjustNodes(String connectionString, String adjustAlgorithm, int delta, int window, int maxNodes,
                           int minNodes, double arGoal, double arMin, int minAverageAmountPerNode) {
            super(adjustAlgorithm, delta, maxNodes, minNodes, arGoal, arMin, minAverageAmountPerNode);
            this.rand = new Random();
            this.maxNodes = maxNodes;
            this.minNodes = minNodes;
            this.connectionString = connectionString;
            this.delta = delta;
            this.window = window;
        }


        @Override
        void beginTransaction() {
            if (client != null) {
                session.startTransaction(transactionOptions);
            }
            else {
                client = MongoClients.create(connectionString);
                productStock = client.getDatabase("testdb").getCollection("product_stock");
                productTx = client.getDatabase("testdb").getCollection("product_tx");
                session = client.startSession();
                session.startTransaction(transactionOptions);
            }
        }


        @Override
        void commitTransaction() {
            session.commitTransaction();
        }


        @Override
        void abortTransaction() {
            session.abortTransaction();
        }


        @Override
        Map<String, int[]> getValuesAndStats() throws Exception {
            Map<String, int[]> result = new HashMap<>();

            FindIterable<Document> stats = productTx.find(session, Filters.gte("last_updated",
                    LocalDateTime.now().minus(delta * window / 100, ChronoField.MILLI_OF_DAY.getBaseUnit())));
            for (Document d: stats) {
              result.put(d.getString("pid"), new int[]{0, 0, d.getInteger("commits"), d.getInteger("aborts")});
            }

            AggregateIterable<Document> valuesAndSizes = productStock.aggregate(session, Arrays.asList(
                    Aggregates.match(Filters.in("pid", result.keySet())),
                    Aggregates.group("$pid", Accumulators.sum("count", 1), Accumulators.sum("sum", "$stock"))
            ));
            for (Document d: valuesAndSizes) {
                result.get(d.getString("_id"))[0] = d.getInteger("sum");
                result.get(d.getString("_id"))[1] = d.getInteger("count");
            }

            return result;
        }


        @Override
        void clearTxStats() {
            productTx.deleteMany(new Document());
        }


        @Override
        void addNode(String pid) {
            FindIterable projection = productStock.find(session,
                    Filters.eq("pid", pid)).projection(Projections.include("rk"));
            List<Integer> productRks = new ArrayList<>();

            for (Object o : projection) {
                Document d = ((Document) o);
                productRks.add(d.getInteger("rk"));
            }

            if (productRks.size() < maxNodes) {
                List<Integer> availableRks = IntStream.range(0, maxNodes).boxed().collect(Collectors.toList());
                availableRks.removeAll(productRks);
                int newRk = availableRks.get(rand.nextInt(availableRks.size()));
                Document toInsert = new Document();
                toInsert.put("pid", pid);
                toInsert.put("rk", newRk);
                toInsert.put("stock", 0);
                productStock.insertOne(session, toInsert);
            }
        }


        @Override
        void removeNode(String pid) {
            FindIterable projection = productStock.find(session,
                    Filters.eq("pid", pid)).projection(Projections.include("rk", "stock"));
            List<Integer> rks = new ArrayList<>();
            List<Integer> stocks = new ArrayList<>();

            for (Object o : projection) {
                Document d = ((Document) o);
                rks.add(d.getInteger("rk"));
                stocks.add(d.getInteger("stock"));
            }

            if (rks.size() > minNodes) {
                int r = rand.nextInt(rks.size());
                int rkToRemove = rks.remove(r);
                int amount = stocks.get(r);
                int rkToUpdate = rks.get(rand.nextInt(rks.size()));

                Document toRemove = new Document();
                toRemove.put("pid", pid);
                toRemove.put("rk", rkToRemove);
                productStock.deleteOne(session, toRemove);

                Document toUpdate = new Document();
                toUpdate.put("pid", pid);
                toUpdate.put("rk", rkToUpdate);
                productStock.updateOne(session, toUpdate, Updates.inc("stock", amount));
            }
        }


        @Override
        void closeConnection() {
            client.close();
        }
    }


    public class Monitor extends MrvWorkers.Monitor {

        private MongoClient client;
        private MongoCollection productStock;
        private MongoCollection productTx;


        public Monitor(String connectionString, int delta) {
            super(delta);
            client = MongoClients.create(connectionString);
            productStock = client.getDatabase("testdb").getCollection("product_stock");
            productTx = client.getDatabase("testdb").getCollection("product_tx");
        }


        @Override
        double[] totalNodesAndAbortRate() {
            long totalStock = productStock.countDocuments();
            Document doc = (Document) productTx.aggregate(Arrays.asList(
                Aggregates.project(
                    Projections.fields(
                        new Document("commits", new Document("$sum", "$commits")),
                        new Document("aborts", new Document("$sum", "$aborts"))
                )))).first();

            try {
                int total = doc.getInteger("commits") + doc.getInteger("aborts");
                double ar = doc.getInteger("aborts") / ((double) (total == 0 ? 1: total));
                return new double[]{totalStock, ar};
            }
            catch (Exception e) {
                return new double[]{0, 0};
            }
        }


        @Override
        void closeConnection() {
            client.close();
        }
    }


    public class MonitorVariation extends MrvWorkers.MonitorVariation {

        private MongoClient client;
        private MongoCollection productStock;

        public MonitorVariation(String connectionString) throws Exception {
            super();
            client = MongoClients.create(connectionString);
            productStock = client.getDatabase("testdb").getCollection("product_stock");
        }

        @Override
        Map<String, VariationAndZeros> getVariationsAndZeros() throws Exception {
            AggregateIterable<Document> stdevAndAvgs = productStock.aggregate(Arrays.asList(Aggregates.group("$pid",
                    Accumulators.stdDevPop("stdev", "$stock"), Accumulators.avg("avg", "$stock"))));
            AggregateIterable<Document> zeros = productStock.aggregate(Arrays.asList(
                    Aggregates.match(Filters.eq("stock", 0)),
                    Aggregates.group("$pid",Accumulators.sum("count", 1))));

            Map<String, List<Document>> data = new HashMap<>();
            for (Document d : stdevAndAvgs) {
                data.put(d.getString("_id"), new ArrayList<>());
                data.get(d.getString("_id")).add(d);
            }
            for (Document d : zeros) {
                data.get(d.getString("_id")).add(d);
            }

            Map<String, VariationAndZeros> m = new HashMap<>();
            for (Map.Entry<String, List<Document>> e : data.entrySet()) {
                Document stdevAndAvg = e.getValue().get(0);
                int numZeros = e.getValue().size() == 2 ? e.getValue().get(1).getInteger("count") : 0;
                double variation = stdevAndAvg.getDouble("stdev") > 0 ?
                        stdevAndAvg.getDouble("stdev") / stdevAndAvg.getDouble("avg") : 0;
                m.put(e.getKey(), new VariationAndZeros(variation, numZeros));
            }

            return m;
        }
    }
}
