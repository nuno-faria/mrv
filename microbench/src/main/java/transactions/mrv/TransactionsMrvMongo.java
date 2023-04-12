package transactions.mrv;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import transactions.Transactions;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TransactionsMrvMongo implements Transactions {

    private MongoClient client;
    private static int maxNodes;
    private Random rand;
    private ClientSession session;
    private MongoDatabase database;
    private MongoCollection productsStock;
    private TransactionOptions transactionOptions;
    private int id;


    public TransactionsMrvMongo() {
        rand = new Random();
    }


    @Override
    public void setId(int id) {
        this.id = id;
    }


    @Override
    public void populate(String connectionString, String dbms, int pIdLimit, int initialStock, Map<String, Object> extraConfigs) {
        maxNodes = (int) extraConfigs.get("maxNodes");
        int initialNodes = Math.min((int) extraConfigs.get("initialNodes"), maxNodes);
        int zeroNodesPercentage = (int) extraConfigs.get("zeroNodesPercentage");
        int nodesWithStock = Math.max(1, initialNodes - initialNodes * zeroNodesPercentage / 100);
        int stockPerNode = initialStock / nodesWithStock;

        MongoClient cl = MongoClients.create(connectionString);
        MongoDatabase database = cl.getDatabase("testdb");
        MongoCollection productOrig = database.getCollection("product_orig");
        MongoCollection productStock = database.getCollection("product_stock");
        MongoCollection productTx = database.getCollection("product_tx");
        productOrig.drop();
        productStock.drop();
        productTx.drop();

        List<Document> productOrigList = new ArrayList<>();
        List<Document> productStockList = new ArrayList<>();
        List<Document> productTxList = new ArrayList<>();
        for (int i=0; i<pIdLimit; i++) {
            Document pOrig = new Document();
            pOrig.put("pid", "p" + i);
            productOrigList.add(pOrig);

            List<Integer> range = IntStream.range(0, maxNodes).boxed().collect(Collectors.toList());
            for (int j=0; j<initialNodes; j++) {
                Document pStock = new Document();
                pStock.put("pid", "p" + i);
                pStock.put("rk", range.remove(rand.nextInt(range.size())));

                pStock.put("stock", j < nodesWithStock ? stockPerNode : 0);
                productStockList.add(pStock);
            }

            Document pTx = new Document();
            pTx.put("pid", "p" + i);
            pTx.put("commits", 0);
            pTx.put("aborts", 0);
            pTx.put("last_updated", LocalDateTime.now());
            productTxList.add(pTx);
        }

        productOrig.insertMany(productOrigList);
        productStock.insertMany(productStockList);
        productTx.insertMany(productTxList);
        productOrig.createIndex(new Document("pid", 1), new IndexOptions().unique(true));
        Document stockIndex = new Document();
        stockIndex.put("pid", 1);
        stockIndex.put("rk", 1);
        productStock.createIndex(stockIndex, new IndexOptions().unique(true));
        productOrig.createIndex(new Document("pid", 1), new IndexOptions().unique(true));
        cl.close();
    }


    @Override
    public String getType() {
        return "mrv";
    }


    @Override
    public void setConnection(String connectionString, String dmbs, int isolation) {
        client = MongoClients.create(connectionString);
        session = client.startSession();
        transactionOptions = TransactionOptions.builder()
                .readPreference(ReadPreference.primary())
                .readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY)
                .build();
        database = client.getDatabase("testdb");
        productsStock = database.getCollection("product_stock");
    }


    public void closeConnection() {
        session.close();
        client.close();
    }


    private void endTransaction(String pId, boolean committed) {
        MrvWorkersMongo.addTxResult(pId, committed);
    }


    private Document lookup(String pId, int rk) {
        Document node = ((Document) productsStock.find(
                Filters.and(Filters.eq("pid", pId), Filters.gte("rk", rk))
            ).sort(Sorts.ascending("rk")).limit(1).first());
        if (node == null) {
            node = ((Document) productsStock.find(
                Filters.and(Filters.eq("pid", pId), Filters.gte("rk", 0))
            ).sort(Sorts.ascending("rk")).limit(1).first());
        }
        return node;
    }


    @Override
    public int decrementStock(List<String> pIds, int amount) {
        String p = pIds.get(0);
        try {
            session.startTransaction(transactionOptions);
            for (String pId: pIds) {
                p = pId;
                int rk = rand.nextInt(maxNodes);
                Document node = lookup(pId, rk);
                int initialRk = node.getInteger("rk");
                boolean done = false;

                while (amount > 0 && !done) {
                    int nodeStock = node.getInteger("stock");
                    if (nodeStock > 0) {
                        int amountToDecrement = Math.min(amount, nodeStock);
                        productsStock.updateOne(session, node, Updates.inc("stock", -amountToDecrement));
                        amount -= amountToDecrement;
                    }
                    if (amount > 0) {
                        rk = rk + 1;
                        node = lookup(pId, rk);
                        done = node.getInteger("rk") == initialRk;
                    }
                }

                if (amount > 0) {
                    session.abortTransaction();
                    return -1;
                }
            }
            session.commitTransaction();
            endTransaction(p, true);
            return 1;
        }
        catch (Exception e) {
            session.abortTransaction();
            endTransaction(p, false);
            return 0;
        }
    }


    @Override
    public boolean incrementStock(List<String> pIds, int amount) {
        String p = pIds.get(0);
        try {
            session.startTransaction(transactionOptions);
            for (String pId: pIds) {
                p = pId;
                int rk = rand.nextInt(maxNodes);
                Document node = lookup(pId, rk);
                productsStock.updateOne(session, node, Updates.inc("stock", amount));
            }
            session.commitTransaction();
            endTransaction(p, true);
            return true;
        }
        catch (Exception e) {
            session.abortTransaction();
            endTransaction(p, false);
            return false;
        }
    }

    
    @Override
    public int getStock(String pId) {
        session.startTransaction(transactionOptions);
        Document d = (Document) productsStock.aggregate(session, Arrays.asList(
            Aggregates.match(Filters.eq("pid", pId)),
            Aggregates.group("$pid", Accumulators.sum("total", "$stock"))
        )).first();
        int stock = (int) d.get("total");
        session.commitTransaction();
        return stock;
    }
}
