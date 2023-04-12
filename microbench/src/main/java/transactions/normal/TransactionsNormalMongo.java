package transactions.normal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import transactions.Transactions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransactionsNormalMongo implements Transactions {

    private MongoClient client;
    private ClientSession session;
    private TransactionOptions transactionOptions;
    private MongoDatabase database;
    private MongoCollection products;
    private int id;


    @Override
    public void setId(int id) {
        this.id = id;
    }


    @Override
    public void populate(String connectionString, String dbms, int pIdLimit, int initialStock, Map<String, Object> extraConfigs) {
        MongoClient cl = MongoClients.create(connectionString);
        MongoCollection collection = cl.getDatabase("testdb").getCollection("product");
        collection.drop();

        List<Document> l = new ArrayList<>();
        for (int i=0; i<pIdLimit; i++) {
            Document d = new Document();
            d.put("pid", "p" + i);
            d.put("stock", initialStock);
            l.add(d);
        }

        collection.insertMany(l);
        collection.createIndex(new Document("pid", 1), new IndexOptions().unique(true));
        cl.close();
    }


    @Override
    public String getType() {
        return "normal";
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
        products = database.getCollection("product");
    }


    public void closeConnection() {
        session.close();
        client.close();
    }


    @Override
    public int decrementStock(List<String> pIds, int amount) {
        try {
            session.startTransaction(transactionOptions);
            for (String pId: pIds) {
                Document search = new Document();
                search.put("pid", pId);

                Document row = (Document) products.find(session, search).limit(1).first();
                int stock = row.getInteger("stock");

                if (stock >= amount) {
                    products.updateOne(session, search, Updates.inc("stock", -amount));
                }
                else {
                    session.abortTransaction();
                    return -1;
                }
            }
            session.commitTransaction();
            return 1;
        }
        catch (Exception e) {
            session.abortTransaction();
            return 0;
        }
    }


    @Override
    public boolean incrementStock(List<String> pIds, int amount) {
        try {
            session.startTransaction(transactionOptions);
            for (String pId: pIds) {
                Document search = new Document();
                search.put("pid", pId);
                products.updateOne(session, search, Updates.inc("stock", amount));
            }
            session.commitTransaction();
            return true;
        }
        catch (Exception e) {
            session.abortTransaction();
            return false;
        }
    }


    @Override
    public int getStock(String pId) {
        session.startTransaction(transactionOptions);
        Document search = new Document();
        search.put("pid", pId);
        Document d = (Document) products.find(search).projection(Projections.include("stock")).first();
        int stock = (int) d.get("stock");
        session.commitTransaction();
        return stock;
    }
}
