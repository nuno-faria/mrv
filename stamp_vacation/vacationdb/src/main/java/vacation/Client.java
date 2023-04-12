package vacation;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.CollectionSampler;
import org.apache.commons.rng.sampling.DiscreteProbabilityCollectionSampler;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Client extends Thread {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    private final Definitions def;
    private final Jdbi jdbi;

    private final UniformRandomProvider rng;
    private final PermutationSampler baseSampler;
    private final CollectionSampler<ReservationType> typeSampler;
    private final DiscreteProbabilityCollectionSampler<Supplier<Consumer<Handle>>> opSampler;

    private final AtomicBoolean error;
    private final AtomicInteger txn;

    private final Timer txntimer;
    private final Histogram aborts;

    private List<TxStatus> currentQueryStatuses = new ArrayList<>();


    public Client(int cli, Definitions def, Jdbi jdbi, AtomicBoolean error, AtomicInteger txn, MetricRegistry metrics) {
        setName("client-"+cli);
        this.def = def;
        this.jdbi = jdbi;
        this.rng = RandomSource.create(RandomSource.MT);
        this.baseSampler = new PermutationSampler(rng, def.RELATIONS, Math.min(20, def.RELATIONS));
        this.typeSampler = new CollectionSampler<>(rng, Arrays.asList(ReservationType.values()));
        this.opSampler = new DiscreteProbabilityCollectionSampler<>(rng,
                Arrays.asList(this::makeReservation, this::deleteCustomer, this::updateTables),
                new double[]{def.USER, (100-def.USER)/2, (100-def.USER)/2});
        this.error = error;
        this.txn = txn;
        this.txntimer = metrics.timer(MetricRegistry.name(Client.class, "times"));
        this.aborts = metrics.histogram(MetricRegistry.name(Client.class, "aborts"));
    }


    public void run() {
        long end = System.currentTimeMillis() + def.TIME * 1000;
        while(end > System.currentTimeMillis()) {
            logger.debug("starting transaction");
            Consumer<Handle> op = opSampler.sample().get();
            boolean retrying = def.retry;
            int count = 0;
            do {
                try {
                    jdbi.useTransaction(def.isolation, h -> {
                        op.accept(h);
                    });
                    retrying = false;
                    if (def.mrv) {
                        TxStatusWorker.worker.addStatus(currentQueryStatuses);
                    }
                    currentQueryStatuses.clear();
                    try(final Timer.Context context = txntimer.time()) { }
                    logger.debug("transaction success (after {} aborts)", count);
                } catch (Exception e) {
                    if (def.mrv) {
                        try {
                            TxStatus lastStatus = currentQueryStatuses.get(currentQueryStatuses.size() - 1);
                            lastStatus.commit = false;
                            TxStatusWorker.worker.addStatus(lastStatus);
                        }
                        catch (Exception ignored) {}
                    }
                    currentQueryStatuses.clear();
                    Throwable t = e.getCause();
                    if (t instanceof SQLException) {
                        count++;
                        logger.warn("transaction aborted with code: {}", ((SQLException) t).getSQLState());
                    } else {
                        throw e;
                    }
                }
            } while(retrying);
            aborts.update(count);
        }
    }


    public Consumer<Handle> makeReservation() {
        int customerId;
        if (def.XinitialCustomers != 0) {
            customerId = rng.nextInt(def.XinitialCustomers) + 1;
        }
        else {
            customerId = rng.nextInt(def.queryRange) + 1;
        }

        int[] baseIds = baseSampler.sample();
        ReservationType[] types = new ReservationType[def.NUMBER];
        for(int i=0;i<types.length; i++)
            types[i] = typeSampler.sample();

        return h->{
            int[] maxPrices = new int[ReservationType.values().length];
            int[] maxIds = new int[ReservationType.values().length];
            boolean isFound = false;

            for(int i = 0; i< def.NUMBER; i++) {
                ReservationType type = types[i];
                int id = baseIds[i%baseIds.length]+1;
                Optional<Integer> price;
                if (!def.mrv || !def.mrvOpt) {
                    price = h.createQuery("SELECT price FROM <typename>_reservation WHERE id = :id AND numFree > 0")
                            .define("typename", type.toString())
                            .bind("id", id)
                            .mapTo(Integer.class)
                            .findOne();
                }
                else {
                    price = h.createQuery("SELECT price FROM <typename>_reservation WHERE id = :id AND <typename>_reservation_numfree_greater_than(:id, 0)")
                            .define("typename", type.toString())
                            .bind("id", id)
                            .mapTo(Integer.class)
                            .findOne();
                }

                if (price.isPresent() && price.get()>maxPrices[type.getType()]) {
                    maxPrices[type.getType()] = price.get();
                    maxIds[type.getType()] = id;
                    isFound = true;
                }
            }

            if (isFound) {
                Optional<Integer> customer = h.createQuery("SELECT id FROM customer WHERE id = :id")
                        .bind("id", customerId)
                        .mapTo(Integer.class)
                        .findOne();

                if (!customer.isPresent()) {
                    h.createUpdate("INSERT INTO customer VALUES (:id)")
                            .bind("id", customerId)
                            .execute();
                }

                for(int type = 0; type< ReservationType.values().length; type++) {
                    if (maxIds[type]>0) {
                        h.createUpdate("INSERT INTO reservation_info VALUES (?,?,?,?)")
                                .bind(0, customerId)
                                .bind(1, maxIds[type])
                                .bind(2, type)
                                .bind(3, maxPrices[type])
                                .execute();
                        currentQueryStatuses.add(new TxStatus(ReservationType.forType(type).toString() + "_reservation",
                                "numfree", "{ \"id\": " + maxIds[type] + " }", true, "id = " + maxIds[type]));

                        if (!def.mrv || !def.mrvOpt) {
                            h.createUpdate("UPDATE <typename>_reservation " +
                                "SET numFree=numFree-1 " +
                                "WHERE id = ?")
                                .define("typename", ReservationType.forType(type).toString())
                                .bind(0, maxIds[type])
                                .execute();
                        }
                        else {
                            h.createUpdate("SELECT update_<typename>_reservation_numfree(?, -1)")
                                .define("typename", ReservationType.forType(type).toString())
                                .bind(0, maxIds[type])
                                .execute();
                        }
                    }
                }
            }
        };
    }

    public Consumer<Handle> deleteCustomer() {
        int customerId = rng.nextInt(def.queryRange) + 1;

        return h-> {
            h.createUpdate("DELETE FROM customer WHERE id = ?")
                    .bind(0, customerId)
                    .execute();

            Optional<Integer> bill = h.createQuery("SELECT SUM(price) FROM reservation_info WHERE customer_id = ?")
                    .bind(0, customerId)
                    .mapTo(Integer.class)
                    .findOne();

            if (bill.isPresent()) {
                for (ReservationType rt : ReservationType.values()) {
                    currentQueryStatuses.add(new TxStatus(
                            String.format("INSERT INTO tx_status " +
                                            "SELECT '%s_reservation', 'numfree', '{ \"id\": ' || id || ' }', <placeholder>, <placeholder>, now(), 'id = ' || id " +
                                            "FROM (SELECT DISTINCT id FROM reservation_info WHERE type = %d AND customer_id = %d) AS T " +
                                            "ON CONFLICT (table_name, column_name, pk) DO UPDATE " +
                                            "SET commits = tx_status.commits + <placeholder>, " +
                                            "aborts = tx_status.aborts + <placeholder>, last_updated = now()",
                                    rt.toString(), rt.getType(), customerId), true));
                    if (def.Xordered) {
                        if (!def.mrv || !def.mrvOpt) {
                            h.createUpdate("UPDATE <typename>_reservation  " +
                                    "SET numFree = numFree + 1 " +
                                    "WHERE id in ( " +
                                    "    SELECT id " +
                                    "    FROM <typename>_reservation  " +
                                    "    WHERE id IN ( " +
                                    "        SELECT id  " +
                                    "        FROM reservation_info  " +
                                    "        WHERE type = <typevalue> AND customer_id = ? " +
                                    "    ) " +
                                    "    ORDER BY id " +
                                    "    FOR UPDATE " +
                                    ") ")
                                    .define("typename", rt.toString())
                                    .define("typevalue", rt.getType())
                                    .bind(0, customerId)
                                    .execute();
                        }
                        else {
                            h.createUpdate("SELECT update_<typename>_reservation_numfree(id, 1) " +
                                    "FROM ( " +
                                    "    SELECT id " +
                                    "    FROM <typename>_reservation  " +
                                    "    WHERE id IN ( " +
                                    "        SELECT id  " +
                                    "        FROM reservation_info  " +
                                    "        WHERE type = <typevalue> AND customer_id = ? " +
                                    "    ) " +
                                    "    ORDER BY id " +
                                    "    FOR UPDATE " +
                                    ") T ")
                                    .define("typename", rt.toString())
                                    .define("typevalue", rt.getType())
                                    .bind(0, customerId)
                                    .execute();
                        }
                    }
                    else {
                        if (!def.mrv || !def.mrvOpt) {
                            h.createUpdate("UPDATE <typename>_reservation SET numFree = numFree + 1 " +
                                    "WHERE id in (SELECT id FROM reservation_info WHERE type = <typevalue> AND customer_id = ?)")
                                    .define("typename", rt.toString())
                                    .define("typevalue", rt.getType())
                                    .bind(0, customerId)
                                    .execute();
                        }
                        else {
                            h.createUpdate("SELECT update_<typename>_reservation_numfree(id, 1) " +
                                    "FROM ( " +
                                    "   SELECT id FROM reservation_info WHERE type = <typevalue> AND customer_id = ? " +
                                    ") T ")
                                    .define("typename", rt.toString())
                                    .define("typevalue", rt.getType())
                                    .bind(0, customerId)
                                    .execute();
                        }
                    }
                }
                h.createUpdate("DELETE FROM reservation_info WHERE customer_id = ?")
                        .bind(0, customerId)
                        .execute();
            }
        };
    }


    public class UpdateTableInfo implements Comparable<UpdateTableInfo> {
        public int id;
        public ReservationType type;
        public boolean op;
        public int price;

        public UpdateTableInfo(int id, ReservationType type, boolean op, int price) {
            this.id = id;
            this.type = type;
            this.op = op;
            this.price = price;
        }


        @Override
        public int compareTo(UpdateTableInfo o) {
            if (this.id == o.id && this.type == o.type) {
                return 0;
            }
            else {
                return this.type != o.type ? this.type.ordinal() - o.type.ordinal() : this.id - o.id;
            }
        }

        @Override
        public String toString() {
            return "UpdateTableInfo{" +
                    "id=" + id +
                    ", type=" + type +
                    ", op=" + op +
                    ", price=" + price +
                    '}';
        }
    }


    public Consumer<Handle> updateTables() {
        int[] baseIds = baseSampler.sample();
        ReservationType[] types = new ReservationType[def.NUMBER];
        boolean[] ops = new boolean[def.NUMBER];
        int[] prices = new int[def.NUMBER];
        for(int i=0;i<def.NUMBER; i++) {
            types[i] = typeSampler.sample();
            ops[i] = rng.nextInt(2) != 0;
            prices[i] = (rng.nextInt(5) * 10) + 50;
        }

        Collection<UpdateTableInfo> updateInfo;
        if (def.Xordered) {
            updateInfo = new TreeSet<>();
        }
        else {
            updateInfo = new ArrayList<>();
        }
        for (int i = 0; i < def.NUMBER; i++) {
            UpdateTableInfo info = new UpdateTableInfo(baseIds[i % baseIds.length] + 1, types[i], ops[i], prices[i]);
            if (!updateInfo.contains(info)) { // contains necessary for arraylist
                updateInfo.add(info);
            }
        }

        return h-> {
            for (UpdateTableInfo info : updateInfo) {
                int id = info.id;
                ReservationType type = info.type;
                boolean op = info.op;
                int price = info.price;
                if (op) {
                    currentQueryStatuses.add(new TxStatus(type.toString() + "_reservation",
                            Arrays.asList("numtotal", "numfree"), "{ \"id\": " + id + " }", true, "id = " + id));

                    // Delete
                    if (!def.mrv || !def.mrvOpt) {
                        h.createUpdate("UPDATE <typename>_reservation " +
                                "SET numTotal=numTotal-100, numFree=numFree-100 " +
                                "WHERE id = :id AND numFree >= 100")
                                .define("typename", type.toString())
                                .bind("id", id)
                                .execute();
                    }
                    else {
                        h.createUpdate("SELECT update_<typename>_reservation_numtotal(:id, -100), update_<typename>_reservation_numfree(:id, -100) " +
                                        "FROM <typename>_reservation " + 
                                        "WHERE id = :id AND <typename>_reservation_numfree_greater_or_equal(:id, 100)")
                                .define("typename", type.toString())
                                .bind("id", id)
                                .execute();
                    }
                } else {
                    // Add
                    if (!def.mrv || !def.mrvOpt) {
                        currentQueryStatuses.add(new TxStatus(type.toString() + "_reservation",
                                Arrays.asList("numtotal", "numfree", "price"), "{ \"id\": " + id + " }", true, "id = " + id));
                        h.createUpdate("UPDATE <typename>_reservation " +
                                "SET numTotal=numTotal+100, numFree=numFree+100, price=:price "+
                                "WHERE id = :id")
                                .define("typename", type.toString())
                                .bind("id", id)
                                .bind("price", price)
                                .execute();
                    }
                    else {
                        h.createUpdate("UPDATE <typename>_reservation " +
                                "SET price=:price "+
                                "WHERE id = :id")
                                .define("typename", type.toString())
                                .bind("id", id)
                                .bind("price", price)
                                .execute();
                        currentQueryStatuses.add(new TxStatus(type.toString() + "_reservation",
                                Arrays.asList("numtotal", "numfree", "price"), "{ \"id\": " + id + " }", true, "id = " + id));
                        h.createUpdate("SELECT update_<typename>_reservation_numtotal(:id, 100), update_<typename>_reservation_numfree(:id, 100)")
                            .define("typename", type.toString())
                            .bind("id", id)
                            .execute();
                    }
                }
            }
        };
    }
}
