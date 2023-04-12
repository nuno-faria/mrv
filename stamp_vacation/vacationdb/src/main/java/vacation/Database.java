package vacation;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.sampling.PermutationSampler;
import org.apache.commons.rng.simple.RandomSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.SqlLogger;
import org.jdbi.v3.core.statement.StatementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Database {
    private static Logger logger = LoggerFactory.getLogger(Database.class);

    private final Definitions param;
    private final Jdbi jdbi;
    private final UniformRandomProvider rng;
    private final PermutationSampler ps;

    public Database(Definitions def) {
        this.param = def;
        this.rng = RandomSource.create(RandomSource.MT);
        this.ps = new PermutationSampler(rng, def.RELATIONS, def.RELATIONS);

        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(def.database);
        if (def.user != null) {
            ds.setUsername(def.user);
            ds.setPassword(def.passwd);
        }
        ds.setMaximumPoolSize(50);
        jdbi = Jdbi.create(ds);

        jdbi.setSqlLogger(new SqlLogger() {
            @Override
            public void logBeforeExecution(StatementContext context) {
                logger.trace("SQL before: {}", context.getRenderedSql());
            }

            @Override
            public void logAfterExecution(StatementContext context) {
            }

            @Override
            public void logException(StatementContext context, SQLException ex) {
                //if (ex.getSQLState().startsWith("40")) {
                logger.debug("transaction rollback", ex);
                //} else
                //    logger.error("unexptected SQL error", ex);
            }
        });
    }

    public Jdbi getConnection() {
        return jdbi;
    }

    public void createTables() {
        logger.info("creating tables");
        jdbi.useHandle(h -> {
            h.execute("CREATE TABLE customer (id INTEGER PRIMARY KEY)");

            h.execute("CREATE TABLE reservation_info (customer_id INTEGER, id INTEGER, type INTEGER, price INTEGER)");

            for (ReservationType rt : ReservationType.values())
                h.createUpdate("CREATE TABLE <typename>_reservation (id INTEGER PRIMARY KEY, numFree INTEGER, numTotal INTEGER, price INTEGER)")
                        .define("typename", rt.toString())
                        .execute();
        });
    }

    public void populateTables() {
        logger.info("inserting table content");
        jdbi.useHandle(h -> {
            int i = 0;

            PreparedBatch batch = h.prepareBatch("INSERT INTO customer (id) VALUES (?)");
            int[] customerIds = param.XinitialCustomers != 0 ?
                                            IntStream.range(0, param.XinitialCustomers).toArray() : ps.sample();
            for (int id : customerIds) {
                batch.add(id + 1);
                if (i%1000 == 0)
                    batch.execute();
                i += 1;
            }
            batch.execute();

            i = 0;
            for (ReservationType rt : ReservationType.values()) {
                batch = h.prepareBatch("INSERT INTO <typename>_reservation VALUES (?,?,?,?)")
                        .define("typename", rt.toString());
                for (int id : ps.sample()) {
                    int numTotal = (rng.nextInt(5) + 1) * param.XinitialStockMultiplier;
                    batch.add(id + 1, numTotal, numTotal, (rng.nextInt(5) * 10) + 50);
                    if (i%1000 == 0)
                        batch.execute();
                    i += 1;
                }
                batch.execute();
            }
        });
    }

    public void deleteTables() {
        logger.info("deleting table content");
        jdbi.useHandle(h -> {
            h.execute("DELETE FROM customer");

            h.execute("DELETE FROM reservation_info");

            for (ReservationType rt : ReservationType.values())
                h.createUpdate("DELETE FROM <typename>_reservation")
                        .define("typename", rt.toString())
                        .execute();
        });
    }

    public void dropTables() {
        logger.info("dropping tables");
        jdbi.useHandle(h -> {
            h.execute("DROP TABLE customer");

            h.execute("DROP TABLE reservation_info");

            for (ReservationType rt : ReservationType.values())
                h.createUpdate("DROP TABLE <typename>_reservation")
                        .define("typename", rt.toString())
                        .execute();
        });
    }
}
