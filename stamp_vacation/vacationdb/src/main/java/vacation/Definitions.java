package vacation;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.EnumConverter;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;

@Parameters(commandDescription = "STAMP Vacation benchmark for SQL databases.")
public class Definitions {
    private static class IsolationConverter extends EnumConverter<TransactionIsolationLevel> {
        public IsolationConverter() {
            super("isolation", TransactionIsolationLevel.class);
        }
    }

    @Parameter(names = {"-d","--database"}, description = "JDBC database url")
    public String database = "jdbc:derby:memory:vacation;create=true";

    @Parameter(names = {"-U","--dbuser"}, description = "database user name")
    public String user = null;

    @Parameter(names = {"-P","--dbpassword"}, description = "database password")
    public String passwd = null;

    @Parameter(names = {"-i","--isolation"}, description = "isolation level", converter = IsolationConverter.class)
    public TransactionIsolationLevel isolation = TransactionIsolationLevel.REPEATABLE_READ;

    @Parameter(names = {"-r","--relations"}, description = "number of rows in relations")
    public int RELATIONS = 1<<16;

    @Parameter(names = {"-t","--time"}, description = "duration of the benchmark in seconds")
    public int TIME = 1<<26;

    @Parameter(names = {"-c","--clients"}, description = "number of concurrent clients")
    public int CLIENTS = 1;

    @Parameter(names = {"-q","--queried"}, description = "% of data queried")
    public int QUERIES = 90;

    @Parameter(names = {"-n","--number"}, description = "number of queries in each transaction")
    public int NUMBER = 10;

    @Parameter(names = {"-u","--user"}, description = "% of user transactions (MakeReservation)")
    public int USER = 80;

    @Parameter(names = {"-y","--retry"}, description = "retry deadlocked/conflict transactions")
    public boolean retry = false;

    @Parameter(names = {"-m", "--mrv"}, description = "whether it is using multi record values or not (to add tx status to db")
    public boolean mrv = false;

    @Parameter(names = {"--Xordered"}, description = "ensure all transactions update items in the same order (to avoid deadlocks)")
    public boolean Xordered = false;

    @Parameter(names = {"--XinitialCustomers"}, description = "sets the number of initial customers to some constant "
            + "(since for small r the number of customers is also small, leading to multiple threads using the same "
            + "customer and generating unnecessary aborts).")
    public int XinitialCustomers = 0;

    @Parameter(names = {"--XinitialStockMultiplier"}, description = "sets the initial stock between XinitialStockMultiplier * 1 "
            + "and XinitialStockMultiplier * 5 (higher initial stock required for workloads with low r, to avoid unnecessary aborts).")
    public int XinitialStockMultiplier = 100;

    @Parameter(names = {"--mrv-opt"}, description = "to use an optimized (less generic and requires more changes to the application) versions of mrvs )")
    public boolean mrvOpt = false;

    public int queryRange;

    public void update() {
        queryRange = (int) (QUERIES / 100.0 * RELATIONS + 0.5);
    }
}
