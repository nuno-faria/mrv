package vacation;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleToIntFunction;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    @Parameter(names = {"-h","-?","--help"}, help = true, description = "display usage information")
    public boolean help;

    @Parameter(names = {"-o","--output"}, description = "output CSV files to folder")
    public String output = null;

    @Parameter(names = {"--no-create"}, description = "skip table creation")
    public boolean nocreate = false;

    @Parameter(names = {"--delete"}, description = "delete data before population")
    public boolean delete = false;

    @Parameter(names = {"--no-populate"}, description = "skip table population")
    public boolean nopopulate = false;

    @Parameter(names = {"--no-run"}, description = "skip running the benchmark")
    public boolean norun = false;

    @Parameter(names = {"--no-drop"}, description = "skip table population")
    public boolean nodrop = false;

    public static void main(String[] args) throws Exception {

        Definitions def = new Definitions();
        Main main = new Main();

        JCommander parser = JCommander.newBuilder()
                .addObject(def)
                .addObject(main)
                .build();
        try {
            parser.parse(args);

            if (main.help) {
                parser.usage();
                return;
            }

            def.update();
        } catch(Exception e) {
            logger.error("invalid options: {}", e.getMessage());
            parser.usage();
            return;
        }

        Database database = new Database(def);

        if (!main.nocreate)
            database.createTables();
        if (main.delete)
            database.deleteTables();
        if (!main.nopopulate)
            database.populateTables();

        if (!main.norun) {

            MetricRegistry metrics = new MetricRegistry();

            ConsoleReporter console = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();

            JmxReporter jmx = JmxReporter.forRegistry(metrics).build();
            jmx.start();

            CsvReporter csv = null;

            if (main.output != null) {
                csv = CsvReporter.forRegistry(metrics)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build(new File(main.output));
            }

            AtomicBoolean error = new AtomicBoolean(false);
            AtomicInteger txn = new AtomicInteger(0);

            if (def.mrv) {
                TxStatusWorker txStatusWorker = new TxStatusWorker(database.getConnection());
                new Thread(txStatusWorker).start();
            }

            Client[] c = new Client[def.CLIENTS];
            for (int i = 0; i < c.length; i++)
                c[i] = new Client(i + 1, def, database.getConnection(), error, txn, metrics);

            logger.info("starting {} client threads", def.CLIENTS);

            for (int i = 0; i < c.length; i++)
                c[i].start();

            if (csv != null)
                csv.start(1, TimeUnit.SECONDS);

            for (int i = 0; i < c.length; i++)
                c[i].join();

            if (csv != null)
                csv.stop();

            logger.info("all client threads stopped");

            if (error.get())
                logger.error("terminating after unexpected error");
            else
                console.report();
        }

        if (!main.nodrop)
            database.dropTables();

        System.exit(0);
    }
}
