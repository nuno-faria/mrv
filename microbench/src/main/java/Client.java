import transactions.Transactions;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Executes increment/decrement transactions util the time runs out
 */
public class Client implements Runnable {

    private AtomicBoolean over;
    private Transactions transactions;
    private String mode;
    private int pIdLimit;
    private int amountLimit;
    private int productsAccessed;
    private boolean noCollision;
    private int id;
    private String opDistribution;
    private int unevenScale;
    private String accessDistribution;
    private Random rand;
    //private List<Integer> powerLawPids;
    private NavigableMap<Long, Integer> powerLawIds;
    private long powerLawTotal;
    public List<TxResult> results;
    private double hybridReadRatio;
    private String hybridReadRatioUnit;
    private int totalClients;


    public Client(String connectionString, String dbms, String mode, String opDistribution, int unevenScale,
                  String accessDistribution, AtomicBoolean over, Transactions transactions, int pIdLimit, int amountLimit,
                  int productsAccessed, int isolation, boolean noCollision, double hybridReadRatio, String hybridReadRatioUnit,
                  int totalClients, int id) {
        try {
            this.over = over;
            // so transactions objects are not shared
            this.transactions = transactions.getClass().getDeclaredConstructor().newInstance();
            this.transactions.setConnection(connectionString, dbms, isolation);
            this.transactions.setId(id);
            this.mode = mode;
            this.opDistribution = opDistribution;
            this.unevenScale = unevenScale;
            this.accessDistribution = accessDistribution;
            this.pIdLimit = pIdLimit;
            this.amountLimit = amountLimit;
            this.productsAccessed = productsAccessed;
            this.noCollision = noCollision;
            this.hybridReadRatio = hybridReadRatio;
            this.hybridReadRatioUnit = hybridReadRatioUnit;
            this.totalClients = totalClients;
            this.id = id;
            this.results = new ArrayList<>();
            this.rand = new Random();

            if (accessDistribution.equals("powerlaw")) {
                this.powerLawIds = new TreeMap<>();
                double smallestPowerLaw = Math.pow(pIdLimit, -1);
                IntStream.range(0, pIdLimit).forEach(x -> {
                    double powerLawValue = Math.pow(x + 1, -1);
                    int nCopies = (int) (powerLawValue / smallestPowerLaw);
                    powerLawTotal += nCopies;
                    powerLawIds.put(powerLawTotal, x);
                });
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Generates a list of products ids to access based on user preference
     * @return List of product ids
     */
    private List<String> genProducts() {
        List<String> products = new ArrayList<>();
        if (noCollision) {
            products.add("p" + this.id);
        }
        else {
            List<Integer> ids;
            if (accessDistribution.equals("uniform")) {
                ids = IntStream.range(0, pIdLimit).boxed().collect(Collectors.toList());
                Collections.shuffle(ids);
                for (int i = 0; i < Math.min(productsAccessed, pIdLimit); i++) {
                    // this ensures all products are different
                    products.add("p" + ids.get(i));
                }
            }
            else {
                for (int i = 0; i < Math.min(productsAccessed, pIdLimit); i++) {
                    products.add("p" + powerLawIds.higherEntry(ThreadLocalRandom.current().nextLong(0, powerLawTotal)).getValue());
                }
            }
        }
        products.sort(String::compareTo);
        return products;
    }


    @Override
    public void run() {
        int i = 1;
        int unevenAdd = 0;
        while (!over.get()) {
            boolean write = mode.equals("write") || mode.equals("increasedLoad") ||
                    (mode.equals("hybrid") && hybridReadRatioUnit.equals("transactions") && rand.nextDouble() > hybridReadRatio) ||
                    (mode.equals("hybrid") && hybridReadRatioUnit.equals("clients") && id >= totalClients * hybridReadRatio);
            char type;
            boolean result = false;
            long duration = 0;
            List<String> products = genProducts();

            if (write) {
                if (opDistribution.equals("uniform") || opDistribution.equals("adds") || opDistribution.equals("subs")) {
                    int amount = 1 + rand.nextInt(amountLimit);
                    boolean add = opDistribution.equals("adds") || (!opDistribution.equals("subs") && rand.nextBoolean());
                    if (add) {
                        long begin = System.nanoTime();
                        result = transactions.incrementStock(products, amount);
                        duration = System.nanoTime() - begin;
                        type = 'a';
                    }
                    else {
                        long begin = System.nanoTime();
                        result = transactions.decrementStock(products, amount) == 1;
                        duration = System.nanoTime() - begin;
                        type = 's';
                    }
                }
                else {
                    if (i % (unevenScale + 1) == 0) {
                        type = 'a';
                        // adds retried in order to commit
                        while (!result) {
                            long begin = System.nanoTime();
                            // distribute the adds evenly, so we don't end up with products without stock
                            products = List.of("p" + ((this.id + unevenAdd) % this.pIdLimit));
                            result = transactions.incrementStock(products, unevenScale);
                            duration = System.nanoTime() - begin;
                            if (!result) {
                                results.add(new TxResult(System.currentTimeMillis(), false, duration / 1e6, 'a', products.size()));
                            }
                        }
                        unevenAdd += 1;
                    }
                    else {
                        boolean done = false;
                        type = 's';
                        // subs retried if there is still stock
                        while (!done) {
                            long begin = System.nanoTime();
                            int r = transactions.decrementStock(products, 1);
                            duration = System.nanoTime() - begin;
                            if (r == 0) { // aborted due to conflict
                                results.add(new TxResult(System.currentTimeMillis(), false, duration / 1e6, 's', products.size()));
                            }
                            else {
                                result = r == 1;
                                done = true;
                            }
                        }
                    }
                }
            }
            else {
                long begin = System.nanoTime();
                for (String pId: products) {
                    transactions.getStock(pId);
                }
                duration = System.nanoTime() - begin;
                result = true;
                type = 'r';
            }

            results.add(new TxResult(System.currentTimeMillis(), result, duration / 1e6, type, products.size()));
            i += 1;
        }
        transactions.closeConnection();
    }
}
