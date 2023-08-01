import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Stores the benchmark configurations
 */
public class Config {

    public List<String> servers;
    public String dbms;
    public String database;
    public String user;
    public String pass;
    public String projectId;
    public String instance;
    public String credentials;
    public List<String> connectionStrings;
    public String mode;
    public List<Integer> loadIncreases;
    public int loadIncrease;
    public String opDistribution;
    public List<Integer> unevenScales;
    public int unevenScale;
    public String accessDistribution;
    public List<Integer> clients;
    public int cli;
    public List<Integer> sizes;
    public int size;
    public List<Integer> initialStocks;
    public int initialStock;
    public int time;
    public int timeBucketErr;
    public int cooldown;
    public List<Integer> productsAccessed;
    public int pAccessed;
    public List<Integer> amountLimits;
    public int amountLimit;
    public List<Integer> isolations;
    public int isolation;
    public List<Boolean> noCollisions;
    public boolean noCollision;
    public List<String> types;
    public String type;
    public PrintWriter out;
    public List<Double> hybridReadRatios;
    public double hybridReadRatio;
    public String hybridReadRatioUnit;

    //mrv only
    public int maxNodes;
    public int minNodes;
    public List<Integer> initialNodes;
    public int initNodes;
    public List<Integer> zeroNodesPercentages;
    public int zeroNodesPercentage;
    public List<Integer> balanceDeltas;
    public int balanceDelta;
    public List<String> balanceAlgorithms;
    public String balanceAlgorithm;
    public List<Integer> balanceMinmaxKs;
    public int balanceMinmaxK;
    public List<Integer> balanceMinmaxKRatios;
    public int balanceMinmaxKRatio;
    public List<Integer> balanceMinDiffs;
    public int balanceMinDiff;
    public List<Integer> balanceWindows;
    public int balanceWindow;
    public List<String> adjustAlgorithms;
    public String adjustAlgorithm;
    public List<Integer> adjustDeltas;
    public int adjustDelta;
    public List<Integer> adjustWindows;
    public int adjustWindow;
    public int monitorDelta;
    public PrintWriter outMonitor;
    public String workers;
    public double arGoal;
    public double arMin;
    public int minAverageAmountPerNode;
    public int distributeAddsSize;

    // phase reconfiguration only
    public int phaseDeltaMillis;
    public List<Double> abortRatesToSplit;
    public double abortRateToSplit;
    public List<Double> waitingRatiosToJoin;
    public double waitingRatioToJoin;
    public List<Double> noStockRatiosToJoin;
    public double noStockRatioToJoin;


    // current configurations
    private int currentBenchmarkConfigIndex = 0;
    private int currentMrvConfigIndex = 0;
    private int currentPhaseReconciliationConfigIndex = 0;
    private List<List<Object>> possibleBenchmarkConfigs;
    private List<List<Object>> possibleMrvConfigs;
    private List<List<Object>> possiblePhaseReconciliationConfigs;


    /**
     * Populates a config object with the info from the configuration file (YAML) at 'path'
     * @param path Path of the config file
     * @return Config object
     */
    public static Config readConfig(String path) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(path));
        Config config = yaml.loadAs(inputStream, Config.class);

        config.connectionStrings = new ArrayList<>();
        if (config.dbms.equals("postgresql") || config.dbms.equals("mysql") || config.dbms.equals("mariadb")) {
            config.servers.forEach(server ->
                    config.connectionStrings.add(
                    "jdbc:" + config.dbms + "://"
                    + server + "/"
                    + config.database
                    + "?user=" + config.user
                    + "&password=" + config.pass));
        }
        else if (config.dbms.equals("mongodb")) {
            StringBuilder s = new StringBuilder("mongodb://");
            if (config.user != null && config.pass != null) {
                s.append(config.user + ":" + config.pass + "@");
            }
            for (String server : config.servers) {
                s.append(server).append(",");
            }
            s.deleteCharAt(s.length()-1);
            s.append("/admin?replicaSet=replica_set");
            config.connectionStrings.add(s.toString());
        }
        else {
            System.err.println("Unsupported database engine: " + config.dbms);
            System.exit(1);
        }

        if (!config.mode.equals("increasedLoad")) {
            config.loadIncreases = Arrays.asList(1);
        }

        config.possibleBenchmarkConfigs = listsProduct(0, config.clients, config.sizes, config.initialStocks,
                config.productsAccessed, config.amountLimits, config.isolations, config.noCollisions, config.types,
                config.hybridReadRatios, config.unevenScales, config.loadIncreases);

        config.possibleMrvConfigs = listsProduct(0, config.initialNodes, config.zeroNodesPercentages,
                config.balanceAlgorithms, config.balanceDeltas, config.balanceMinmaxKs, config.balanceMinmaxKRatios,
                config.balanceMinDiffs, config.balanceWindows, config.adjustAlgorithms, config.adjustDeltas,
                config.adjustWindows);

        config.possiblePhaseReconciliationConfigs = listsProduct(0, config.abortRatesToSplit,
                config.waitingRatiosToJoin, config.noStockRatiosToJoin);
        return config;
    }


    /**
     * Computes the product of multiples lists
     * @param lists List of possible combinations
     */
    private static List<List<Object>> listsProduct(int i, List<?> ... lists) {
        List<List<Object>> combinations = new ArrayList<>();
        if (i == lists.length) {
            combinations.add(new ArrayList<>());
        }
        else {
            for (Object o : lists[i]) {
                for (List<Object> l: listsProduct(i + 1, lists)) {
                    l.add(0, o);
                    combinations.add(l);
                }
            }
        }
        return combinations;
    }


    /**
     * Updates the configurations with the next benchmark config combination
     * @return True if there is a next config, False if not
     */
    public boolean nextBenchmarkConfig() {
        if (currentBenchmarkConfigIndex == possibleBenchmarkConfigs.size()) {
            return false;
        }
        else {
            List<Object> currentConfigs = possibleBenchmarkConfigs.get(currentBenchmarkConfigIndex);
            cli = (int) currentConfigs.get(0);
            size = (int) currentConfigs.get(1);
            initialStock = (int) currentConfigs.get(2);
            pAccessed = (int) currentConfigs.get(3);
            amountLimit = (int) currentConfigs.get(4);
            isolation = (int) currentConfigs.get(5);
            noCollision = (boolean) currentConfigs.get(6);
            type = (String) currentConfigs.get(7);
            hybridReadRatio = (double) currentConfigs.get(8);
            unevenScale = (int) currentConfigs.get(9);
            loadIncrease = (int) currentConfigs.get(10);
            currentBenchmarkConfigIndex++;
            return true;
        }
    }

    /**
     * Updates the configurations with the next workers config combination
     * @return True if there is a next config, False if not
     */
    public boolean nextMrvConfig() {
        if (currentMrvConfigIndex == possibleMrvConfigs.size()) {
            currentMrvConfigIndex = 0;
            return false;
        }
        else {
            List<Object> currentConfigs = possibleMrvConfigs.get(currentMrvConfigIndex);
            initNodes = (int) currentConfigs.get(0);
            zeroNodesPercentage = (int) currentConfigs.get(1);
            balanceAlgorithm = (String) currentConfigs.get(2);
            balanceDelta = (int) currentConfigs.get(3);
            balanceMinmaxK = (int) currentConfigs.get(4);
            balanceMinmaxKRatio = (int) currentConfigs.get(5);
            balanceMinDiff = (int) currentConfigs.get(6);
            balanceWindow = (int) currentConfigs.get(7);
            adjustAlgorithm = (String) currentConfigs.get(8);
            adjustDelta = (int) currentConfigs.get(9);
            adjustWindow = (int) currentConfigs.get(10);
            currentMrvConfigIndex++;
            return true;

        }
    }


    /**
     * Updates the configurations with the next phase reconfiguration combination
     * @return True if there is a next config, False if not
     */
    public boolean nextPhaseReconciliationConfig() {
        if (currentPhaseReconciliationConfigIndex == possiblePhaseReconciliationConfigs.size()) {
            currentPhaseReconciliationConfigIndex = 0;
            return false;
        }
        else {
            List<Object> currentConfigs = possiblePhaseReconciliationConfigs.get(currentPhaseReconciliationConfigIndex);
            abortRateToSplit = (double) currentConfigs.get(0);
            waitingRatioToJoin = (double) currentConfigs.get(1);
            noStockRatioToJoin = (double) currentConfigs.get(2);
            currentPhaseReconciliationConfigIndex++;
            return true;

        }
    }


    @Override
    public String toString() {
        return "Config{" +
                "mode='" + mode + '\'' +
                ", loadIncrease=" + loadIncrease +
                ", opDistribution='" + opDistribution + '\'' +
                ", unevenScale=" + unevenScale +
                ", accessDistribution='" + accessDistribution + '\'' +
                ", clients=" + cli +
                ", pIdLimit=" + size +
                ", initialStock=" + initialStock +
                ", productsAccessed=" + pAccessed +
                ", amountLimit=" + amountLimit +
                ", isolation=" + isolation +
                ", noCollision=" + noCollision +
                ", type=" + type +
                ", initialNodes=" + initNodes +
                ", zeroNodesPercentage=" + zeroNodesPercentage +
                ", workers=" + workers +
                ", balanceDelta=" + balanceDelta +
                ", balanceAlgorithm='" + balanceAlgorithm + '\'' +
                ", balanceMinmaxK=" + balanceMinmaxK +
                ", balanceMinmaxKRatio=" + balanceMinmaxKRatio +
                ", balanceMinDiff=" + balanceMinDiff +
                ", balanceWindow=" + balanceWindow +
                ", adjustAlgorithm='" + adjustAlgorithm + '\'' +
                ", adjustDelta=" + adjustDelta +
                ", adjustWindow=" + adjustWindow +
                ", hybridReadRatio=" + hybridReadRatio +
                ", hybridReadRatioUnit=" + hybridReadRatioUnit +
                ", abortRateToSplit=" + abortRateToSplit +
                ", waitingRatioToJoin=" + waitingRatioToJoin +
                ", noStockRatioToJoin=" + noStockRatioToJoin +
                '}';
    }
}
