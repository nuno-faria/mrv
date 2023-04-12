import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;

public class Config {

    public String connectionString;
    public String statusTable;
    public int adjustWorkers;
    public int adjustDelta;
    public int adjustWindow;
    public int minAverageAmountPerNode;
    public int maxNodes;
    public int minNodes;
    public double arGoal;
    public double arMin;
    public int balanceWorkers;
    public int balanceDelta;
    public int balanceWindow;
    public int balanceMinmaxK;
    public int balanceMinmaxKRatio;
    public int balanceMinDiff;
    //public List<String> balanceTables;
    public String balanceAlgorithm;
    public boolean monitor;
    public int monitorDelta;
    public List<String> monitorTables;


    public static Config readConfig(String path) throws FileNotFoundException {
        Yaml yaml = new Yaml();
        InputStream inputStream = new FileInputStream(new File(path));
        return yaml.loadAs(inputStream, Config.class);
    }


    @Override
    public String toString() {
        return "Config{" +
                "connectionString='" + connectionString + '\'' +
                ", statusTable='" + statusTable + '\'' +
                ", adjustWorkers=" + adjustWorkers +
                ", adjustDelta=" + adjustDelta +
                ", adjustWindow=" + adjustWindow +
                ", minAverageAmountPerNode=" + minAverageAmountPerNode +
                ", maxNodes=" + maxNodes +
                ", minNodes=" + minNodes +
                ", arGoal=" + arGoal +
                ", arMin=" + arMin +
                ", balanceWorkers=" + balanceWorkers +
                ", balanceDelta=" + balanceDelta +
                ", balanceWindow=" + balanceWindow +
                ", balanceMinmaxK=" + balanceMinmaxK +
                ", balanceMinDiff=" + balanceMinDiff +
                ", balanceAlgorithm='" + balanceAlgorithm + '\'' +
                ", monitor=" + monitor +
                ", monitorDelta=" + monitorDelta +
                ", monitorTables=" + monitorTables +
                '}';
    }
}
