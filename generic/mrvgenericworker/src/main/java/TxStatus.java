import org.yaml.snakeyaml.Yaml;

import java.util.Map;
import java.util.stream.Collectors;


/**
 * Models the data of the txstatus table
 */
public class TxStatus {

    public String tableName;
    public String columnName;
    public String pkJson;
    public int commits;
    public int aborts;
    public Map<String, Object> pk;
    public String pkCond;
    public String pkColumns;
    public String pkValues;
    public double abortRate;
    public int mrvSize;
    public Number mrvTotal;


    public TxStatus(String tableName, String columnName, String pkJson, int commits, int aborts) {
        Yaml yaml = new Yaml();
        this.tableName = tableName;
        this.columnName = columnName;
        this.pkJson = pkJson;
        this.commits = commits;
        this.aborts = aborts;
        this.pk = yaml.load(pkJson);
        this.pkCond = pk.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(" AND "));
        this.pkColumns = String.join(",", pk.keySet());
        this.pkValues = pk.values().stream().map(Object::toString).collect(Collectors.joining(","));
        this.abortRate = (double) aborts / (aborts + commits);
    }


    public TxStatus(String tableName, String columnName, String pkJson, int commits, int aborts, int mrvSize, Number mrvTotal) {
        this(tableName, columnName, pkJson, commits, aborts);
        this.mrvSize = mrvSize;
        this.mrvTotal = mrvTotal;        
    }


    @Override
	public String toString() {
		return "TxStatus [abortRate=" + abortRate + ", aborts=" + aborts + ", columnName=" + columnName + ", commits="
				+ commits + ", mrvSize=" + mrvSize + ", mrvTotal=" + mrvTotal + ", pk=" + pk + ", pkColumns="
				+ pkColumns + ", pkCond=" + pkCond + ", pkJson=" + pkJson + ", pkValues=" + pkValues + ", tableName="
				+ tableName + "]";
	}
}
