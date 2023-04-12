package vacation;

import java.util.Arrays;
import java.util.List;

public class TxStatus {

    public String type;
    public String tableName;
    public List<String> columnNames;
    public String pk;
    public String query;
    public boolean commit;
    public String pkSql;


    public TxStatus(String tableName, String columnName, String pk, boolean commit, String pkSql) {
        this(tableName, Arrays.asList(columnName), pk, commit, pkSql);
    }


    public TxStatus(String tableName, List<String> columnNames, String pk, boolean commit, String pkSql) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.pk = pk;
        this.commit = commit;
        this.type = "data";
        this.pkSql = pkSql;
    }


    public TxStatus(String query, boolean commit) {
        this.query = query;
        this.commit = commit;
        this.type = "query";
    }
}
