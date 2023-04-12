package transactions;

import java.util.List;
import java.util.Map;

public interface Transactions {

    // Populates the database (called only once per test)
    void populate(String connectionString, String dbms, int pIdLimit, int initialStock, Map<String, Object> extraConfigs);
    
    // Returns the type of this object
    String getType();
    
    // Prepares this transactions database connection
    void setConnection(String connectionString, String dbms, int isolation) throws Exception;
    
    // Closes the connection
    void closeConnection();
    
    // Decrement stock transaction (returns 1 if committed, 0 if aborted due to conflict, -1 if aborted due to no stock)
    int decrementStock(List<String> pIds, int amount);
    
    // Increment stock transaction
    boolean incrementStock(List<String> pIds, int amount);

    // Returns the current stock of some product
    int getStock(String pId);

    // Sets the identifier to id
    void setId(int id);
}
