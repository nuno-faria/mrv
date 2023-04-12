/**
 * Stores the timestamp, result and response time of a transaction
 */
public class TxResult {

    long timestamp;
    boolean result;
    double rt;
    char type;
    int nProducts;

    public TxResult(long timestamp, boolean result, double rt, char type, int nProducts) {
        this.timestamp = timestamp;
        this.result = result;
        this.rt = rt;
        this.type = type;
        this.nProducts = nProducts;
    }
}
