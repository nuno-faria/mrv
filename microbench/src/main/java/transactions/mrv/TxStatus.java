package transactions.mrv;


import java.time.LocalDateTime;

public class TxStatus {

    public String pId;
    public boolean committed;
    public LocalDateTime time;

    public TxStatus(String pId, boolean committed, LocalDateTime time) {
        this.pId = pId;
        this.committed = committed;
        this.time = time;
    }
}
