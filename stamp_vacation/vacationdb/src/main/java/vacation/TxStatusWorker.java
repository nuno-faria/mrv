package vacation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jdbi.v3.core.Jdbi;

public class TxStatusWorker implements Runnable {

    private Jdbi jdbi;
    private BlockingQueue<TxStatus> queue;
    public static TxStatusWorker worker = null;


    public TxStatusWorker(Jdbi jdbi) {
        if (worker == null) {
            this.jdbi = jdbi;
            this.queue = new LinkedBlockingQueue<>();
            worker = this;
        }
    }


    public void addStatus(TxStatus status) {
        this.queue.add(status);
    }


    public void addStatus(List<TxStatus> statuses) {
        this.queue.addAll(statuses);
    }


    @Override
    public void run() {
        while (true) {
            List<TxStatus> statuses = new ArrayList<>();
            Map<String, Integer[]> queries = new HashMap<>();
            queue.drainTo(statuses);

            for (TxStatus status: statuses) {
                if (status.type.equals("data")) {
                    for (String columnName: status.columnNames) {
                        if (!columnName.equals("price")) {
                            String query = "INSERT INTO tx_status VALUES ('" + status.tableName + "','" + columnName + "','" +
                                            status.pk + "', %d, %d, now(), '" + status.pkSql + "') ON CONFLICT (table_name, column_name, pk) DO UPDATE " +
                                            "SET commits = tx_status.commits + %d, aborts = tx_status.aborts + %d, last_updated = now()";
                            queries.putIfAbsent(query, new Integer[]{0, 0});
                            queries.get(query)[status.commit ? 0 : 1] += 1;
                        }
                    }
                }
                else {
                    String query = status.query.replaceAll("<placeholder>", "%d");
                    queries.putIfAbsent(query, new Integer[]{0, 0});
                    queries.get(query)[status.commit ? 0 : 1] += 1;
                }
            }

            for (Map.Entry<String, Integer[]> d: queries.entrySet()) {
                boolean done = false;
                while (!done) {
                    try {
                        jdbi.withHandle(h ->
                                h.createUpdate(String.format(d.getKey(), d.getValue()[0], d.getValue()[1],
                                    d.getValue()[0], d.getValue()[1])).execute());
                        done = true;
                    }
                    catch (Exception e) { }
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
