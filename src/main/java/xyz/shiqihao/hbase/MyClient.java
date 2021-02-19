package xyz.shiqihao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class MyClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test-table"));

        // put
        table.put(buildPut("java-value"));

        // get
        Result getRes = table.get(new Get(Bytes.toBytes("java-row")));
        System.out.println("GET: " + new String(getRes.getValue(Bytes.toBytes("col-fam-1"), Bytes.toBytes("my-col-1"))));

        // scan
        try (ResultScanner scanner = table.getScanner(new Scan())) {
            for (Result r : scanner) {
                //String row = new String(r.getRow());
                //String value = new String(r.getValue(Bytes.toBytes("col-fam-0"), Bytes.toBytes("my-col-0")));
                //String createAt = Instant.ofEpochMilli(r.getColumnLatestCell(Bytes.toBytes("col-fam-0"), Bytes.toBytes("my-col-0")).getTimestamp()).toString();
                //System.out.println("[" + createAt + "] " + row + " " + value);
                System.out.println(r);
            }
        }

        // delete
        Delete delete = new Delete(Bytes.toBytes("java-row")).addFamily(Bytes.toBytes("col-fam-1"));
        table.delete(delete);
    }

    static Put buildPut(String value) {
        Put put = new Put(Bytes.toBytes("java-row"));
        put.addColumn(Bytes.toBytes("col-fam-1"), Bytes.toBytes("my-col-1"), Bytes.toBytes(value));
        return put;
    }
}
