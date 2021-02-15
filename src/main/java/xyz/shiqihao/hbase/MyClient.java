package xyz.shiqihao.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.time.Instant;

public class MyClient {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("test-table"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result r = scanner.next(); r != null; r = scanner.next()) {
            String row = new String(r.getRow());
            String value = new String(r.getValue(Bytes.toBytes("col-fam-0"), Bytes.toBytes("my-col-0")));
            String createAt = Instant.ofEpochMilli(r.getColumnLatestCell(Bytes.toBytes("col-fam-0"), Bytes.toBytes("my-col-0")).getTimestamp()).toString();
            System.out.println("[" + createAt + "] " + row + " " + value);
        }
    }
}
