import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class ExampleClient {

  public static void main(String[] args) throws IOException {
    Configuration config = HBaseConfiguration.create();
    // Tworzenie tabeli
    HBaseAdmin admin = new HBaseAdmin(config);
    try {
      TableName tableName = TableName.valueOf("test");
      HTableDescriptor htd = new HTableDescriptor(tableName);
      HColumnDescriptor hcd = new HColumnDescriptor("data");
      htd.addFamily(hcd);
      admin.createTable(htd);
      HTableDescriptor[] tables = admin.listTables();
      if (tables.length != 1 &&
          Bytes.equals(tableName.getName(), tables[0].getTableName().getName())) {
        throw new IOException("Nieudana próba utworzenia tabeli");
      }
      // Wykonywanie operacji na tabeli (trzy operacje dodawania danych, jedna pobierania i jedno skanowanie)
      HTable table = new HTable(config, tableName);
      try {
        for (int i = 1; i <= 3; i++) {
          byte[] row = Bytes.toBytes("row" + i);
          Put put = new Put(row);
          byte[] columnFamily = Bytes.toBytes("data");
          byte[] qualifier = Bytes.toBytes(String.valueOf(i));
          byte[] value = Bytes.toBytes("value" + i);
          put.add(columnFamily, qualifier, value);
          table.put(put);
        }
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);
        System.out.println("Pobieranie: " + result);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
          for (Result scannerResult : scanner) {
            System.out.println("Skanowanie: " + scannerResult);
          }
        } finally {
          scanner.close();
        }
        // Dezaktywacja i usuwanie tabeli
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      } finally {
        table.close();
      }
    } finally {
      admin.close();
    }
  }
}