package wjw.test.aerospike.query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

import wjw.test.aerospike.Console;

public class TestQuery {

  public static void main(String[] args) {
    final String nameSpace = "test";
    final String setName = "set_testindex";

    final Console console = new Console();

    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      System.out.println("start Scan...");
      long currentTimeMillis = System.currentTimeMillis();
      client.scanAll(null, nameSpace, setName, new ScanCallback() {
        @Override
        public void scanCallback(Key key, Record record) throws AerospikeException {
          console.info(record.toString());
        }
      });

      float ticket = System.currentTimeMillis() - currentTimeMillis;
      System.out.println("用时" + secondToHMS((long) (ticket / 1000)));

      {
        Statement stmt = new Statement();
        stmt.setNamespace("test");
        stmt.setSetName("set_testindex");
        stmt.setBinNames("bin_2");
        stmt.setFilters( Filter.range("bin_2", 2,5) );

        RecordSet rs = client.query(null, stmt);
        while (rs.next()) {
          Key key = rs.getKey();
          Record record = rs.getRecord();
          console.info(record.toString());
        }
      }
      
    } finally {
      client.close();
    }
  }

  public static String secondToHMS(long ticket) {
    long day1 = ticket / (24 * 3600);
    long hour1 = ticket % (24 * 3600) / 3600;
    long minute1 = ticket % 3600 / 60;
    long second1 = ticket % 60;

    StringBuilder sb = new StringBuilder();
    if (day1 > 0) {
      sb.append(day1 + "天");
    }
    if (hour1 > 0) {
      sb.append(hour1 + "小时");
    }
    if (minute1 > 0) {
      sb.append(minute1 + "分");
    }
    sb.append(second1 + "秒");

    return sb.toString();
  }

}
