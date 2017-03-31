package wjw.test.aerospike.benchmark;

import java.util.concurrent.atomic.AtomicInteger;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.ext.AerospikeClientWraper;

public class TestScan {

  public static void main(String[] args) {
    final String nameSpace = "ns_mem";
    final String setName = "set_mem";
    final String binName = "bin_mem";

    final AtomicInteger order = new AtomicInteger(0);
    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      System.out.println("start Scan...");
      long currentTimeMillis = System.currentTimeMillis();
      client.scanAll(null, nameSpace, setName, new ScanCallback() {
        @Override
        public void scanCallback(Key key, Record record) throws AerospikeException {
          String[] value = record.getString(binName).split(":");
          if (value[1].equals("11")) {
            order.incrementAndGet();
          }
        }
      });

      float ticket = System.currentTimeMillis() - currentTimeMillis;
      System.out.println("用时" + secondToHMS((long) (ticket / 1000)));
      System.out.println("order:" + order.get());
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
