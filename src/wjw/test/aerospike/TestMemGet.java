package wjw.test.aerospike;

import com.aerospike.client.ext.AerospikeClientWraper;

public class TestMemGet {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClientWraper clientWraper = new AerospikeClientWraper("T1:3000,T2,T3:3000,T4");
    try {
      String value = clientWraper.memGet("key1");
      if (value != null) {
        console.info(value);
      } else {
        console.info("not found");
      }
    } finally {
      clientWraper.close();
    }
  }

}
