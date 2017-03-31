package wjw.test.aerospike;

import com.aerospike.client.ext.AerospikeClientWraper;

public class TestMemPut {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClientWraper clientWraper = new AerospikeClientWraper("T1:3000,T2,T3:3000,T4");
    try {
      clientWraper.memPut("key1", "test(≤‚ ‘)", 180);
    } finally {
      clientWraper.close();
    }
  }

}
