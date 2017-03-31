package wjw.test.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ext.AerospikeClientWraper;

public class TestGet {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      Key key = new Key("test", "set", "key1");

      // Read a record
      Record record = client.get(client.readPolicyDefault, key);
      if (record != null) {
        console.info(record.toString());
      } else {
        console.info("not found");
      }
    } finally {
      client.close();
    }
  }

}
