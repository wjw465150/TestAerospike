package wjw.test.aerospike.query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Statement;

import wjw.test.aerospike.Console;

public class TestIndex {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 60*100; //µ•Œª:√Î

      for (int i = 0; i < 10; i++) {
        Key key = new Key("test", "set_testindex", "key_" + i);
        client.put(writePolicy, key, new Bin("bin_1", "test(≤‚ ‘)_" + i));
        client.put(writePolicy, key, new Bin("bin_2", i));
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      
    } finally {
      client.close();
    }
  }

}
