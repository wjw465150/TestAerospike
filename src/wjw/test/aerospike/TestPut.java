package wjw.test.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;

public class TestPut {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 60; //µ•Œª:√Î

      Key key = new Key("test", "set", "key1");
      client.put(writePolicy, key, new Bin("bin1", "test(≤‚ ‘)"));
    } finally {
      client.close();
    }
  }

}
