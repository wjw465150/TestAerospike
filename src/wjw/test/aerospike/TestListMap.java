package wjw.test.aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;

public class TestListMap {
  public static void main(String[] args) {
    Console console = new Console();

    AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient();
    try {
      WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 60; //µ•Œª:√Î

      Key key = new Key("test", "set", "listkey1");
      //client.delete(null, key);

      ArrayList<String> list = new ArrayList<String>();
      list.add("string1");
      list.add("string2");
      list.add("string3");

      Bin bin = new Bin("listbin1", list);
      client.put(writePolicy, key, bin);

      Record record = client.get(null, key, bin.name);
      List<?> receivedList = (List<?>) record.getValue(bin.name);
      console.info(receivedList.toString());
    } finally {
      client.close();
    }
  }

}
