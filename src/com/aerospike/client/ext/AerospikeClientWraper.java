package com.aerospike.client.ext;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;

/**
 * demo: <br/>
 * AerospikeClientWraper clientWraper = new
 * AerospikeClientWraper("T1:3000,T2,T3:3000,T4"); <br/>
 * try { <br/>
 * clientWraper.memPut("key1", "test(≤‚ ‘)", 180); <br/>
 * } finally { <br/>
 * clientWraper.close(); <br/>
 * } <br/>
 * 
 * @author Administrator
 *
 */
public final class AerospikeClientWraper {
  private final AerospikeClient client;

  private static final String   mem_namespace        = "ns_mem";
  private static final String   mem_set              = "set_mem";
  private static final String   mem_bin              = "bin_mem";

  private static final int      default_readTimeout  = 100;
  private static final int      default_writeTimeout = 200;

  public AerospikeClientWraper(String servers) {
    this(servers, default_readTimeout, default_writeTimeout);
  }

  public AerospikeClientWraper(String servers, int readTimeout, int writeTimeout) {
    String[] hostAndPorts = servers.split(",");
    Host[] hosts = new Host[hostAndPorts.length];
    for (int i = 0; i < hostAndPorts.length; i++) {
      String[] hp = hostAndPorts[i].split(":");
      if (hp.length == 1) {
        hosts[i] = new Host(hp[0], 3000);
      } else {
        hosts[i] = new Host(hp[0], Integer.parseInt(hp[1]));
      }
    }

    ClientPolicy policy = new ClientPolicy();
    policy.readPolicyDefault.socketTimeout = readTimeout;
    policy.readPolicyDefault.maxRetries = 3;
    policy.readPolicyDefault.sleepBetweenRetries = 10;
    policy.writePolicyDefault.socketTimeout = writeTimeout;
    policy.writePolicyDefault.maxRetries = 3;
    policy.writePolicyDefault.sleepBetweenRetries = 50;
    //The user defined key is not stored on the server by default . Instead, the user key is converted to a hash digest which is then used to identify a record. 
    //If the user key needs to persist on the server, use one of the following methods:
    //1. Set WritePolicy.sendKey to true. In this case, the key will be sent to the server for storage on writes and retrieved on multi-record scans and queries.
    //2. Explicitly store and retrieve the user key in a bin.
    policy.writePolicyDefault.sendKey = false;

    client = new AerospikeClient(policy, hosts);
  }

  public final AerospikeClient getClient() {
    return client;
  }

  public final void close() {
    client.close();
  }

  public final void memPut(String keyName, String value, int expiration) throws AerospikeException {
    WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault);
    writePolicy.expiration = expiration; //µ•Œª:√Î

    Key key = new Key(mem_namespace, mem_set, keyName);
    client.put(writePolicy, key, new Bin(mem_bin, value));
  }

  public final String memGet(String keyName) throws AerospikeException {
    Key key = new Key(mem_namespace, mem_set, keyName);

    Record record = client.get(client.readPolicyDefault, key);
    if (record == null) {
      return null;
    } else {
      return record.getString(mem_bin);
    }
  }

  public final boolean memDelete(String keyName) throws AerospikeException {
    Key key = new Key(mem_namespace, mem_set, keyName);
    return client.delete(client.writePolicyDefault, key);
  }

}
