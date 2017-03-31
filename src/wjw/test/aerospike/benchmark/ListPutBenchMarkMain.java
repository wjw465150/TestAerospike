package wjw.test.aerospike.benchmark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;

public class ListPutBenchMarkMain {
  static final int COUNT     = 100;                                                                //线程数
  static final int LOOP      = 10000;                                                              //每个线程循环次数

  String           nameSpace = "ns_mem";
  String           setName   = "set_mem";
  String           keyName   = "key_list";
  String           binName   = "bin_list";

  AerospikeClient  client;
  WritePolicy      writePolicy;

  String           data      = "This is test(测试List)Aerospike1234567890qwertyuiopasdfghjklzxcvbnm";

  class Worker implements Runnable {
    private CyclicBarrier  barrier;
    private CountDownLatch doneSignal;
    private int            pos;

    Worker(CyclicBarrier barrier, CountDownLatch doneSignal, int pos) {
      this.barrier = barrier;
      this.doneSignal = doneSignal;
      this.pos = pos;
    }

    public void run() {
      try {
        barrier.await();

        //System.out.println("线程:" + pos + ",开始调用put()");
        for (int i = 1; i <= LOOP; i++) {
          try {
            Key key = new Key(nameSpace, setName, keyName);
            client.operate(null, key, ListOperation.insert(binName, -1, Value.get(data + ":" + pos + ":" + i)));
          } catch (Exception ex) {
            System.out.println("线程:" + pos + ",put错误:" + ex.getMessage());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        //System.out.println("线程:" + pos + ",结束调用List Put()");
        doneSignal.countDown();
      }

    }
  }

  public void doExecute(String[] args) {
    {
      client = (new AerospikeClientWraper(args[0])).getClient();

      writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 10 * 60; //单位:秒
    }

    { //初始化List

      Key key = new Key(nameSpace, setName, keyName);
      client.delete(null, key);

      Record record = client.operate(null, key, ListOperation.append(binName, Value.get(new java.util.ArrayList<String>())));

      System.out.println("Record: " + record);
    }

    CyclicBarrier barrier = new CyclicBarrier(COUNT);
    CountDownLatch doneSignal = new CountDownLatch(COUNT);

    ExecutorService exec = Executors.newFixedThreadPool(COUNT);
    long currentTimeMillis = System.currentTimeMillis();
    for (int i = 0; i < COUNT; i++) {
      exec.submit(new Worker(barrier, doneSignal, i));
    }

    try {
      doneSignal.await(); //等待所有的线程完成工作
    } catch (InterruptedException ex) {
    }
    float ticket = System.currentTimeMillis() - currentTimeMillis;
    System.out.println((COUNT * LOOP) + "次用时" + secondToHMS((long) (ticket / 1000)));
    System.out.println("平均每次:" + (ticket / (COUNT * LOOP)) + "毫秒");
    System.out.println("平均每秒:" + ((COUNT * LOOP) / (ticket / 1000.00)) + "次");

    exec.shutdown();
    client.close();
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

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out
          .println("用法:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.ListPutBenchMarkMain 服务器1ip,服务器2ip,...");
      System.out
          .println("例如:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.ListPutBenchMarkMain T1,T2,T3");
      System.exit(0);
    }

    ListPutBenchMarkMain testPut = new ListPutBenchMarkMain();
    testPut.doExecute(args);

    System.exit(0);
  }

}
