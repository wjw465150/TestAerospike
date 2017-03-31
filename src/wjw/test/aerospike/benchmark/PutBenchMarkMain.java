package wjw.test.aerospike.benchmark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;

public class PutBenchMarkMain {
  static final int COUNT     = 100;    //�߳���
  static final int LOOP      = 10000; //ÿ���߳�ѭ������

  String           nameSpace = "ns_mem";
  String           setName   = "set_mem";
  String           binName   = "bin_mem";

  AerospikeClient  client;
  WritePolicy      writePolicy;

  String           data      = "This is test(����)Aerospike1234567890qwertyuiopasdfghjklzxcvbnm";
//  {
//    for (int i = 0; i < 20; i++) {
//      data = data + "This is test(����)Aerospike1234567890qwertyuiopasdfghjklzxcvbnm";
//    }
//  }
  
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

        //System.out.println("�߳�:" + pos + ",��ʼ����put()");
        for (int i = 1; i <= LOOP; i++) {
          try {
            Key key = new Key(nameSpace, setName, pos + ":" + i);
            client.put(writePolicy, key, new Bin(binName, data + ":" + pos + ":" + i));
          } catch (Exception ex) {
            System.out.println("�߳�:" + pos + ",put����:" + ex.getMessage());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        //System.out.println("�߳�:" + pos + ",��������put()");
        doneSignal.countDown();
      }

    }
  }

  public void doExecute(String[] args) {
    {
      client = (new AerospikeClientWraper(args[0])).getClient();

      writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 10 * 60; //��λ:��
    }

    CyclicBarrier barrier = new CyclicBarrier(COUNT);
    CountDownLatch doneSignal = new CountDownLatch(COUNT);

    ExecutorService exec = Executors.newFixedThreadPool(COUNT);
    long currentTimeMillis = System.currentTimeMillis();
    for (int i = 0; i < COUNT; i++) {
      exec.submit(new Worker(barrier, doneSignal, i));
    }

    try {
      doneSignal.await(); //�ȴ����е��߳���ɹ���
    } catch (InterruptedException ex) {
    }
    float ticket = System.currentTimeMillis() - currentTimeMillis;
    System.out.println((COUNT * LOOP) + "����ʱ" + secondToHMS((long) (ticket / 1000)));
    System.out.println("ƽ��ÿ��:" + (ticket / (COUNT * LOOP)) + "����");
    System.out.println("ƽ��ÿ��:" + ((COUNT * LOOP) / (ticket / 1000.00)) + "��");

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
      sb.append(day1 + "��");
    }
    if (hour1 > 0) {
      sb.append(hour1 + "Сʱ");
    }
    if (minute1 > 0) {
      sb.append(minute1 + "��");
    }
    sb.append(second1 + "��");

    return sb.toString();
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out
          .println("�÷�:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.PutBenchMarkMain ������1ip,������2ip,...");
      System.out
          .println("����:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.PutBenchMarkMain T1,T2,T3");
      System.exit(0);
    }

    PutBenchMarkMain testPut = new PutBenchMarkMain();
    testPut.doExecute(args);

    System.exit(0);
  }

}
