package wjw.test.aerospike.benchmark;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ext.AerospikeClientWraper;
import com.aerospike.client.policy.WritePolicy;

public class GetBenchMarkMain2 {
  static final int COUNT     = 50;    //�߳���
  static final int LOOP      = 10000; //ÿ���߳�ѭ������

  String           nameSpace = "bar";
  String           setName   = "set1";
  String           binName   = "bin1";

  AerospikeClient  client;
  WritePolicy      writePolicy;

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
      String data = "test(����)Aerospike";
      try {
        barrier.await();

        //System.out.println("�߳�:" + pos + ",��ʼ����get()");
        String msg;
        for (int i = 1; i <= LOOP; i++) {
          try {
            Key key = new Key(nameSpace, setName, pos + ":" + i);
            Record record = client.get(null, key);
            if (record == null) {
              System.out.println("not found: " + pos + ":" + i);
            } else {
              msg = record.getString(binName);
              if ((data + ":" + pos + ":" + i).equals(msg) == false) {
                System.out.println("�߳�:" + pos + ",get����:" + msg);
              }
            }
          } catch (Exception ex) {
            System.out.println("�߳�:" + pos + ",get����:" + ex.getMessage());
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        //System.out.println("�߳�:" + pos + ",��������get()");
        doneSignal.countDown();
      }

    }
  }

  public void doExecute(String[] args) {
    {
      client = (new AerospikeClientWraper(args[0])).getClient();

      writePolicy = new WritePolicy(client.writePolicyDefault);
      writePolicy.expiration = 3 * 60 * 60; //��λ:��
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
          .println("�÷�:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.GetBenchMarkMain2 ������1ip,������2ip,...");
      System.out
          .println("����:java -Xmx512m -cp ./classes:./lib/* wjw.test.aerospike.benchmark.GetBenchMarkMain2 T1,T2,T3");
      System.exit(0);
    }

    GetBenchMarkMain2 testGet = new GetBenchMarkMain2();
    testGet.doExecute(args);

    System.exit(0);
  }

}