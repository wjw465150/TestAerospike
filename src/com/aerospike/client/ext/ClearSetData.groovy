package com.aerospike.client.ext

import java.text.SimpleDateFormat

import com.aerospike.client.AerospikeClient
import com.aerospike.client.AerospikeException
import com.aerospike.client.Key
import com.aerospike.client.Record
import com.aerospike.client.ResultCode
import com.aerospike.client.ScanCallback
import com.aerospike.client.policy.ScanPolicy

/**
 * 清空指定Set的全部数据!
 * 用法: /opt/app/groovy/bin/groovy -cp ${/path/aerospike-client-X.X.X-jar-with-dependencies.jar} /opt/app/groovy/script/ClearSetData.groovy -h ${ass_ip} -p ${ass_port:3000} -n ${namespace_name} -s ${set_name}
 *
 */

//创建 CliBuilder 实例，并定义命令行选项
String strUsage='''/opt/app/groovy/bin/groovy -cp ${/path/aerospike-client-X.X.X-jar-with-dependencies.jar} /opt/app/groovy/script/ClearSetData.groovy -h ${ass_ip} -p ${ass_port:3000} -n ${namespace_name} -s ${set_name}
'''

def cmdline = new CliBuilder(width: 200, usage: strUsage,header:"Options:")
cmdline.u( longOpt: "usage", required: false, "Print usage." )
cmdline.h( longOpt: "host", required: true, args: 1, "Server hostname (default: localhost)" )
cmdline.p( longOpt: "port", required: true, args: 1, "Server port (default: 3000)" )
cmdline.n( argName: "namespace", required: true, args: 1, "Namespace (default: test)" )
cmdline.s( argName: "set", required: true, args: 1, "Set to delete (default: test)" )

def opt = cmdline.parse(args)
if (!opt) { return }
if (opt.u) {
  cmdline.usage()
  return
}

String host = "127.0.0.1"
if (opt.h) {
  host = opt.h
}

int port=3000
if (opt.p) {
  port = opt.p.toInteger()
}

String namespace = "test"
if (opt.n) {
  namespace = opt.n
}

String set = "test"
if (opt.s) {
  set = opt.s
}

println "Host: ${host}"
println "Port: ${port}"
println "Name space: ${namespace}"
println "Set: " + set

if (set == null){
  println "You must specify a set"
  return
}

try {
  int count = 0

  final AerospikeClient client = new AerospikeClient(host, port)


  ScanPolicy scanPolicy = new ScanPolicy()
  scanPolicy.includeBinData = false
  /*
   * scan the entire Set using scanAll(). This will scan each node
   * in the cluster and return the record Digest to the call back object
   */
  client.scanAll(scanPolicy, namespace, set, new ScanCallback() {
        public void scanCallback(Key key, Record record) throws AerospikeException {
          if (client.delete(null, key)) {
            count++
          }
          if (count % 10000 == 0){
            println "[${getCurrent()}] Deleted: ${count}"
          }
        }
      }, new String[0]);
    
  println "Deleted ${count} records from set ${set}"
} catch (AerospikeException e) {
  int resultCode = e.getResultCode()
  println ResultCode.getResultString(resultCode)
  println "Error details: ${e}"
}

private String getCurrent() {
  return (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")).format(new java.util.Date())
}

