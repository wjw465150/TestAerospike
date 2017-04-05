package com.aerospike.client.ext

import java.text.SimpleDateFormat

import com.aerospike.client.AerospikeClient
import com.aerospike.client.AerospikeException
import com.aerospike.client.Key
import com.aerospike.client.Record
import com.aerospike.client.ResultCode
import com.aerospike.client.policy.ScanPolicy
import com.aerospike.client.policy.WritePolicy

import groovy.transform.CompileStatic

/**
 * 清空指定Set的全部数据!
 * 用法: /opt/app/groovy/bin/groovy -cp ${/path/aerospike-client-X.X.X-jar-with-dependencies.jar} /opt/app/groovy/script/ClearSetData.groovy -h ${ass_ip} -p ${ass_port:3000} -n ${namespace_name} -s ${set_name}
 *
 */

//创建 CliBuilder 实例，并定义命令行选项
String strUsage='''/opt/app/groovy/bin/groovy -cp ${/path/aerospike-client-X.X.X-jar-with-dependencies.jar} /opt/app/groovy/script/ClearSetData.groovy -h ${ass_ip} -p ${ass_port:3000} -n ${namespace_name} -s ${set_name}
'''

def cmdline = new CliBuilder(width: 300, usage: strUsage,header:"Options:")
cmdline.u( longOpt: "usage", required: false, "Print usage." )
cmdline.h( longOpt: "host", required: true, args: 1, "Server hostname" )
cmdline.p( longOpt: "port", required: true, args: 1, "Server port" )
cmdline.n( argName: "namespace", required: true, args: 1, "Namespace" )
cmdline.s( argName: "set", required: true, args: 1, "Set to delete" )

def opt = cmdline.parse(args)
if (!opt) { return }
if (opt.u) {
  cmdline.usage()
  return
}

String host = null
if (opt.h) {
  host = opt.h
}
if (host == null){
  println "You must specify a host"
  return
}

int port=0
if (opt.p) {
  port = opt.p.toInteger()
}
if (port == 0){
  println "You must specify a port"
  return
}

String namespace = null
if (opt.n) {
  namespace = opt.n
}
if (namespace == null){
  println "You must specify a namespace"
  return
}

String set = null
if (opt.s) {
  set = opt.s
}
if (set == null){
  println "You must specify a set"
  return
}

println "Host: ${host}"
println "Port: ${port}"
println "Name space: ${namespace}"
println "Set: ${set}"


try {
  int count = 0

  final AerospikeClient client = new AerospikeClient(host, port)


  ScanPolicy scanPolicy = new ScanPolicy()
  scanPolicy.includeBinData = false

  WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault)
  writePolicy.timeout = 3000

  /*
   * scan the entire Set using scanAll(). This will scan each node
   * in the cluster and return the record Digest to the call back object
   */
  def scanCallback={Key key, Record record ->
    if (client.delete(writePolicy, key)) {
      count++
    }

    if (count % 10000 == 0){
      println "[${getCurrent()}] Deleted: ${count}"
    }
  }
  client.scanAll(scanPolicy, namespace, set, scanCallback, [] as String[])

  println "Deleted ${count} records from set ${set}"
} catch (AerospikeException e) {
  int resultCode = e.getResultCode()
  println ResultCode.getResultString(resultCode)
  println "Error details: ${e}"
}

@CompileStatic
private String getCurrent() {
  return (new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss")).format(new java.util.Date())
}

