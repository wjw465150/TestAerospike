package wjw.test.aerospike.benchmark

import com.aerospike.client.AerospikeClient
import com.aerospike.client.Language
import com.aerospike.client.ext.AerospikeClientWraper

String udfName="count.lua"
File udfFile=new File(File.createTempDir(),udfName)
udfFile.text=$/
local function one(rec)
    return 1
end

local function add(a, b)
    return a + b
end

function count(stream)
    return stream : map(one) : reduce(add);
end
/$

AerospikeClient client = (new AerospikeClientWraper("T1:3000,T2,T3:3000,T4")).getClient()
try {

  println "��ɾ����ǰ��UDF:${udfName}"
  client.removeUdf(null,udfName)


  client.register(null, udfFile.absolutePath, udfName, Language.LUA).waitTillComplete()
  println "����ע��UDF�ɹ�:${udfName}"
} finally {
  udfFile.getParentFile().deleteDir()
  client.close()
}