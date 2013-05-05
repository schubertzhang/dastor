var portalUri="/";

function showLoadingImage(id){
  document.getElementById(id).innerHTML="<image src='img/loading.gif'>";
}

function sort(list,attr){
  for (var i=0; i<list.length; i++){
    for (var j=i+1; j<list.length; j++){
      if (list[i][attr]>list[j][attr]){
        var t=list[i];
        list[i]=list[j];
        list[j]=t;
      }
    }
  }
}

function loadClusterName(){
  var id = "clusterName";
  showLoadingImage(id);
  var uri=portalUri+"ClusterServlet";
  var params="op=cluster_name";
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        document.getElementById(id).innerHTML="<table align='center' width='900'><tr><td class='STYLE2'>Cluster Name : "+m.name+"</td></tr></table>";
      }else{
        document.getElementById(id).innerHTML="Load cluster info error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load cluster info error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load cluster info error.";
    }});
}

function loadNodeList(){
  var id = "nodeList";
  showLoadingImage(id);
  var uri=portalUri+"ClusterServlet";
  var params="op=node_list";
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "<table align='center' width='900'>";
        view += "<tr><td class='STYLE2'>Live Nodes</td></tr>";
        
        sort(m.liveList,"ip");
        
        if (m.liveList.length>0) {
          view += "<tr><td><table border='1' width='890'>";
          view += "<tr class='STYLE3'><td>host</td><td>Operating</td><td>Generation</td><td>Uptime</td><td>Configured Capacity</td><td>Load</td><td>Gross Load</td><td>Heap Size</td></tr>"
          for (var i=0; i<m.liveList.length; i++){
            view += "<tr>";
            view += "<td class='STYLE3'><a class='STYLE3' target='_blank' href='node.jsp?ip="+m.liveList[i].ip+"'>"+m.liveList[i].ip+"</a></td>";
            view += "<td class='STYLE3'>"+m.liveList[i].operating+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].generation+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].uptime+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].configuredCapacity+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].load+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].grossLoad+"</td>";
            view += "<td class='STYLE3'>"+m.liveList[i].heapusage+"</td>";
            view += "</tr>";
          }
          view += "</table></td></tr>";
        }else{
          view += "<tr><td class='STYLE3'>no live node</td></tr>"
        }
        view += "<tr><td class='STYLE2'>Dead Nodes</td></tr>"
        if (m.deadList.length>0) {
          //ip=m.deadList[i].ip;
          view += "<tr><td><table border='1'>";
          for (var i=0; i<m.deadList.length; i++){
            view += "<tr>";
            view += "<td class='STYLE3'>"+m.deadList[i].ip+"</td>";
            view += "</tr>";
          }
          view += "</table></td></tr>";
        }else{
          view += "<tr><td class='STYLE3'>no dead node</td></tr>"
        }
        view += "</table>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load Node List error : "+m.reason;
      }

      if (m.liveList.length>0){
        showLoadingImage("schema");
        uri=portalUri+"NodeServlet";
        params="op=schema&ip="+m.liveList[0].ip;
        new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
          onComplete:function(request){
            var mm = eval("("+request.responseText+")");
            var mview = "";
            for (var i=0; i<mm.spaceSchemaList.length; i++) {
              mview+="<table align='center' width='900'>";
              mview+="<tr><td class='STYLE2'>Space:"+mm.spaceSchemaList[i].name+"</td></tr>";
              mview+="<table align='center' border='1' width='890'>";
              mview+="<tr><td colspan='2' class='STYLE3'>Replication Factor: "+mm.spaceSchemaList[i].replicationFactor+"</td></tr>";
              sort(mm.spaceSchemaList[i].bucketSchemaList, "name");
              for (var j=0; j<mm.spaceSchemaList[i].bucketSchemaList.length; j++){
                mview += "<tr class='STYLE3'>";
                mview += "<td width='150'>"+mm.spaceSchemaList[i].bucketSchemaList[j].name+"</td>";
                mview += "<td width='740'>";
                if (mm.spaceSchemaList[i].bucketSchemaList[j].comment != null)
                  mview += "Comment: "+mm.spaceSchemaList[i].bucketSchemaList[j].comment+"<br>";
                mview += "Key Cache Capacity: "+mm.spaceSchemaList[i].bucketSchemaList[j].keyCacheSize+"<br>";
                mview += "Row Cache Capacity: "+mm.spaceSchemaList[i].bucketSchemaList[j].rowCacheSize+"<br>";
                mview += "Compact Skip Size: "+mm.spaceSchemaList[i].bucketSchemaList[j].compactSkipSize+"<br>";
                mview += "Compress Algorithm: "+mm.spaceSchemaList[i].bucketSchemaList[j].compressAlgo+"<br>";
                mview += "</td>";
                mview += "</tr>";
              }
              mview+="</table></td></tr></table><br>";
            }
            document.getElementById("schema").innerHTML=mview;
          }} 
        );
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load Node List error.";
      document.getElementById("schema").innerHTML="Cannot Load Schema Info.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load Node List error.";
      document.getElementById("schema").innerHTML="Cannot Load Schema Info.";
    }});
}

function loadNodeInfo(){
  var id = "info";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=info&ip="+ip;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "<table align='center' width='900' border='1' class='STYLE3'>";
        view += "<tr><td width='200'>Host</td><td>"+m.node.ip+"</td></tr>"
        view += "<tr><td width='200'>Generation</td><td>"+m.node.generation+"</td></tr>"
        view += "<tr><td width='200'>Operating</td><td>"+m.node.operating+"</td></tr>"
        view += "<tr><td width='200'>Uptime</td><td>"+m.node.uptime+"</td></tr>"
        view += "<tr><td width='200'>Configured Capacity</td><td>"+m.node.configuredCapacity+"</td></tr>"
        view += "<tr><td width='200'>Load</td><td>"+m.node.load+"</td></tr>"
        view += "<tr><td width='200'>Gross Load</td><td>"+m.node.grossLoad+"</td></tr>"
        view += "<tr><td width='200'>Heap Usage</td><td>"+m.node.heapusage+"</td></tr>"
        view += "</rable>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load node info error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load node info error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load node info error.";
    }});
}

function loadTotalBucketStats(){
  var id = "stats";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=cfstats&ip="+ip;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "";
        for (var i=0; i<m.spaceList.length; i++) {
          view+="<table align='center' width='900'>";
          view+="<tr><td class='STYLE2'>"+m.spaceList[i].name+"</td></tr>";
          view+="<table align='center' border='1' width='890'>";
          view+="<tr><td colspan='2' class='STYLE3'>Read Count: "+m.spaceList[i].readCount+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Read Latency (ms): "+m.spaceList[i].readLatency+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Read Throughput: "+m.spaceList[i].readThroughput+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Write Count: "+m.spaceList[i].writeCount+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Write Latency (ms): "+m.spaceList[i].writeLatency+"</td></tr>";
          for (var j=0; j<m.spaceList[i].bucketList.length; j++){
            view += "<tr class='STYLE3'>";
            view += "<td width='150'>"+m.spaceList[i].bucketList[j].name+"</td>";
            view += "<td width='740'>";
            view += "SSTable count: "+m.spaceList[i].bucketList[j].ssTableCount+"<br>";
            view += "Space used (live): "+m.spaceList[i].bucketList[j].spaceUsedLive+"<br>";
            view += "Space used (total): "+m.spaceList[i].bucketList[j].spaceUsedTotal+"<br>";

            view += "Memtable Columns Count: "+m.spaceList[i].bucketList[j].memtableColumnCount+"<br>";
            view += "Memtable Data Size: "+m.spaceList[i].bucketList[j].memtableDataSize+"<br>";
            view += "Memtable Switch Count: "+m.spaceList[i].bucketList[j].memtableSwitchCount+"<br>";
            view += "Read Count: "+m.spaceList[i].bucketList[j].readCount+"<br>";
            view += "Read Latency (ms): "+m.spaceList[i].bucketList[j].readLatency+"<br>";
            view += "Read Throughput: "+m.spaceList[i].bucketList[j].readThroughput+"<br>";
            view += "Write Count: "+m.spaceList[i].bucketList[j].writeCount+"<br>";
            view += "Write Latency (ms): "+m.spaceList[i].bucketList[j].writeLatency+"<br>";
            view += "Write Throughput: "+m.spaceList[i].bucketList[j].writeThroughput+"<br>";
            view += "Pending Tasks: "+m.spaceList[i].bucketList[j].pendingTasks+"<br>";
            if (m.spaceList[i].bucketList[j].keyCacheCapacity>0){
              view += "Key cache capacity: "+m.spaceList[i].bucketList[j].keyCacheCapacity+"<br>";
              view += "Key cache size:: "+m.spaceList[i].bucketList[j].keyCacheSize+"<br>";
              view += "Key cache hit rate:: "+m.spaceList[i].bucketList[j].keyCacheHitRate+"<br>";
            }else{
              view += "Key cache: disabled <br>";
            }
            if (m.spaceList[i].bucketList[j].rowCacheCapacity>0){
              view += "Row cache capacity: "+m.spaceList[i].bucketList[j].rowCacheCapacity+"<br>";
              view += "Row cache size:: "+m.spaceList[i].bucketList[j].rowCacheSize+"<br>";
              view += "Row cache hit rate:: "+m.spaceList[i].bucketList[j].rowCacheHitRate+"<br>";
            }else{
              view += "Row cache: disabled <br>";
            }
            view += "Compacted row minimum size: "+m.spaceList[i].bucketList[j].compactedRowMinimumSize+"<br>";
            view += "Compacted row maximum size: "+m.spaceList[i].bucketList[j].compactedRowMaximumSize+"<br>";
            view += "Compacted row mean size: "+m.spaceList[i].bucketList[j].compactedRowMeanSize+"<br>";
            view += "Status: "+m.spaceList[i].bucketList[j].status+"<br>";
            view += "Timestamp: "+m.spaceList[i].bucketList[j].statusTimestamp+"<br>";
            view += "</td>";
            view += "</tr>";
          }
          view+="</table></td></tr></table><br>";
        }
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load bucket stats error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    }});
}

function loadBucketStats(){
  var id = "stats";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=single_bucket_state&ip="+ip+"&space="+space+"&bucket="+bucket;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "<table align='center' width='900' border='1' class='STYLE3'>";
        view += "<tr><td width='400'>SSTable count: </td><td>"+m.bucket.ssTableCount+"</td>";
        view += "<tr><td width='400'>Space used (live) (bytes): </td><td>"+m.bucket.spaceUsedLive+"</td>";
        view += "<tr><td width='400'>Space used (total) (bytes): </td><td>"+m.bucket.spaceUsedTotal+"</td>";
        view += "<tr><td width='400'>Memtable Cell Count: </td><td>"+m.bucket.memtableCellCount+"</td>";
        view += "<tr><td width='400'>Memtable Data Size (bytes): </td><td>"+m.bucket.memtableDataSize+"</td>";
        view += "<tr><td width='400'>Memtable Flush Count: </td><td>"+m.bucket.memtableSwitchCount+"</td>";
        view += "<tr><td width='400'>Read Count: </td><td>"+m.bucket.readCount+"</td>";
        view += "<tr><td width='400'>Read Latency (ms): </td><td>"+m.bucket.readLatency+"</td>";
        view += "<tr><td width='400'>Read Throughput: </td><td>"+m.bucket.readThroughput+"</td>";
        view += "<tr><td width='400'>Write Count: </td><td>"+m.bucket.writeCount+"</td>";
        view += "<tr><td width='400'>Write Latency (ms): </td><td>"+m.bucket.writeLatency+"</td>";
        view += "<tr><td width='400'>Write Throughput: </td><td>"+m.bucket.writeThroughput+"</td>";
        view += "<tr><td width='400'>Pending Tasks: </td><td>"+m.bucket.pendingTasks+"</td>";
        if (m.bucket.keyCacheCapacity>0){
          view += "<tr><td width='400'>Key cache capacity: </td><td>"+m.bucket.keyCacheCapacity+"</td>";
          view += "<tr><td width='400'>Key cache size: </td><td>"+m.bucket.keyCacheSize+"</td>";
          view += "<tr><td width='400'>Key cache hit rate: </td><td>"+m.bucket.keyCacheHitRate+"</td>";
        }else{
          view += "<tr><td width='400'>Key cache: </td><td>disabled </td>";
        }
        if (m.bucket.rowCacheCapacity>0){
          view += "<tr><td width='400'>Row cache capacity: </td><td>"+m.bucket.rowCacheCapacity+"</td>";
          view += "<tr><td width='400'>Row cache size: </td><td>"+m.bucket.rowCacheSize+"</td>";
          view += "<tr><td width='400'>Row cache hit rate: </td><td>"+m.bucket.rowCacheHitRate+"</td>";
        }else{
          view += "<tr><td width='400'>Row cache: </td><td>disabled </td>";
        }
        view += "<tr><td width='400'>Compacted row minimum size: </td><td>"+m.bucket.compactedRowMinimumSize+"</td>";
        view += "<tr><td width='400'>Compacted row maximum size: </td><td>"+m.bucket.compactedRowMaximumSize+"</td>";
        view += "<tr><td width='400'>Compacted row mean size: </td><td>"+m.bucket.compactedRowMeanSize+"</td>";
        view += "<tr><td width='400'>Status: </td><td>"+m.bucket.status+"</td>";
        view += "<tr><td width='400'>Timestamp: </td><td>"+m.bucket.statusTimestamp+"</td>";
        view+="</table>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load bucket stats error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    }});
}

function loadStorageInfo(){
  var id = "storage";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=fsstats&ip="+ip;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "";
        view+="<table align='center' width='900'>";
        view+="<tr><td class='STYLE2'>Storage Data</td></tr>";
        view+="<tr><td class='STYLE3'><table align='center' border='1' width='890'>";
        view+="<tr><td width='300'>storage path</td><td width='300'>fs name</td><td width='100'>mount on</td><td width='100'>Capacity</td><td width='100'>Used</td></tr>"
        for (var i=0; i<m.storageInfo.length; i++){
          view+="<tr>";
          view+="<td>"+m.storageInfo[i].fileName+"</td>";
          view+="<td>"+m.storageInfo[i].fsName+"</td>";
          view+="<td>"+m.storageInfo[i].mountOn+"</td>";
          view+="<td>"+m.storageInfo[i].total+"</td>";
          view+="<td>"+m.storageInfo[i].used+"</td>";
          view+="</tr>";
        }
        view+="<table></td></tr>";
        view+="<tr><td class='STYLE2'>CommitLog Data</td></tr>";
        view+="<tr><td class='STYLE3'><table align='center' border='1' width='890'>";
        view+="<tr><td width='300'>storage path</td><td width='300'>fs name</td><td width='100'>mount on</td><td width='100'>Total</td><td width='100'>Used</td></tr>"
        for (var i=0; i<m.logInfo.length; i++){
          view+="<tr>";
          view+="<td>"+m.logInfo[i].fileName+"</td>";
          view+="<td>"+m.logInfo[i].fsName+"</td>";
          view+="<td>"+m.logInfo[i].mountOn+"</td>";
          view+="<td>"+m.logInfo[i].total+"</td>";
          view+="<td>"+m.logInfo[i].used+"</td>";
          view+="</tr>";
        }
        view+="<table></td></tr></table>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load bucket stats error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load bucket stats error.";
    }});
}

function loadProxyInfo(){
  var id = "proxy";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=proxy&ip="+ip;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "";
        view+="<table align='center' border='1' width='900' class='STYLE3'>";
        view+="<tr><td width='200'>Total Reads</td><td>"+m.readOperations+"</td></tr>";
        view+="<tr><td width='200'>Total Read Latency (ms)</td><td>"+m.totalReadLatency+"</td></tr>";
        view+="<tr><td width='200'>Recent Read Latency (ms)</td><td>"+m.recentReadLatency+"</td></tr>";
//      view+="<tr><td width='200'>Total Ranges</td><td>"+m.rangeOperations+"</td></tr>";
//      view+="<tr><td width='200'>Total Range Latency (ms)</td><td>"+m.totalRangeLatency+"</td></tr>";
//      view+="<tr><td width='200'>Recent Range Latency (ms)</td><td>"+m.recentRangeLatency+" </td></tr>";
        view+="<tr><td width='200'>Total Writes</td><td>"+m.writeOperations+"</td></tr>";
        view+="<tr><td width='200'>Total Write Latency (ms)</td><td>"+m.totalWriteLatency+"</td></tr>";
        view+="<tr><td width='200'>Recent Write Latency (ms)</td><td>"+m.recentWriteLatency+"</td></tr>";
        view+="</table>";
        view+="<table></td></tr></table>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load proxy stats error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load proxy stats error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load proxy stats error.";
    }});
}

function loadTotalSchemaInfo(){
  var id = "schema";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=schema&ip="+ip;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view = "";
        
        for (var i=0; i<m.spaceSchemaList.length; i++) {
          view+="<table align='center' width='900'>";
          view+="<tr><td class='STYLE2'>Space:"+m.spaceSchemaList[i].name+"</td></tr>";
          view+="<table align='center' border='1' width='890'>";
          view+="<tr><td colspan='2' class='STYLE3'>Read Count: "+m.spaceSchemaList[i].readCount+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Read Latency (ms): "+m.spaceSchemaList[i].readLatency+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Write Count: "+m.spaceSchemaList[i].writeCount+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Write Latency (ms): "+m.spaceSchemaList[i].writeLatency+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Pending Tasks: "+m.spaceSchemaList[i].pendingTasks+"</td></tr>";
          view+="<tr><td colspan='2' class='STYLE3'>Replication Factor: "+m.spaceSchemaList[i].replicationFactor+"</td></tr>";

          sort(m.spaceSchemaList[i].bucketSchemaList, "name");
          for (var j=0; j<m.spaceSchemaList[i].bucketSchemaList.length; j++){
            view += "<tr class='STYLE3'>";
            view += "<td width='150'><a class='STYLE3' href='bucket.jsp?ip="+ip+"&space="+m.spaceSchemaList[i].name+"&bucket="+m.spaceSchemaList[i].bucketSchemaList[j].name+"'>"+m.spaceSchemaList[i].bucketSchemaList[j].name+"</a></td>";
            view += "<td width='740'>";
            if (m.spaceSchemaList[i].bucketSchemaList[j].comment != null)
              view += "Comment: "+m.spaceSchemaList[i].bucketSchemaList[j].comment+"<br>";
            view += "Key Cache Capacity: "+m.spaceSchemaList[i].bucketSchemaList[j].keyCacheSize+"<br>";
            view += "Row Cache Capacity: "+m.spaceSchemaList[i].bucketSchemaList[j].rowCacheSize+"<br>";
            view += "Compact Skip Size: "+m.spaceSchemaList[i].bucketSchemaList[j].compactSkipSize+"<br>";
            view += "Compress Algo: "+m.spaceSchemaList[i].bucketSchemaList[j].compressAlgo+"<br>";
            view += "</td>";
            view += "</tr>";
          }
          view+="</table></td></tr></table><br>";
        }
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load schema error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load schema error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load schema error.";
    }});
}

function loadBucketSchemaInfo()
{
  var id = "schema";
  showLoadingImage(id);
  var uri=portalUri+"NodeServlet";
  var params="op=single_bucket_schema&ip="+ip+"&space="+space+"&bucket="+bucket;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
        var view="<table align='center' width='900' border='1' class='STYLE3'>";
        if (m.bucket.comment != null)
          view+="<tr><td width='200'>Comment: </td><td>"+m.bucket.comment+"</td></tr>";
        view+="<tr><td width='200'>Key Cache Capacity: </td><td>"+m.bucket.keyCacheSize+"</td></tr>";
        view+="<tr><td width='200'>Row Cache Capacity: </td><td>"+m.bucket.rowCacheSize+"</td></tr>";
        view+="<tr><td width='200'>Compact Skip Size: </td><td>"+m.bucket.compactSkipSize+"</td></tr>";
        view+="<tr><td width='200'>Compress Algo: </td><td>"+m.bucket.compressAlgo+"</td></tr>";
        view+="</table>";
        document.getElementById(id).innerHTML=view;
      }else{
        document.getElementById(id).innerHTML="Load schema error : "+m.reason;
      }
    },
    onFailure:function(){
      document.getElementById(id).innerHTML="Load schema error.";
    },
    onException:function(){
      document.getElementById(id).innerHTML="Load schema error.";
    }});
}

function trim(string) {
  var i=0; j=0;
  for(i = 0; i<string.length && string.charAt(i)==" "; i++);
  for(j = string.length; j>0 && string.charAt(j-1)==" "; j--);
  if(i>j) return  "";  
  return  string.substring(i,j);  
}

function oper(op){

  var go = confirm("Are you sure to do this operation : " + op);

  if (!go)
    return;
  
  var uri=portalUri+"NodeServlet";
  var params="op="+op+"&ip="+ip+"&space="+space+"&bucket="+bucket;
  new Ajax.Request(uri,{method:'post',parameters:params,requestHeaders:{'If-Modified-Since':'0'},
    onComplete:function(request){
      var m = eval("("+request.responseText+")");
      if (m.result=="ok"){
      	alert("Exec Operation : "+op+" OK");
      }else{
    	alert("Exec Operation : "+op+" Error : "+m.reason);
      }
    },
    onFailure:function(){
      alert("Exec Operation : "+op+" Error");
    },
    onException:function(){
      alert("Exec Operation : "+op+" Error");
    }});
}

function reset(){
  if (trim(document.getElementById("resetSpace").value)==""){
    alert("space name cannot be null");
    return;
  }
  if (trim(document.getElementById("resetBucket").value)==""){
    alert("bucket name cannot be null");
    return;
  }
  space=document.getElementById("resetSpace");
  bucket=document.getElementById("resetBucket");
  oper("reset");
}

function undoReset(){
  if (trim(document.getElementById("undoResetSpace").value)==""){
    alert("space name cannot be null");
    return;
  }
  if (trim(document.getElementById("undoResetBucket").value)==""){
    alert("bucket name cannot be null");
    return;
  }
  space=document.getElementById("undoResetSpace");
  bucket=document.getElementById("undoResetBucket");
  oper("undoreset");
}
