<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<link href="css/dastor.css" rel="stylesheet" type="text/css" />
<script language="javascript" src="js/dastor.js" ></script>
<script language="javascript" src="js/prototype.js" ></script>

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Welcome to DaStor</title>
</head>
<body>
<table width="1000" align="center">
  <tr><td><%@ include file="head.jsp" %></td></tr>
  <tr><td><span class="STYLE4">Cluster</span></td></tr>
  <tr><td id="clusterName"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Node List</span></td></tr>
  <tr><td id="nodeList"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Operations</span></td></tr>
  <tr><td class='STYLE2'>
    <table border='1' align='center' width='900' class='STYLE2'>
      <tr>
        <td width='100'>Reset</td>
        <td width='300'>Space : <input type='text' id='resetSpace'> </td>
        <td width='300'>Bucket : <input type='text' id='resetBucket'> </td>
        <td><button onClick="javascript:reset()">Submit</button></td>
      </tr>
      <tr>
        <td width='100'>Undo Reset</td>
        <td width='300'>Space : <input type='text' id='undoResetSpace'> </td>
        <td width='300'>Bucket : <input type='text' id='undoResetBucket'> </td>
        <td><button onClick="javascript:undoReset()">Submit</button></td>
      </tr>
    </table>
  </td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Data Schema</span></td></tr>
  <tr><td id="schema"></td></tr>
  <tr><td><hr></td></tr>
  
</table>
</body>
</html>

<script language="javascript">
var ip;
var space;
var bucket;
window.onLoad=new function(){
  loadClusterName();
  loadNodeList();
}

</script>