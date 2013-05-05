<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%
  boolean show = true;
  String ip = request.getParameter("ip");
  if (ip==null || ip.equals("")) {
    response.sendRedirect("cluster.jsp");
    show = false;
  }
%>

<%
  if (show) {
%>


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
  <tr><td><span class="STYLE4">Node Info</span></td></tr>
  <tr><td id="info"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Proxy Info</span></td></tr>
  <tr><td id="proxy"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Storage Info</span></td></tr>
  <tr><td id="storage"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Schema Info</span></td></tr>
  <tr><td id="schema"></td></tr>
  <tr><td><hr></td></tr>
  <!-- tr><td><span class="STYLE4">Bucket Stats</span></td></tr>
  <tr><td id="stats"></td></tr>
  <tr><td><hr></td></tr-->
  
</table>
</body>
</html>

<script language="javascript">

  var ip = "<%=ip%>";

window.onLoad=new function(){
  loadNodeInfo();
//loadBucketStats();
  loadStorageInfo();
  loadProxyInfo();
  loadTotalSchemaInfo();
}

</script>

<%
  } 
%>