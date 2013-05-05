<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%
    boolean show = true;
    String ip = request.getParameter("ip");
    String space = request.getParameter("space");
    String bucket = request.getParameter("bucket");
    if (ip == null || space == null || bucket == null || ip.equals("")
            || space.equals("") || bucket.equals("")) {
        response.sendRedirect("node.jsp");
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
  <tr><td><span class="STYLE4">Bucket Info</span></td></tr>
  <tr><td class='STYLE2'>
    <table border='1' align='center' width='900' class='STYLE2'>
      <tr><td>
        Node: <%=ip %><br>
        Space: <%=space %> <br>
        Bucket: <%=bucket %><br>
      </td></tr>
    </table>
  </td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Operations</span></td></tr>
  <tr><td class='STYLE2'>
    <table border='1' align='center' width='900' class='STYLE2'>
      <tr>
        <td width='600'>Flush this bucket</td>
        <td><button onClick="javascript:oper('flush')">Submit</button></td>
      </tr>
      <tr>
        <td width='600'>Compact this bucket</td>
        <td><button onClick="javascript:oper('compact')">Submit</button></td>
      </tr>
    </table>
  </td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Schema Info</span></td></tr>
  <tr><td id="schema"></td></tr>
  <tr><td><hr></td></tr>
  <tr><td><span class="STYLE4">Stats Info</span></td></tr>
  <tr><td id="stats"></td></tr>
  <tr><td><hr></td></tr>
  <!-- tr><td><span class="STYLE4">Bucket Stats</span></td></tr>
  <tr><td id="stats"></td></tr>
  <tr><td><hr></td></tr-->
  
</table>
</body>
</html>

<script language="javascript">

  var ip = "<%=ip%>";
  var space = "<%=space%>";
  var bucket = "<%=bucket%>";

window.onLoad=new function(){
  loadBucketSchemaInfo();
  loadBucketStats();
}

</script>

<%
    }
%>