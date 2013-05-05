<?php

# colors
$write_color = "3333bb";
$write_avg_color = "33cc33";
$dastor_back_color = "CEE3F6";

/* Pass in by reference! */
function graph_dastorwrites_report ( &$rrdtool_graph ) {

    global $context,
           $hostname,
           $write_color,
           $write_avg_color,
           $dastor_back_color,
           $range,
           $rrd_dir,
           $size,
           $strip_domainname;

    if ($strip_domainname) {
       $hostname = strip_domainname($hostname);
    }

    $rrdtool_graph['color'] = "BACK#'$dastor_back_color'";

    $title = 'DastorWrites';
    $rrdtool_graph['height'] += ($size == 'medium') ? 28 : 0;
    if ($context != 'host') {
       $rrdtool_graph['title'] = $title;
    } else {
       $rrdtool_graph['title'] = "$hostname $title last $range";
    }
    $rrdtool_graph['lower-limit']    = '0';
    $rrdtool_graph['vertical-label'] = 'Ops/Sec';
    $rrdtool_graph['extras']         = '--rigid';

    $series = "DEF:'write'='${rrd_dir}/dastor_Service_WriteThroughput.rrd':'sum':AVERAGE "
       ."VDEF:'writeavrg'='write',AVERAGE "
       ."LINE2:'write'#$write_color:'Writes' "
       ."LINE2:'writeavrg'#$write_avg_color:'WritesAverage' ";
       
    $rrdtool_graph['series'] = $series;

    return $rrdtool_graph;

}

?>
