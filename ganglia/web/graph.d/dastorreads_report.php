<?php

# colors
$read_color = "3333bb";
$read_avg_color = "33cc33";
$dastor_back_color = "CEE3F6";

/* Pass in by reference! */
function graph_dastorreads_report ( &$rrdtool_graph ) {

    global $context,
           $hostname,
           $read_color,
           $read_avg_color,
           $dastor_back_color,
           $range,
           $rrd_dir,
           $size,
           $strip_domainname;

    if ($strip_domainname) {
       $hostname = strip_domainname($hostname);
    }

    $rrdtool_graph['color'] = "BACK#'$dastor_back_color'";

    $title = 'DastorReads';
    $rrdtool_graph['height'] += ($size == 'medium') ? 28 : 0;
    if ($context != 'host') {
       $rrdtool_graph['title'] = $title;
    } else {
       $rrdtool_graph['title'] = "$hostname $title last $range";
    }
    $rrdtool_graph['lower-limit']    = '0';
    $rrdtool_graph['vertical-label'] = 'Ops/Sec';
    $rrdtool_graph['extras']         = '--rigid';

    $series = "DEF:'read'='${rrd_dir}/dastor_Service_ReadThroughput.rrd':'sum':AVERAGE "
       ."VDEF:'readavrg'='read',AVERAGE "
       ."LINE2:'read'#$read_color:'Reads' "
       ."LINE2:'readavrg'#$read_avg_color:'ReadsAverage' ";

    $rrdtool_graph['series'] = $series;

    return $rrdtool_graph;

}

?>
