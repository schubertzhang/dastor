<?php

# colors
$grossload_color = "CCCCCC";
$liveload_color = "0000FF";
$dastor_back_color = "CEE3F6";


/* Pass in by reference! */
function graph_dastorload_report ( &$rrdtool_graph ) {

    global $context,
           $hostname,
           $grossload_color,
           $liveload_color,
           $dastor_back_color,
           $range,
           $rrd_dir,
           $size,
           $strip_domainname;

    if ($strip_domainname) {
       $hostname = strip_domainname($hostname);
    }

    $rrdtool_graph['color'] = "BACK#'$dastor_back_color'";

    $title = 'DastorLoad';
    $rrdtool_graph['height'] += ($size == 'medium') ? 28 : 0;
    if ($context != 'host') {
       $rrdtool_graph['title'] = $title;
    } else {
       $rrdtool_graph['title'] = "$hostname $title last $range";
    }
    /* $rrdtool_graph['lower-limit']    = '0'; */
    $rrdtool_graph['vertical-label'] = 'Bytes';
    /* $rrdtool_graph['extras']         = '--rigid'; */

    $series = "DEF:'grossload'='${rrd_dir}/dastor_Storage_GrossLoad.rrd':'sum':AVERAGE "
       ."DEF:'liveload'='${rrd_dir}/dastor_Storage_LiveLoad.rrd':'sum':AVERAGE "
       ."AREA:'grossload'#$grossload_color:'GrossLoad' "
       ."LINE2:'liveload'#$liveload_color:'LiveLoad' ";

    $rrdtool_graph['series'] = $series;

    return $rrdtool_graph;

}

?>
