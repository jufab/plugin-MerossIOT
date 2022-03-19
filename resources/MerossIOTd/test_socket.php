<?php
$apikey = "t19UsOSOcMdSp3hZb72yv6ayFVga5gKgPYY1JQhs8jTUfrR7SYvgPImYYsJICK0h";
$sock = 'unix://MerossIOTd.sock';
$fp = stream_socket_client($sock, $errno, $errstr);
$result = '';
if ($fp) {
    //{"action":"set_on","args":["1910118690659525186748e1e9100794","0"],"apikey":"vRGn39PrJNKPB3iQN72GKmSEbua2yLlTiey2ccUD4NGcrcgCtObl1xvi3lNCdcxX"}
    $query = [ 'action' => 'set_off', 'args' => ['1910118690659525186748e1e9100794','0'], 'apikey' => $apikey ];
    //$query = [ 'action' => 'get_devices_meross', 'args' => '', 'apikey' => $apikey ];
    fwrite($fp, json_encode($query));
    while (!feof($fp)) {
        $result .= fgets($fp, 1024);
    }
    fclose($fp);
}
//$result = (is_json($result)) ? json_decode($result, true) : $result;
print_r($result);
?>
