<?php
/**
 * Created by IntelliJ IDEA.
 * User: shisongran
 * Date: 2017/2/8
 * Time: 下午3:24
 */

$GLOBALS["MYSQL_HOST"] = "";
$GLOBALS["MYSQL_USER"] = "";
$GLOBALS["MYSQL_PASSWORD"] = "";
$GLOBALS["MYSQL_DATABASE"] = "";


$GLOBALS["AREA_DEFAULT"] = 1;

$GLOBALS["WORKER_UPDATE_ID_IN_SINGLE_QUERY"] = 10;
$GLOBALS["WORKER_QUERY_INTERVAL"] = 1000000;


Go();

function Go(){
    $rangeInfo = FindIdRange();
    if (false == $rangeInfo[0]){
        warning("FindIdRange failed. MysqlErrorNo[%d], MysqlError[%s]", $rangeInfo[1], $rangeInfo[2]);
        exit(1);
    }

    WorkerExec($rangeInfo[1], $rangeInfo[2]);
}

function WorkerExec($start, $lastId){
    trace("Start Worker With id range [%d, %d]", $start, $lastId);

    $cursor = $start;
    $realLastId = $lastId + 1; // +1 保证所有ID都能遍历到

    while ($cursor < $realLastId){
        $end = $cursor + $GLOBALS["WORKER_UPDATE_ID_IN_SINGLE_QUERY"];
        $end = $end > $realLastId ? $realLastId : $end;

        UpdateAreaId($cursor, $end);

        usleep($GLOBALS["WORKER_QUERY_INTERVAL"]);

        $cursor = $end;
    }
}

function FindIdRange(){
    $dbConnect = mysqli_connect($GLOBALS["MYSQL_HOST"],
        $GLOBALS["MYSQL_USER"],
        $GLOBALS["MYSQL_PASSWORD"],
        $GLOBALS["MYSQL_DATABASE"]);

    if (!$dbConnect) {
        return array(false, mysqli_connect_errno(), mysqli_connect_error());
    }

    $result = mysqli_query($dbConnect, "SELECT id FROM xxx ORDER BY id ASC LIMIT 1");
    if (!$result) {
        return array(false, mysqli_errno($dbConnect), mysqli_error($dbConnect));
    }

    $rowArray = $result->fetch_assoc();

    $startId = $rowArray["id"];

    $result = mysqli_query($dbConnect, "SELECT id FROM xxx ORDER BY id DESC LIMIT 1");
    if (!$result) {
        return array(false, mysqli_errno($dbConnect), mysqli_error($dbConnect));
    }

    $rowArray = $result->fetch_assoc();

    $endId = $rowArray["id"];

    mysqli_close($dbConnect);

    return array(true, $startId, $endId);
}

function UpdateAreaId($start, $finish){

    $sqlFormat = "UPDATE xxx SET xxxx = %d WHERE id >= %d AND id < %d";

    $realSql = sprintf($sqlFormat, $GLOBALS["AREA_DEFAULT"], $start, $finish);


    $result = doMysqlUpdateQuery($realSql);

    switch ($result[0]){
        case true:
            trace("UpdateAreaId SQL[%s] Success. AffectRow[%d]", $realSql, $result[1]);
            return false;
        case false:
            warning("UpdateAreaId SQL[%s] Fail. MysqlErrorNo[%d], MysqlError[%s]", $realSql, $result[1], $result[2]);
            return true;
    }

    return false;
}

function doMysqlUpdateQuery($sql){
    $dbConnect = mysqli_connect($GLOBALS["MYSQL_HOST"],
        $GLOBALS["MYSQL_USER"],
        $GLOBALS["MYSQL_PASSWORD"],
        $GLOBALS["MYSQL_DATABASE"]);

    if (!$dbConnect) {
        return array(false, mysqli_connect_errno(), mysqli_connect_error());
    }

    $result = mysqli_query($dbConnect, $sql);
    if (!$result) {
        return array(false, mysqli_errno($dbConnect), mysqli_error($dbConnect));
    }

    $affectRows = mysqli_affected_rows($dbConnect);

    mysqli_close($dbConnect);

    return array(true, $affectRows);
}

function warning(){
    $args = func_get_args();
    $fmt = array_shift($args);

    $str = vsprintf($fmt, $args);
    writeLog("\033[31m [WARNING] \033[0m", $str);
}

function trace(){
    $args = func_get_args();
    $fmt = array_shift($args);

    $str = vsprintf($fmt, $args);
    writeLog("[TRACE]", $str);
}

function writeLog($level, $str){
    $depth = 1;

    $trace = debug_backtrace();
    if ($depth >= count($trace)) {
        $depth = count($trace) - 1;
    }

    $file = basename($trace[$depth]['file']);
    $line = $trace[$depth]['line'];

    $str = sprintf("%s %s pid[%d] [%s:%d] %s", $level, date('m-d H:i:s:', time()), getmypid(), $file, $line, $str);

    echo($str . "\n");
}


