setlocal
set ZOOMAIN=com.linemetrics.monk.DataMonk
echo off
java -cp "service.jar" %ZOOMAIN% 2> error.log > service.log
endlocal
