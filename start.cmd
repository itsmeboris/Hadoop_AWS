:: BatchGotAdmin
:-------------------------------------
REM  --> Check for permissions
>nul 2>&1 "%SYSTEMROOT%\system32\cacls.exe" "%SYSTEMROOT%\system32\config\system"

REM --> If error flag set, we do not have admin.
if '%errorlevel%' NEQ '0' (
    echo Requesting administrative privileges...
    goto UACPrompt
) else ( goto gotAdmin )

:UACPrompt
    echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs"
    set params = %*:"=""
    echo UAC.ShellExecute "cmd.exe", "/c %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs"

    "%temp%\getadmin.vbs"
    del "%temp%\getadmin.vbs"
    exit /B

:gotAdmin

    REM ****Application is Starting****

    cd C:\Users\boris\Ex2

    start "Configurations" configuration.bat

    echo please wait for HDFS to finish loading... then press a key...

    TASKKILL /F /FI "WINDOWTITLE eq Administrator: Configurations"
    TIMEOUT 10

    start cmd.exe /c hdfs dfsadmin -safemode leave
    TIMEOUT 5

    start cmd.exe /c %HADOOP_HOME%\bin\hadoop fs -rm -R /user
    TIMEOUT 10
    start cmd.exe /c %HADOOP_HOME%\bin\hadoop fs -rm -R /tmp
    TIMEOUT 10
    start cmd.exe /c %HADOOP_HOME%\bin\hadoop fs -mkdir -p /user/input
    TIMEOUT 10
    start cmd.exe /c %HADOOP_HOME%\bin\hadoop fs -put src/main/resources/bible+shakes.nopunc /user/input
    TIMEOUT 10

    start "Connections" connections.bat
    TIMEOUT 10

    TASKKILL /F /FI "WINDOWTITLE eq Administrator: Connections"
    TIMEOUT 10

    start cmd.exe /c %HADOOP_HOME%/bin/hadoop jar target/Ex2-1.0-SNAPSHOT-jar-with-dependencies.jar

    echo please wait till Map Reduce completes
    PAUSE
    start cmd.exe /c %HADOOP_HOME%\bin\hadoop fs -get /user/output/bible+shakes.nopunc/part-r-00000 output.txt
    TIMEOUT 10