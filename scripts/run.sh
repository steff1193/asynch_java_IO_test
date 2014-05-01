#!/bin/bash

facultyOf="$1"

top -b | grep --line-buffered "Cpu" >> synch.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} synch &>>./stdout.txt;
kill $top_pid
top -b | grep --line-buffered "Cpu" >> synch_1_thread.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} synch_threaded 1 &>>./stdout.txt;
kill $top_pid
top -b | grep --line-buffered "Cpu" >> asynch_1_thread.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} asynch_threaded 1 &>>./stdout.txt;
kill $top_pid
top -b | grep --line-buffered "Cpu" >> synch_30_threads.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} synch_threaded 30 &>>./stdout.txt;
kill $top_pid
top -b | grep --line-buffered "Cpu" >> asynch_30_threads.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} asynch_threaded 30 &>>./stdout.txt;
kill $top_pid
top -b | grep --line-buffered "Cpu" >> asynch_future.top &
top_pid=$!
/usr/lib/jvm/java-7-oracle/bin/java -classpath . dk.designware.asynchiotest.MyAsyncDiskIOTest ./test.dat 2000000 20000 ${facultyOf} asynch_future &>>./stdout.txt;
kill $top_pid
