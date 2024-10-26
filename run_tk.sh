#!/bin/bash

OUTPUTPATH="tk_res/"

echo ===== TopK JAVA VERSION =====

echo ===== Compile =====
javac -classpath `/hadoop/bin/yarn classpath` TopKOutDegree.java
jar cf tk.jar TopKOutDegree*.class
echo
echo ===== Clear old output files on HDFS =====
/hadoop/bin/hdfs dfs -rm -r $OUTPUTPATH
echo
echo ===== RUN CASE1=====
/hadoop/bin/yarn jar tk.jar TopKOutDegree /hw5_data/case1 $OUTPUTPATH"case1" 20
echo
echo ===== RUN CASE2=====
/hadoop/bin/yarn jar tk.jar TopKOutDegree /hw5_data/case2 $OUTPUTPATH"case2" 20
echo

echo DONE!

echo You can use "/hadoop/bin/hdfs dfs -get tk_res/ {your_local_path}" to get the result file
echo
