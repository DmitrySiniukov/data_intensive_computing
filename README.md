# LAB 1

`javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java`

`jar -cvf topten.jar -C topten_classes/ .`

`$HADOOP_HOME/bin/hadoop jar topten.jar topten.TopTen /topten_input`

## Check in hbase:

`$HBASE_HOME/bin/hbase shell`

`scan 'topten'`


--

Delete table in hbase:

`disable 'topten'`

`drop 'topten'`