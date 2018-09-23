javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java^C


jar -cvf topten.jar -C topten_classes/ .

$HADOOP_HOME/bin/hadoop jar topten.jar topten.TopTen /topten_input /topten_output^C
