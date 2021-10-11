#COMPILAZIONE JAVA 
- rm -r build/ && rm -r Challenge.jar 
- mkdir build 
- javac -cp $(hadoop classpath) -d build src/*
- jar cvf Challenge.jar -C build .
#ESECUZIONE 
- hadoop jar Challenge.jar ChallengeDriver (eventuali parametri solo nel caso si voglia modificare i path dei file)

#CONTROLLO RISULTATI 
- hdfs dfs -ls /output6/ -> per l'analisi delle reviews
- hdfs dfs -cat /output6/part-r-0000<0-11> -> per l'analisi delle reviews
- hdfs dfs -ls /output6SENTI/ -> per l'analisi del preprocessing del sentinet



