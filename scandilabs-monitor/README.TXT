
# To build the project:
mvn compile assembly:single
scp target/catamaran-monitor-1.0-SNAPSHOT-jar-with-dependencies.jar [host]:[log_file_directory]

# Configure
Copy log-monitor.properties.sample to [host]:[log_file_directory]/log-monitor.properties
Then edit log-monitor.properties

# To run:
ssh [host]
cd [log_file_directory]
nohup java -jar catamaran-monitor-1.0-SNAPSHOT-jar-with-dependencies.jar &

# To check that everything is working right:
cat [log_file_directory]/nohup.out

# To stop:
ps -ef | grep java
[find the right process id]
kill [process_id]