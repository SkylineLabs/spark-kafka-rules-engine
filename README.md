The application is a big data ready business rules engine based on Apache Spark streaming.
The Streaming engine consumes data from Apache Kafka.
The data streamed has to be in JSON format.

**Needs Spark, Kafka, MongoDB installed**

Download the necessary libraries to compile the Spark application.
Save the mentioned jar files in the ‘jars’ folder of Apache Spark installation.

  1. spark-streaming-kafka-0-8-assembly_2.11-2.1.0.jar
  2. spark-streaming-kafka-0-8-assembly_2.11-2.1.0-sources.jar
  3. org.eclipse.paho.client.mqttv3-1.0.2.jar
  4. mongodb-driver-core-3.4.1.jar
  5. mongodb-driver-3.4.1.jar
  6. mongo-spark-connector_2.10-2.0.0.jar
  7. mail-1.4.1.jar
  8. courier_2.12-0.1.4.jar
  9. courier_2.12-0.1.4-sources.jar
  10.bson-3.4.1.jar


The jar versions mentioned above have been tested while development.
The Spark code is present in the ‘Spark’ folder submitted.
Run the Spark application using the following commands (In the same folder as the Scala file)
The following commands are for Spark installed at location “"/home/ubuntu/spark”

  1. scalac *.scala -classpath "/home/ubuntu/spark/jars/*”
  2. jar -cvf Iot.jar in/skylinelabs/spark/*.class /home/ubuntu/spark/jars/* 
  3. spark-submit --class in.skylinelabs.spark.IoT --master local Iot.jar


**This project is a sub-module of ConnectX IoT platform.**
**Refer https://github.com/SkylineLabs/ConnectX-IoT-platform for more details**


License
-------

Apache License Version 2.0



