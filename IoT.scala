package in.skylinelabs.spark

import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.eclipse.paho.client.mqttv3.MqttClient
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.json4s.jackson.JsonMethods.compact
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.JsonMethods.render

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.mongodb.spark.config.WriteConfig

import org.joda.time.DateTime

import kafka.serializer.StringDecoder
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.apache.http.entity.StringEntity
import org.joda.time.DateTimeZone

object IoT {

  def publishMQTT(publishTopic: String, finalMessage: String, client: MqttClient) {
    try {
      var messageMQTT = new MqttMessage(finalMessage.toString.getBytes("utf-8"))
      messageMQTT.setQos(0)
      if (client.isConnected()) {
        var msgTopic = client.getTopic(publishTopic)
        msgTopic.publish(messageMQTT)

      } else {
        try {
          val brokerUrl = "tcp://35.162.23.96:1883"
          var client = new MqttClient(brokerUrl, "admin/a/a")
          val mqttOptions = new MqttConnectOptions()
          mqttOptions.setUserName("admin/a/a")
          mqttOptions.setPassword("pass".toCharArray())
          client.connect(mqttOptions)
          var msgTopic = client.getTopic(publishTopic)
          messageMQTT = new MqttMessage(finalMessage.toString.getBytes("utf-8"))
          messageMQTT.setQos(0)
          msgTopic.publish(messageMQTT)
        } catch {
          case e: Exception => println("MQTT publish fail")
        }
      }
    } catch {
      case e: Exception => println("MQTT publish fail")
    }
  }

  //MQTT Publish state for specified device
  def publishMQTTStateSpecific(projectName: String, sc: SparkContext, thingID: String, deviceIDWithType: String, thingType: String, client: MqttClient) {
    try {
      println("Requested state for " + deviceIDWithType)
      val readMongoRequestStateURI = "mongodb://localhost"
      val StateGetConfigval = ReadConfig(Map("collection" -> projectName, "database" -> "state", "uri" -> readMongoRequestStateURI, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
      val messageRDD = MongoSpark.load(sc, StateGetConfigval).collect()
      val filteredMessageRdd = messageRDD.filter(doc => doc.getString("_id") == deviceIDWithType)
      val numberOfStates = filteredMessageRdd.length - 1
      val stateResponse = parse(filteredMessageRdd(numberOfStates).toJson()) //.getBytes("state") //Get last known state           
      val stateMessage = compact(render(stateResponse \ "state")).toString.substring(1).dropRight(1)
      println("JSON for state extracted is " + stateResponse)
      val publishTopic = "$state/" + projectName + "/" + thingType + "/" + thingID + "/" + "response"
      println("Publishing response to state request with topic " + publishTopic)
      publishMQTT(publishTopic, stateMessage, client)
    } catch {
      case e: Exception => println("Somethign went wrong, unable to publish state for " + thingType + "/" + thingID)
    }
  }

  //MQTT Publish state for thing type
  def publishMQTTStateAll(projectName: String, sc: SparkContext, deviceIDWithType: String, thingType: String, client: MqttClient) {
    try {
      println("Requested state for " + deviceIDWithType)
      val readMongoRequestStateURI = "mongodb://localhost"
      val StateGetConfigval = ReadConfig(Map("collection" -> projectName, "database" -> "state", "uri" -> readMongoRequestStateURI, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
      val messageRDD = MongoSpark.load(sc, StateGetConfigval).collect()
      val filteredMessageRdd = messageRDD.filter(doc => doc.getString("type") == thingType)
      val numberOfStates = filteredMessageRdd.length - 1

      if (numberOfStates != -1) {
        for (x <- 0 to numberOfStates) {
          val stateResponse = parse(filteredMessageRdd(x).toJson()) //.getBytes("state") //Get last known state           
          val stateMessage = compact(render(stateResponse \ "state")).toString.substring(1).dropRight(1)
          val topic = compact(render(stateResponse \ "_id")).toString.substring(1).dropRight(1)
          println("JSON for state extracted is " + stateMessage)
          val publishTopic = "$state/" + projectName + "/" + topic + "/" + "response"
          println("Publishing response to state request with topic " + publishTopic)
          publishMQTT(publishTopic, stateMessage, client)
        }
      }

    } catch {
      case e: Exception => println("Somethign went wrong, unable to publish state for thing type " + thingType)
    }

  }

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  def main(args: Array[String]) {

    var prevHour = 0

    val brokerUrl = "tcp://35.162.23.96:1883"
    var client = new MqttClient(brokerUrl, "admin/a/a")
    val mqttOptions = new MqttConnectOptions()
    mqttOptions.setUserName("admin/a/a")
    mqttOptions.setPassword("pass".toCharArray())
    try {
      client.connect(mqttOptions)
      if (client.isConnected()) {
        println("Connected to MQTT")
      } else {
        println("Check MQTT username and password")
      }

    } catch {
      case e: Exception => println("Check MQTT username and password OR Check internet")
    }

    val uriInit: String = "mongodb://localhost/rules.rules"
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("IoTRulesEngine")
      .set("spark.app.id", "IoTRulesEngine")
      .set("spark.mongodb.input.uri", uriInit)
      .set("spark.mongodb.output.uri", uriInit)

    val ssc = new StreamingContext(conf, Seconds(1))
    val sc = ssc.sparkContext
    val sqc = new SQLContext(sc)

    val topicsSet = Set("kafka")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.31.37.158:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_._2)
    lines.foreachRDD(rdd => {

      /***********************Time based rules****************************/
      try {

        try {
          var hour: Int = DateTime.now().getHourOfDay
          var minutes: Int = DateTime.now().getMinuteOfHour

          if (minutes > 30) {
            hour = hour + 6
          } else {
            hour = hour + 5
          }

          var hourmap: Array[String] = Array("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen", "twenty", "twentyone", "twentytwo", "twentythree")
          var hourString: String = hourmap(hour)

          if (prevHour != hour) {
            prevHour = hour
            print("\n\n\n\n\n\n\n\nExecuting Time rules from " + hourString)

            print("\nExecuting time rules \n")
            /*
           * MongoDb contains following
           * DB name rulesTimeDB
           * Collection name = 0, 1, 2.....23 (One for each hour)
           * Every collection has documents of format
           * message (To be sent to MQTT devices)
           * _id (Unique number)
           * action=mqtt (Always)
           * 
           * And typical content for normal mqtt rule
           */

            val rulesGetConfigval = ReadConfig(Map("database" -> "rulesTimeDB", "collection" -> hourString, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
            val firstRDD = MongoSpark.load(sc, rulesGetConfigval)

            if (firstRDD.count() != 0) {
              /*val rulesRDD = firstRDD.filter(doc => !dayString.equals(doc.getString("last_executed")))*/
              val rulesCollectedRdd = firstRDD.collect()

              //This ensures that $iot/projectName/thingType/ID rules saved also captures data from $iot/projectName/thingType/ID/something
              println("Number of time rules found : " + rulesCollectedRdd.length)
              val numberOfRules = rulesCollectedRdd.length - 1

              if (numberOfRules != -1) { //If number of rules != 0
                for (x <- 0 to numberOfRules) {
                  val y = x + 1
                  println("Time rule number : " + y)
                  val action = (rulesCollectedRdd(x)).getString("action")
                  println("Action : " + action)

                  val stateResponse = parse(rulesCollectedRdd(x).toJson()) //.getBytes("state") //Get last known state           
                  val resultRow = compact(render(stateResponse \ "message")).toString.substring(1).dropRight(1)

                  println("Message : " + resultRow)

                  action match {
                    case "mqtt" => {
                      val publishTopic = (rulesCollectedRdd(x)).getString("republish_topic")
                      val finalMessage = resultRow
                      try {
                        publishMQTT(publishTopic, finalMessage, client)
                        print("MQTT publish success")
                      } catch {
                        case e: Exception =>
                          println("MQTT error in rules output")
                          print("MQTT publish failed")
                      }
                    }

                    case _ => {

                    }
                  }
                }
              }
            }

          } else {
            print("Time rules already executed\n")
          }

        } catch {
          case e: Exception => println("Something went wrong with Time Rules")
        }
        /***********************Time based rules****************************/

        if (rdd.isEmpty()) {
          println("Empty :(")
        } else {

          val inputJSON = sqc.read.json(rdd.filter(JSON.parseFull(_).isDefined))
          println(inputJSON.toString)
          val inputCount = inputJSON.collect().length - 1

          val messageRow = inputJSON.select("message").collect()
          val topicArray = inputJSON.select("topic").collect()
          val topicArrayLength = topicArray.length

          println("Number of messages recieved are " + topicArrayLength)
          println(topicArray.toString)

          for (x <- 0 to inputCount) {
            try {
              println("Handling message number " + x)

              val topicInput = (topicArray(x))(0).toString
              val topicFull = topicInput.substring(1).toString
              println("Topic Input : " + topicInput)
              println("Topic remove dollar : " + topicFull)

              //Format expected topicFull = "iot/projectName/typeGroup/id"
              val splitMessage = topicFull.split("/")
              val topicLength = splitMessage.length - 1
              val messageType = splitMessage(0)
              val projectName = splitMessage(1)
              val thingType = splitMessage(2)

              //If state request, MQTT publish message, no need to do rules engine analytics, just publish the state via MQTT
              if ((splitMessage.length == 5 && splitMessage(0) == "state" && splitMessage(4) == "request") || (splitMessage.length == 5 && splitMessage(0) == "state" && splitMessage(4) == "response")) {
                try {

                  val thingID = splitMessage(3)
                  val deviceIDWithType = thingType + "/" + thingID

                  //If requesting state
                  if ((splitMessage.length == 5 && splitMessage(0) == "state" && splitMessage(4) == "request") && splitMessage(2) != "+") {
                    //If requesting state of a particular device

                    if (splitMessage(3) != "+") {
                      publishMQTTStateSpecific(projectName, sc, thingID, deviceIDWithType, thingType, client)
                    } else {
                      publishMQTTStateAll(projectName, sc, deviceIDWithType, thingType, client)
                    } //If requesting state of entire type
                  }

                  //If message is of response type
                  if (splitMessage.length == 5 && splitMessage(0) == "state" && splitMessage(4) == "response") {
                    println("Got state response for  " + deviceIDWithType)
                  }
                } catch {
                  case e: Exception => println("Somethign went wrong")
                }

              } else { //Else, if its not just a state request, do analytics

                val messageArr = messageRow.map(messageArr => messageArr.getSeq[org.apache.spark.sql.Row](0))
                val message = convertRowToJSON((messageArr(x))(0)).toString
                println("Message : " + message)

                if (splitMessage(0) == "alert" && splitMessage.length == 4) { //If alert message, write to alert db
                  try {
				  
                    val thingID = splitMessage(3)
                    var uriAlertMongoDB = "mongodb://localhost"
                    val writeConfigAlert = WriteConfig(Map("collection" -> projectName, "database" -> "alertDB", "uri" -> uriAlertMongoDB, "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))

                    val alertMessage = "{\"message\":[" + message + "], \"device_name\" : \"" + thingType + "/" + thingID + "\"}"
                    val jsonMessageRDD = sc.parallelize(alertMessage :: Nil)
                    val jsonMessageDF = sqc.read.json(jsonMessageRDD.filter(JSON.parseFull(_).isDefined))
                    MongoSpark.save(jsonMessageDF.write.mode("append"), writeConfigAlert)
                  } catch {
                    case e: Exception => println("Somethign went wrong")
                  }
                }

                //State update messages
                if (splitMessage.length == 5) {
                  val thingID = splitMessage(3)
                  val deviceIDWithType = projectName + "/" + thingType + "/" + thingID

                  if (splitMessage(0) == "state" && splitMessage(4) == "update" && splitMessage(3) != "+" && splitMessage(2) != "+") { //If state update, write to state DB, publish MQTT to inform all about updates
                    try {
                      println("Updating State db")
                      var uriUpdateStateMongoDB = "mongodb://localhost"
                      println(uriUpdateStateMongoDB)
                      val writeConfigState = WriteConfig(Map("writeConcern.w" -> "majority", "collection" -> projectName, "database" -> "state", "uri" -> uriUpdateStateMongoDB), Some(WriteConfig(sc)))
                      val timestamp: Long = System.currentTimeMillis / 1000
                      val stateMessage = "{\"_id\":\"" + deviceIDWithType + "\", \"timestamp\":" + timestamp + ",\"state\":[" + message + "],\"type\":\"" + thingType + "\"}"
                      println("State entered into MongoDB is " + stateMessage)
                      val stateMessageParallel = sc.parallelize(stateMessage :: Nil)
                      val jsonStateMessageDF = sqc.read.json(stateMessageParallel.filter(JSON.parseFull(_).isDefined))
                      println("Dataframe State update " + jsonStateMessageDF.toString)
                      MongoSpark.save(jsonStateMessageDF.write.mode("append"), writeConfigState)
                      publishMQTTStateSpecific(projectName, sc, thingID, deviceIDWithType, thingType, client)
                    } catch {
                      case e: Exception => println("Somethign went wrong in writing to state DB")
                    }
                  }
                }

                try {
                  val rulesGetConfigval = ReadConfig(Map("database" -> "rulesDB", "collection" -> projectName, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
                  val firstRDD = MongoSpark.load(sc, rulesGetConfigval)

                  if (firstRDD.count() != 0) {
                    if (firstRDD.filter(doc => doc.getString("kafka_topic") != None).count() != 0) {
                      val rulesRDD = firstRDD.filter(doc => topicInput.contains(doc.getString("kafka_topic")))
                      val rulesCollectedRdd = rulesRDD.collect()

                      //This ensures that $iot/projectName/thingType/ID rules saved also captures data from $iot/projectName/thingType/ID/something
                      println("Number of rules found : " + rulesCollectedRdd.length)
                      val numberOfRules = rulesCollectedRdd.length - 1

                      if (numberOfRules != -1) { //If number of rules != 0
                        for (x <- 0 to numberOfRules) {
                          val y = x + 1
                          println("Rule number : " + y)
                          val action = (rulesCollectedRdd(x)).getString("action")
                          println("Action : " + action)
                          val query = (rulesCollectedRdd(x)).getString("query")
                          println("Query : " + query)

                          val jsonMessageRDD = sc.parallelize(message :: Nil)
                          val jsonMessageDF = sqc.read.json(jsonMessageRDD.filter(JSON.parseFull(_).isDefined))
                          jsonMessageDF.createOrReplaceTempView("message")
                          val queryResult = sqc.sql(query)
                          val resultArray = queryResult.collect()
                          val resultRow = convertRowToJSON(resultArray(0))
                          println("Result : " + resultRow)

                          action match {
                            case "mqtt" => {
                              val publishTopic = (rulesCollectedRdd(x)).getString("republish_topic")
                              val finalMessage = resultRow
                              try {

                                (publishTopic, finalMessage, client)
                                print("MQTT publish success")
                              } catch {
                                case e: Exception =>
                                  println("MQTT error in rules output")
                                  print("MQTT publish failed")
                              }
                            }

                            case "mongodb" => {
                              val mongoServer = (rulesCollectedRdd(x)).getString("mongoServer")
                              val mongoDB = (rulesCollectedRdd(x)).getString("mongoDB")
                              val mongoCollection = (rulesCollectedRdd(x)).getString("mongoCollection")

                              try {
                                val writeConfigRulesEngine = WriteConfig(Map("database" -> mongoDB, "collection" -> mongoCollection, "uri" -> mongoServer, "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
                                MongoSpark.save(jsonMessageDF.write.mode("append"), writeConfigRulesEngine)
                                print("Write to MongoDB success")
                              } catch {
                                case e: Exception => println("Rules engine error MongoDB write fail")
                              }
                            }

                            case "email" => {
                              val to = (rulesCollectedRdd(x)).getString("to")
                              val message = (rulesCollectedRdd(x)).getString("message")
                              val subject = (rulesCollectedRdd(x)).getString("subject")
                              val url = "http://skylinelabs.in/connectx/email.php";
                              val finalMessage = resultRow

                              val post = new HttpPost(url)
                              val client = new DefaultHttpClient

                              val nameValuePairs = new ArrayList[NameValuePair](2)
                              nameValuePairs.add(new BasicNameValuePair("to", to));
                              nameValuePairs.add(new BasicNameValuePair("subject", subject));
                              nameValuePairs.add(new BasicNameValuePair("message", message + "\n\n" + "Message obtained from device is : \n" + finalMessage));
                              post.setEntity(new UrlEncodedFormEntity(nameValuePairs));

                              val response = client.execute(post)

                            }

                            case "http" => {

                              /*MongoDB data saved :
                              action
                              number 
                              key1, value1
                              .
                              .
                              .
                              */

                              val number = (rulesCollectedRdd(x)).getString("number").toInt
                              val url = (rulesCollectedRdd(x)).getString("url")

                              val finalMessage = resultRow

                              val post = new HttpPost(url)
                              val client = new DefaultHttpClient

                              for (n <- 1 to number) {
                                var keyNumber = "key" + n.toString
                                var valueNumber = "value" + n.toString()
                                val keyWithNumber = (rulesCollectedRdd(x)).getString("keyNumber")
                                val valueWithNumber = (rulesCollectedRdd(x)).getString("valueNumber")
                                post.addHeader(keyWithNumber, valueWithNumber)
                              }

                              post.setEntity(new StringEntity(finalMessage))
                              val response = client.execute(post)

                            }

                            case _ => {

                            }
                          }
                        }
                      }
                    }
                  }
                } catch {
                  case e: Exception => println("Rules engine errror : No rule found")
                }
              }
            } catch {
              case e: Exception => println("Something went wrong iterating messages in RDD")
            }
          }

        }
      } catch {
        case e: Exception => println("Something went wrong with RDD")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

