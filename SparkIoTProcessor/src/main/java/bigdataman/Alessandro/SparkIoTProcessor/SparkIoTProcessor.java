package bigdataman.Alessandro.SparkIoTProcessor;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;

import static com.datastax.spark.connector.cql.CassandraConnector.*;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import kafka.serializer.StringDecoder;

public class SparkIoTProcessor {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		System.out.println("Spark Streaming started now .....");

		//.set('spark.cassandra.connection.host', 'localhost:9042')
		SparkConf conf = new SparkConf().setAppName("kafka-sandbox")
				.setMaster("local[*]");
				//.set("spark.cassandra.connection.host", "127.0.0.1")
				//.set("spark.cassandra.connection.port", "9042")
				//.set("spark.cassandra.connection.timeout_ms", "5000");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("test", "temperatura", CassandraJavaUtil.mapColumnTo(String.class)).select("country");
		
		
		// batchDuration - The time interval at which streaming data will be divided into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));
		
		//JavaRDD<String> cassandraRowsRDD = javaFunctions(sc).cassandraTable("test", "temperatura", mapColumnTo(String.class)).select("country");

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "127.0.0.1:9092");
		Set<String> topics = Collections.singleton("test1");
		/*
		List<Temperatura> allRecord = new ArrayList<Temperatura>();
		Temperatura t = new Temperatura("a", (float) 0.2, "ciaone");
		allRecord.add(t);
		JavaRDD<Temperatura> rdd2 = sc.parallelize(allRecord);
		CassandraJavaUtil.javaFunctions(rdd2).writerBuilder("test", "temperatura", CassandraJavaUtil.mapToRow(Temperatura.class)).saveToCassandra();
		*/

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		  //List<Temperatura> allRecord = new ArrayList<Temperatura>();
		  final String COMMA = ",";
		  directKafkaStream.foreachRDD(rdd -> {
			  
		  System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			  if(rdd.count() > 0) {
				List<Temperatura> allRecord = new ArrayList<Temperatura>();
				rdd.collect().forEach(rawRecord -> {
					  String riga = rawRecord._2;
					  
					  String[] dati = riga.split(",");
					  //System.out.println(dati[0] + ((dati[6] == "AvgTemperature") ? "30" : dati[6]) + dati[1]);
					  allRecord.add( new Temperatura(UUID.randomUUID().toString(), Float.parseFloat(dati[6]), dati[1]));
					 /* 
					  StringTokenizer st = new StringTokenizer(record,",");		  
					  StringBuilder sb = new StringBuilder(); 
					  while(st.hasMoreTokens()) {
						 String region = st.nextToken();
						 String country = st.nextToken();
						 String city = st.nextToken();
						 String mese = st.nextToken();
						 String giorno = st.nextToken();
						 String anno = st.nextToken();
						 String average = st.nextToken();
						 sb.append(region).append(COMMA).append(country).append(COMMA).append(city).append(COMMA).append(mese).append(COMMA).append(giorno).append(COMMA).append(anno).append(COMMA).append(average);
						 allRecord.add(sb.toString());
					  } */
					  
				  });
				System.out.println("All records OUTER MOST :"+allRecord.size());
				JavaRDD<Temperatura> rdd2 = sc.parallelize(allRecord);
				CassandraJavaUtil.javaFunctions(rdd2).writerBuilder("test", "temperatura", CassandraJavaUtil.mapToRow(Temperatura.class)).saveToCassandra();
				System.out.println("dati caricati nel db");
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
	

}

