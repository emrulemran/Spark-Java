package com.sundogsoftware.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadTextFile {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("reading text file").setMaster("local");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = sc.textFile("C:\\SparkScala\\SparkScalaCourse\\textfile\\sample.txt");

		for (String line : lines.collect()) {
			System.out.println(line);
		}
	}

}
