package com.test1;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		List<Double> numbers = new ArrayList<Double>();
		numbers.add(14.5);
		numbers.add(23.7);
		numbers.add(47.5);
		numbers.add(174.5);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("testSparkRDD1").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Double> numbersRDD = sc.parallelize(numbers);
		
		System.out.println(numbersRDD.count());

		sc.close();
	}

}
