package com.upgrad.sparkassignment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * This program is to Filter all the records having RatecodeID as 4
 * 
 * @author MaddelaV
 *
 */
public class FilterOperation {
	// This is my second committ
	public static void main(String[] args) {
		// Creating Spark Configuration object for Spark Application - Filter Operation
		SparkConf scnf = new SparkConf()
				.setAppName("Filter Operation")
				.setMaster("local[*]");
		
		// Java Spark Context object created using Spark Configuration object to work with Java Code
		JavaSparkContext jsc = new JavaSparkContext(scnf);
		
		// Create JAVARDD from multiple files - this can also be passed in as argument to program
		JavaRDD<String> inputData = jsc.textFile("/user/root/spark_assignment/input_dataset/yellow_tripdata_*.*");
		
		// Remove header from CSV files provided and also discard any empty rows
		String header = inputData.first();		
		inputData = inputData.filter(row -> row != header).filter(x -> !x.isEmpty());
		

		final String rateCodeID = "4";
		
		// Filtering out records which have RatecodeID as 4
		JavaRDD<String> filteredData = inputData.filter(x -> {
				
				String[] record = x.split(","); 
				
				//Each record has 6 field as RatecodeID
				String ratecodeID = record[5];
				
				// checking if value is neither null nor empty
				if (ratecodeID != null && ratecodeID != "" && rateCodeID.equals(ratecodeID)) {
					return true;
				} else {
					return false;
				}
		});
		
		// Save the filtered records to a text file, output location is passed in as argument
		filteredData.saveAsTextFile(args[0]);
		
		// Close Java Spark Context which releases any system resources associated with it
		jsc.close();

	}

}
