package org.avinash.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws InterruptedException,IOException{

//		System.out.println("Mapper1");
//		String line = value.toString();
//		String[] words = line.split(" ");
//		for(String word: words) {
//			context.write(new Text(word), new IntWritable(1));
//		}
		Arrays.stream(value.toString().split(" ")).forEach(word -> {
			try {
				context.write(new Text(word), new IntWritable(1));
				System.out.println("Mapper2" + word);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
	}

}
