package PackageOne;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import PackageOne.FriendLists.Reduce;

public class FindMutualFriends {
	
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{

		
		private Text key_data = new Text(); // type of output key
		private Text value_text = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context) 
		throws IOException, InterruptedException {
			String[] key_value_data = value.toString().split("\t");
			String[] value_data;
			//key_txt.set(mydata[0]);
			if(key_value_data.length > 1){
				value_data = key_value_data[1].split(",");
				String[] temp = new String[2];
				for (String data : value_data) {
					temp[0]=key_value_data[0];
					temp[1]=data;
					Arrays.sort(temp);
					
					key_data.set(temp[0] + "," +temp[1]);
					value_text.set(key_value_data[1]);
					
					context.write(key_data , value_text); // create a pair <keyword, 1>
				}
			}
		}

	}
	
	public static class Reducer1 extends Reducer<Text,Text,Text,IntWritable>{
		private Text friend_lists = new Text();
		private HashMap<Text, IntWritable> hmap = new HashMap<>();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
		InterruptedException{
			String[] data = new String[2];
	
			    int index = 0;
			    for(Text value : values){
			            data[index++] = value.toString();
			    }
			
		    String[] list1 = data[0].split(",");
		    String[] list2 = data[1].split(",");
			
		    List<String> list = new LinkedList<String>();
		    Arrays.sort(list1);
			Arrays.sort(list2);
	
			int a = 0, b = 0;
			while (a< list1.length && b< list2.length){
				if(list1[a].equals(list2[b])){
				list.add(list1[a]);
				a=a+1;
				b=b+1;
				}
				else if((list1[a].compareTo(list2[b])) < 0) {
					a = a+1;
				}
				else {
					b=b+1;
				}
	
			}
			
			
			//use cleanup method to add the key-value pairs in a hashmap
		    hmap.put(new Text(key), new IntWritable(list.size()));
			
			}
		
		@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
			
			//sort the hashmap by value in descending order
			Map<Text, IntWritable> map = sort_descending(hmap);
			
			//print only top 10 
		    int count =0;
			   for (Text key: map.keySet()){
				    if(count<10){
			            IntWritable value = hmap.get(key);   
			            context.write(key,value);  
			            count++;
				    }
			   }
		}


		//Mthod to sort the hashmap by descending
		private Map<Text, IntWritable> sort_descending(Map<Text, IntWritable> map) {
			List l1 = new LinkedList(map.entrySet());
			Collections.sort(l1, new Comparator() {
			     public int compare(Object o1, Object o2) {
			          return -((Comparable) ((Map.Entry) (o1)).getValue())
			         .compareTo(((Map.Entry) (o2)).getValue());
			     }
			});

		   Map sorted_map = new LinkedHashMap();
		   for (Iterator iterator = l1.iterator(); iterator.hasNext();) {
		       Map.Entry entry = (Map.Entry)iterator.next();
		       sorted_map.put(entry.getKey(), entry.getValue());
		   }

		   return sorted_map;
		}

	}


	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: FindMutualFriends <in> <out>");
			System.exit(2);
		}


		Job job = new Job(conf, "FindMutualFriends");
		
		job.setJarByClass(FriendLists.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		
		
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
