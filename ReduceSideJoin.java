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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import PackageOne.WordCount.Mapper1;
import PackageOne.WordCount.Reducer1;

public class ReduceSideJoin {
	
	public static class Review_Mapper extends Mapper<Object, Text, Text, FloatWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        	
        	String[] review_data = value.toString().split("::"); 
        	
            Float stars = Float.parseFloat(review_data[3]);
            
            Text review_keys = new Text(review_data[2]);
            FloatWritable review_values = new FloatWritable(stars);
            
            context.write(review_keys, review_values);  
        }
    }

	public static class Business_Mapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] business_data = value.toString().split("::");
			String business_key = business_data[0];
			String business_value = "Business" + "\t" + business_data[1] + "\t" + business_data[2];
            
            context.write(new Text(business_key), new Text(business_value));  
        }
	}
	public static class Mapper_Top_Ten extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] data = value.toString().split("\t");
			String business_id = data[0];
			String avg_stars = data[1];
            
            context.write(new Text(business_id), new Text(avg_stars));  
        }
		
	}
    public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    	
    	private Map<Text, FloatWritable> average_business_rating = new HashMap<Text, FloatWritable>();
        
    	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        	float sum = 0;
        	int total = 0;
        	
        	for(FloatWritable value : values) {
        		sum = sum + value.get();
        		total++;
        	}
        	float avg_rating = sum/total;
        	
        	average_business_rating.put(new Text(key), new FloatWritable(avg_rating));
        }
        
        public static <K extends Comparable, V extends Comparable> Map<K, V> sort_hashmap(Map<K, V> map) {
            List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(list, new Comparator<Map.Entry<K, V>>() {

                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            Map<K, V> final_map = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : list) {
                final_map.put(entry.getKey(), entry.getValue());
            }

            return final_map;
        }

        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	
        		Map<Text, FloatWritable> final_sorted_map = sort_hashmap(average_business_rating);
        		int count = 0;
                for (Text key : final_sorted_map.keySet()) {
                    if (count++ == 10) {
                        break;
                    }
                    
                    context.write(key, final_sorted_map.get(key));
                }
        }
        
    }

    public static class Reduce_Side_Join_Reducer extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String avg_rating = "";
    		String business_category = "";
    		boolean b = false;
    		for(Text value : values) {
    			String d = value.toString();
    			if(d.contains("Business")) {
    				d = d.replace("Business", "");
    				business_category = d;
    			} else {
    				avg_rating = d;
    				b = true;
    			}
    		}
    		if(!b)
    			return;

    		context.write(new Text(key), new Text(business_category + "\t" + avg_rating));
    	}
    }
    // Driver program
    public static void main(String[] args) throws Exception {
    	   Configuration conf = new Configuration();
           String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
           if (otherArgs.length != 4) {
               System.err.println("Usage: Top Ten Business <input1> <input2> <temp output> <final output>");
               System.exit(2);
           }
            
           Job job1 = Job.getInstance(conf, "Top Ten Business with Rating");
           job1.setJarByClass(ReduceSideJoin.class);
    
           job1.setMapperClass(Review_Mapper.class);
           job1.setReducerClass(Reduce.class);
    
           job1.setMapOutputKeyClass(Text.class);
           job1.setMapOutputValueClass(FloatWritable.class);
    
           job1.setOutputKeyClass(Text.class);
           job1.setOutputValueClass(FloatWritable.class);
           job1.setNumReduceTasks(1);
    
           Path reviewPath = new Path(otherArgs[0]);
           Path businessPath = new Path(otherArgs[1]);
           Path intermediatePath = new Path(otherArgs[2]);
           Path outputPath = new Path(otherArgs[3]);
    
           FileInputFormat.addInputPath(job1, reviewPath);
           FileOutputFormat.setOutputPath(job1, intermediatePath);
           job1.waitForCompletion(true);
    
           Job job2 = Job.getInstance(conf, "ReduceSideJoin");
           job2.setJarByClass(ReduceSideJoin.class);
           MultipleInputs.addInputPath(job2, businessPath, TextInputFormat.class, Business_Mapper.class);
           MultipleInputs.addInputPath(job2, intermediatePath, TextInputFormat.class, Mapper_Top_Ten.class);
    
           job2.setReducerClass(Reduce_Side_Join_Reducer.class);
    
           job2.setOutputKeyClass(Text.class);
           job2.setOutputValueClass(Text.class);
           FileInputFormat.setMinInputSplitSize(job2, 500000000);
    
           FileSystem hdfsFS = FileSystem.get(conf);
           if (hdfsFS.exists(outputPath)) {
               hdfsFS.delete(outputPath, true);
           }
           FileOutputFormat.setOutputPath(job2, outputPath);
    
           job2.waitForCompletion(true);
    
           hdfsFS.delete(intermediatePath, true);
    }

}
