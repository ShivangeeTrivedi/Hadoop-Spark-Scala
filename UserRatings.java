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
import java.util.HashSet;
import java.util.Set;

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

public class UserRatings {
	
	public static class User_Ratings_Mapper extends Mapper<Object, Text, Text, Text> {
        Set<String> hs = new HashSet<String>();
        String mapper_input;
 
        public void setup(Context context) throws IOException, InterruptedException {
             
            Configuration conf = context.getConfiguration();
            mapper_input = conf.get("input");
             
            try {
                URI[] cache_location = context.getCacheFiles();
                URI uri = cache_location[0];
                BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));
                String s = br.readLine();
                while (s != null) {
                    if (s.contains(mapper_input)) {
                        hs.add(s.split("::")[0]);
                    }
                    s = br.readLine();
                }
                br.close();
            } catch (Exception e) {
                System.out.println(e);
            }
        }
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
            String[] columns = value.toString().split("::");
            String rating = columns[3];
            String business_id = columns[2];
            String user_id = columns[1];
            if (hs.contains(business_id)) {
                context.write(new Text(user_id.toString()), new Text(rating));
            }
 
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: UserRatings <input1> <input2> <output>");
            System.exit(2);
        }
        conf.set("input", "Palo Alto,");
         
        Job job = Job.getInstance(conf, "UserRatings");
        job.setJarByClass(UserRatings.class);
 
        job.setMapperClass(User_Ratings_Mapper.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        Path input = new Path(otherArgs[0]);
        Path cacheFilePath = new Path(otherArgs[1]);
        Path output = new Path(otherArgs[2]);
 
        FileInputFormat.addInputPath(job, input);
 
        job.addCacheFile(cacheFilePath.toUri());
 
        FileSystem hdfsFS = FileSystem.get(conf);
        if (hdfsFS.exists(output)) {
            hdfsFS.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
