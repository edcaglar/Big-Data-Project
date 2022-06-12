import java.util.*;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException; 
import java.io.IOException; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*;



public class AveragePrice {
  //Mapper class 15 marka 18 price idx
	public static class AveragePriceMapper extends Mapper <LongWritable, Text, Text, FloatWritable> {
		private FloatWritable price = new FloatWritable();
		private Text text = new Text();
		private int hata_sayisi=0;

		public void map(LongWritable key, Text value, Context context) throws IOException {

			String valueString = value.toString();
			String[] carData = valueString.split(",");
			
			if (carData != null){
			  text.set(carData[15]);
			  try {
				  price.set(Float.parseFloat(carData[18]));
			  }
			  catch(Exception e){
				  hata_sayisi++;
			  }
			}
			//System.out.println(hata_sayisi);

			try{
			  context.write(text,price);
			}
			catch(Exception e){
 			  e.printStackTrace();
			}
		}
	}


	   //Reducer class 
	public static class AveragePriceReducer extends Reducer<Text, FloatWritable, Text, Text> {
		private FloatWritable result = new FloatWritable();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException {
			try {
				int count = 0;
				float sum = (float)0;
				for(FloatWritable val : values){
					sum += val.get();
					count+=1;
				}
				float average = (float) sum/count;
				result.set(average);

				//context.write(key, new Text(String.format("%f", result.get())));
				context.write(key,new Text(String.format("%f", result.get())));

			}
			catch(Exception e){
			  e.printStackTrace();	
			}
		}
	}
 //Main function 
  public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();
	  Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Price");
    job.setJarByClass(AveragePrice.class);
    job.setMapperClass(AveragePriceMapper.class);
    job.setReducerClass(AveragePriceReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }


	    
}
