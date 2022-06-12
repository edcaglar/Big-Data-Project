import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class StandardDeviation {


    public static class StandardDeviationMapper extends Mapper<LongWritable, Text, Text,FloatWritable>{
        private FloatWritable price = new FloatWritable();
        private Text brand = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            int n=0;
            String valueString = value.toString();
            String[] lines = valueString.split(",");


            if(lines!= null){
                brand.set(lines[15]);
                try{
                    price.set(Float.parseFloat(lines[18]));
                }
                catch (Exception e){
                    n++;
                }
            }
            try{
                context.write(brand, price);
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

        public static class StandardDeviationReducer extends Reducer<Text,FloatWritable,Text, Text>{
        private FloatWritable result = new FloatWritable();
        public void reduce(Text key,Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException{
            try{
            float sum = 0;
            int n = 0;
            ArrayList<Float> valueList = new ArrayList<>();
            for (FloatWritable val: values){
                valueList.add(val.get());
                sum += val.get();
                n++;
            }
            Float avg = (float) sum/n;
            Float std = 0f, diff = 0f;
            for(Float val: valueList){
                diff += (float) Math.pow(avg - val, 2);
            }
            std = (float) Math.sqrt(diff / n);
            result.set(std);
            context.write(key,new Text(String.format("%f", result.get())));
            }
            catch (Exception e){
                e.printStackTrace();
                }
            }
        }

    public static void main(String[] args) throws  Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"standardDeviation");
        job.setJarByClass(StandardDeviation.class);
        job.setMapperClass(StandardDeviationMapper.class);
        job.setReducerClass(StandardDeviationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
