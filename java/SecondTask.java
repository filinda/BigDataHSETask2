import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondTask {
	
		
	public static class MinuteMapper extends Mapper<Object, Text, Text, Text> {

		
		private Text keyT = new Text();
		private Text valueT = new Text();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(",");
			try{
				FileSplit fileSplit = (FileSplit)context.getInputSplit();
				//System.out.println("ok");
				String filename = fileSplit.getPath().getName();
				//System.out.println(filename);
				String h = filename.split("\\.")[1];
				String u = filename.split("\\.")[2];
				int minute = ((int)Double.parseDouble(tokens[1]))/60 + 1440*(Integer.parseInt(tokens[0]));
				keyT.set(h+"|"+u+"|"+minute+"|");
				valueT.set(tokens[2]+"|"+"1");
				context.write(keyT, valueT);
			}catch(Exception e){
				e.printStackTrace();
				keyT.set("0|0|0|");
				valueT.set("0.0"+"|"+"1");
				context.write(keyT, valueT);
			}
		}
	}

	public static class SummMinuteReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			int amount = 0;
			for (Text val : values) {
				System.out.println(val.toString());
				String[] valArr = val.toString().split("\\|");
				System.out.println(valArr[0]);
				System.out.println(valArr[1]);
				Double curval = Double.parseDouble(valArr[0]);
				int cur_amount = Integer.parseInt(valArr[1]);
				sum += curval;
				amount += cur_amount;
			}
			result.set(sum+"|"+amount);
			context.write(key, result);
		}
	}

	
	public static class DayMapper extends Mapper<Object, Text, Text, Text> {

		
		private Text keyT = new Text();
		private Text valueT = new Text();

		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\|");
			try{
				String h = tokens[0];
				int day = Integer.parseInt(tokens[2])/60/24;
				keyT.set(h+"|"+day);
				Double avg = Double.parseDouble(tokens[3])/Integer.parseInt(tokens[4]);
				valueT.set(avg+"|1");
				context.write(keyT, valueT);
			}catch(Exception e){
				e.printStackTrace();
				keyT.set("0|0|");
				valueT.set("0.0"+"|"+"1");
				context.write(keyT, valueT);
			}
		}
	}
	
	public static class SummDayReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Double sum = 0d;
			int amount = 0;
			for (Text val : values) {
				System.out.println(val.toString());
				String[] valArr = val.toString().split("\\|");
				System.out.println(valArr[0]);
				System.out.println(valArr[1]);
				Double curval = Double.parseDouble(valArr[0]);
				int cur_amount = Integer.parseInt(valArr[1]);
				sum += curval;
				amount += cur_amount;
			}
			result.set(sum+"|"+amount);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "avg minute speed");
		job.setJarByClass(SecondTask.class);
		job.setMapperClass(MinuteMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(SummMinuteReducer.class);
		job.setReducerClass(SummMinuteReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "avg day speed");
		job2.setJarByClass(SecondTask.class);
		job2.setMapperClass(DayMapper.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setCombinerClass(SummDayReducer.class);
		job2.setReducerClass(SummDayReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}