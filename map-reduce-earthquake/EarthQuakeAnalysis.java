import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class EarthQuakeAnalysis {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)	throws IOException {

		   // split line into tokens
		   String[] tokens = value.toString().split(",", 12);

		   if (tokens.length != 12) {
		       System.out.println("- " + tokens.length);
		       return;
		   }

		   // name of the region
		   String k = tokens[11];

		   // magnitude of the earthquake
		   double v = Double.parseDouble(tokens[8]);

		   // collect region and magnitude of earthquake
		   output.collect(new Text(k), new DoubleWritable(v));

	       }
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterator<DoubleWritable> values,	OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

		   // calculate max for each region
        	   double maxMagnitude = Double.MIN_VALUE;

		   while (values.hasNext()) {
            	       maxMagnitude = Math.max(maxMagnitude, values.next().get());
		   }

		   // collect output
        	   output.collect(key, new DoubleWritable(maxMagnitude));

		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(EarthQuakeAnalysis.class);
		conf.setJobName("EarthQuakeAnalysis");

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(DoubleWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		 
		JobClient.runJob(conf);
	}
}
