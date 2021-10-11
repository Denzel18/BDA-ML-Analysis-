import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChallengeDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode =ToolRunner.run(new ChallengeDriver(), args);
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {

		//indirizzo sentiword: hdfs://192.168.104.45:9000/sentiwordnet.txt
		String inputDir = "/sentiwordnet.txt"; // path dell'input directory in HDFS SentiWords di default
		String outputDir = "/output6SENTI"; // path dell'output directory in HDFS SentiWords di default
		
		if (args.length > 0) {
			inputDir = args[0]; // path dell'input directory in HDFS SentiWords
			//outputDir = args[1]; // path dell'output directory in HDFS SentiWords se modificato da errore
		}

		//configurazione alto livello 
		Configuration config = new Configuration();
		Job jobSenti = Job.getInstance(config, "Job Name: Challenge SENTI");
	
		jobSenti.setJarByClass(ChallengeDriver.class);		
		jobSenti.setMapperClass(ChallengeSentiMapper.class);
		jobSenti.setReducerClass(ChallengeSentiReducer.class);

		jobSenti.setOutputKeyClass(Text.class); 
		jobSenti.setOutputValueClass(FloatWritable.class);

		//visto prima...
		FileInputFormat.addInputPath(jobSenti, new Path(inputDir));
		FileOutputFormat.setOutputPath(jobSenti, new Path(outputDir));
		
		boolean success = jobSenti.waitForCompletion(true);
		if (!success) {
			throw new IllegalStateException("Job Senti Challenge failed!");		
		}
		
		//indirizzo recensioni: hdfs://192.168.104.45:9000/test.tsv
		inputDir = "/test.tsv"; // path dell'input directory in HDFS Recensioni di default
		outputDir = "/output6"; // path dell'output directory in HDFS Recensioni di default
		if (args.length > 2) {
			inputDir = args[1]; // path dell'input directory in HDFS Recensioni
			outputDir = args[2]; // path dell'output directory in HDFS Recensioni
		}

		config = new Configuration();
		Job jobReview = Job.getInstance(config, "Job Name Review : Challenge");
		jobReview.setJarByClass(ChallengeDriver.class); 
	
		jobReview.setMapperClass(ChallengeMapper.class);
		jobReview.setReducerClass(ChallengeReducer.class);
		jobReview.setPartitionerClass(ChallengePartitioner.class);	
		jobReview.setNumReduceTasks(12); //1 per ogni mese
			
		jobReview.setOutputKeyClass(Text.class);
		jobReview.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(jobReview, new Path(inputDir));
		FileOutputFormat.setOutputPath(jobReview, new Path(outputDir));
		boolean successReview = jobReview.waitForCompletion(true);
		if (!successReview) {
			throw new IllegalStateException("Job Review Challenge failed!");	
		}
		return 0;
	}

}
