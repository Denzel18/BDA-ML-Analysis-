import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChallengeSentiReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	//i primi 2 parametri sono la coppia key-(list of values) )
	protected void reduce(Text key, Iterable<FloatWritable> votazioni, Context context) throws IOException, InterruptedException { 
		float avg=0;
		int i=0;		
		for (FloatWritable votazione: votazioni) {
			avg += votazione.get();
			i++;
		}
		avg = avg/i;
		context.write(key, new FloatWritable(avg));	
	}
}
