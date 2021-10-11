import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChallengeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	
	protected void reduce(Text key, Iterable<FloatWritable> words, Context context) throws IOException, InterruptedException { 
		float sum=0;
		int i=0;
		for (FloatWritable word: words) {
			sum += word.get();
			i++;
		}
		float avg = (float) sum/i;
		float rounded = (float) (Math.round(avg *100.0)/100.0);
		if(rounded > 0){
			context.write(new Text(key.toString().split(";")[0]), new FloatWritable(rounded));
		}
	}
	

}




