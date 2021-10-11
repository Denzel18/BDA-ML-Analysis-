import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ChallengeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> { 
	
	public HashMap<String, Float> sentiWords = new HashMap<String,Float>();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{		
		String text=value.toString();
		String [] reviews = text.split("\t");		
		if (reviews.length > 0) {
			if(reviews[0].equals("marketplace")){
				FileSystem fs = FileSystem.get(new Configuration());
				Path file = new Path("/output6SENTI/part-r-00000");
        			BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
				String line = "";
				while((line = reader.readLine()) != null){
					String [] extractedLineParts = line.split("\t");
					this.sentiWords.put(extractedLineParts[0], Float.parseFloat(extractedLineParts[1]));
				}
				reader.close();
			}else if (!reviews[12].equals("") && !reviews[13].equals("")) { 
				//ignoro le stringhe vuote  
				String line = reviews[12].toLowerCase()+" "+reviews[13].toLowerCase();   
				StringTokenizer parole = new StringTokenizer(line, " .,?!;:()[]{}'");
				while(parole.hasMoreTokens()) {
					String parola = parole.nextToken().toLowerCase().trim();
					if(!parola.equals(" ") && this.sentiWords.get(parola)!=null){
						float votazione = this.sentiWords.get(parola);
						context.write(new Text(reviews[2]+";"+reviews[14]),new FloatWritable(votazione));
					}
				}
			}
		}
	}

}



