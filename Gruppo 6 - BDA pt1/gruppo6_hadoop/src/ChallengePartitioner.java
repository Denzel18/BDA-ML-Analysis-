import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class ChallengePartitioner extends Partitioner<Text , FloatWritable> {

	public int getPartition(Text key, FloatWritable review, int numReduceTasks) {
		
		try{		
			LocalDate date = LocalDate.parse(key.toString().split(";")[1]);
	
			switch(date.getMonthValue()) { 
				case 1:
					return 0;
				case 2:
					return 1;
				case 3:
					return 2;
				case 4:
					return 3;
				case 5:
					return 4;
				case 6:
					return 5;
				case 7:
					return 6;
				case 8:
					return 7;	
				case 9:
					return 8;		
				case 10:
					return 9;
				case 11:
					return 10;
				case 12:
					return 11;
				default:
					return 0;
			}
		}catch(DateTimeParseException e){
			System.out.println("ERRORE PARTIONER"); 
			return 0; 
		}
	}
}
