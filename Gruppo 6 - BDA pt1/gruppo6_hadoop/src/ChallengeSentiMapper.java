import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//<....> i primi 2 data types si riferiscono alla coppia key-value in input, gli altri 2 alla coppia in output
//id_riga , text , int , float 
public class ChallengeSentiMapper extends Mapper<LongWritable, Text, Text, FloatWritable> { 

	//context variabile globale che usiamo per scrivere su file, ha uno scope globale ...
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//1 Step - converto il nostro value in text, perché dobbiamo lavorare con String Tokenizer 		
		String text=value.toString(); //converto in String perchè non lavoriamo con i Text
		if(text.charAt(0) != 35) {
			//Tokenizzo - Text che ho convertito da value e la punteggiatura che su cui voglio tokenizzare Salvo su works 
			StringTokenizer sentiWords = new StringTokenizer(text, "\t");
			//finché ci sono parole (TOKEN) ....
			String votazione = "";
			String test = ""; 
			while (sentiWords.hasMoreTokens()) {
				votazione += sentiWords.nextToken() + ";";
			}
			String [] campi = votazione.split(";");		
			if(campi.length > 5){
				Float somma_voti = Float.parseFloat(campi[2]) - Float.parseFloat(campi[3]); 
				if(somma_voti != 0 ){
					String [] parole_riga = campi[4].split(" "); 
					for(int z = 0; z < parole_riga.length ; z++) {
						test = parole_riga[z].split("#")[0].toLowerCase(); 
						context.write(new Text(test),new FloatWritable(somma_voti)); 
					}  
				}		
			}
    		}
	}				
}
