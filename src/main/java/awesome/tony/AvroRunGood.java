package awesome.tony;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;

public class AvroRunGood {
	static Random RANDOM = new Random(5);
	
	public static void main(String args[]){
		try{
			Parser p = new Parser();
			
			Schema schema = new Parser().parse(new File("good_schema.avro"));
		
			InputStream in = new FileInputStream("users.avro");
			GenericDatumReader dr = new GenericDatumReader(schema);
			
			int count = 0;
			DataFileStream df = new DataFileStream(in, dr);

			long pre = System.currentTimeMillis();

			while(df.hasNext()){
				Object o = df.next();
				if(++count % 10 == 0){
					System.out.println("Elapsed: "+  (System.currentTimeMillis() - pre));
					
					pre = System.currentTimeMillis();
				}
			}
			
			
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}