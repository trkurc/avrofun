package awesome.tony;

import java.io.File;
import java.util.ArrayList;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

public class AvroFun {
	static Random RANDOM = new Random(5);
	static int COUNT = 0;
	public static void main(String args[]){
		try{

			Schema schema = new Parser().parse(new File("schema.avro"));
			System.out.println(schema);
			File file = new File("users.avro");
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
			dataFileWriter.create(schema, file);

			for(int i=0; i<100; i++){

				GenericData.Record a = new GenericData.Record(schema);
				if(COUNT++ != 30){
					a.put("name", randomString(RANDOM.nextInt(6<<10)));
				}
				else{
					a.put("name", randomString(146<<20));
				}
							
				dataFileWriter.append(a);
			}
			dataFileWriter.close();


		}
		catch(Exception e){
			e.printStackTrace();
		}
	}


	public static void fillGarbage(GenericData.Record r){
		ArrayList x = new ArrayList();
		switch(RANDOM.nextInt(3)){
		case 0:
			// string
			x.add(new Long(RANDOM.nextLong()));
			break;
		case 1:
			if(COUNT++ != 3000){
				x.add(randomString(RANDOM.nextInt(6<<10)));
			}
			else{
				x.add(randomString(146<<20));
			}
			break;
		case 2:	
			GenericData.Record a = new GenericData.Record(r.getSchema());
			fillGarbage(a);
			x.add(a);
			break;	
		}	
		r.put("values", x);
	}

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";


	static String randomString( int len ) 
	{
		StringBuilder sb = new StringBuilder( len );
		for( int i = 0; i < len; i++ ) 
			sb.append( AB.charAt( RANDOM.nextInt(AB.length()) ) );
		return sb.toString();
	}
}
