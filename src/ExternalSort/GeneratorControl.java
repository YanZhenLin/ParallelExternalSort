package ExternalSort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class GeneratorControl {
	public static final int MAXRAND = 10000;
	public static void run( int DataSize, int maxMemorySize, String dataType, File file) throws IOException{
		
		//System.out.println("data type:"+dataType);
		Generator gen = null;
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(file, true))); //may throw an IO exception here
		
		if( DataSize > maxMemorySize ){
			if(dataType.equals("Double")){
				gen = new DoubleGenerator( MAXRAND );
			}else if(dataType.equals("Integer")){
				gen = new IntegerGenerator( MAXRAND );
			}
			int runCounter = 1; //total run counter
			int currentCounter = 0;//this counter reached the maxmemorySize and halts
			int numberOfPortions = DataSize/maxMemorySize; 
			
			StringBuilder builder = new StringBuilder("");
			while( runCounter <= DataSize ){
				 //reset this guy to zero
				Object[] result = gen.generate(maxMemorySize);
				for(currentCounter = 0; currentCounter < maxMemorySize && runCounter <= DataSize; currentCounter++){
					if( runCounter%20 == 0 ){
						builder.append(result[currentCounter] + "\n");
					}else{
						builder.append(result[currentCounter] + " ");
					}
					runCounter++;
				}
			}
			System.out.println(builder.toString()); //the standard output is actually correct
			out.print(builder.toString()); //something is wrong with the print method
			out.close();
		}else{ //else we just compute one segment and write the result in
			if(dataType.equals("Double")){
				gen = new DoubleGenerator( MAXRAND );
			}else if(dataType.equals("Integer")){
				gen = new IntegerGenerator( MAXRAND );
			}
			Object[] result = gen.generate(DataSize);
			StringBuilder builder = new StringBuilder("");
			int j = 1;
			for(int i = 0; i < DataSize; i++){
				if(j%20 == 0 ){
					builder.append(result[i] + "\n");
				}else{
					builder.append(result[i] + " ");
				}
				j++;
			}
			out.print(builder.toString());
			out.close();
		}
	}
}
