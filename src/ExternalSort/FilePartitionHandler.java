package ExternalSort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

//we expect two passing arguments, one is the input file, the other one being the out put file 
/*
 * Important note, we will implement a round robin distribution, assuming that the server
 * performing the distribution will have ample memory to load the entire big data file
 * We will also implement a non round robin algorithm for the case that the distribution server 
 * does not have ample memory either
 * We always assume that computing servers will not have enough memory
 * 
 */
public class FilePartitionHandler {

	File file = null;
	String outputPath = "";
	
	public FilePartitionHandler(File originalFile, String outputDirPath){
		file = originalFile;
		outputPath = outputDirPath;
	}
	
	//using a round robin partition, we assume the distribution server has enough memory
	public File[] roundRobinPartition( int dataSize, int maxMemory, int dataPerLine ) throws IOException{
		//we need a divisor method that will not generate odd number of partitions
		Runtime runtime = Runtime.getRuntime();
		int numberOfProcessors = runtime.availableProcessors(); 
		//we want the job to be split into factors of the number of available processors for maximum parallelism
		int divisor = getDivisor(dataSize, maxMemory, numberOfProcessors);
		System.out.println("number of partitions:"+divisor+" for number of processors"+numberOfProcessors );
		int i = 0;
		File[] partitionFiles = new File[divisor];
		for(i = 0; i < divisor; i++){
			String fileName = outputPath+file.separator+"Random"+dataSize+"Segment"+i+".txt";
			partitionFiles[i] = new File(fileName); 
		}
		
		StringBuffer[] buffers = new StringBuffer[divisor];
		//we will need to initialize each buffer cell with actual buffers
		for(i = 0; i < divisor; i++ ){
			buffers[i] = new StringBuffer("");
		}
		//we actually don't need the line Number here, since we are implementing a round robin, we skip every 
		//we want as little IO as possible, so we will save everything to an array of string buffers, update the string buffer as we go 
		
		Scanner input = new Scanner(file); //may throw a FileNotFoundException
		String line = "";
		int lineCounter = 0;
		
		int whichCounter = 0;
		while(input.hasNext()){
			line = input.nextLine();
			
			//int mod = lineCounter%divisor; //its inefficient to do this, let's think of a different way
			buffers[whichCounter].append(line+"\n");
			lineCounter++;
			whichCounter++;//zero will never get any
			if(whichCounter == divisor) //reset condition
				whichCounter = 0;
		}
		input.close(); //close the scanner
		
		for(i = 0; i < divisor; i++){
			FileWriter writer = new FileWriter(partitionFiles[i]);
			writer.write( buffers[i].toString());
			writer.close();
		}
		return partitionFiles;
	}
	
	//we should let the multiplier be base 2 
	public int getDivisor(int dataSize, int maxMemory, int numberOfProcessors){
		//we need the number with the smallest number of divisor such that the most number
		int coefficient = 0;
		int cover = (int) (maxMemory*numberOfProcessors* Math.pow(2, coefficient));
		
		while( cover < dataSize ){
			coefficient++;
			cover = (int) (maxMemory*numberOfProcessors* Math.pow(2, coefficient));
		}
		return (int)(numberOfProcessors* Math.pow(2, coefficient));
	}
	
}
