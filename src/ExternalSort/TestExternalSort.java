package ExternalSort;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

/*
 * Note** the partition will be a round robin type of partition to 
 * get each segment to have relatively equal number of work
 * 
 */

public class TestExternalSort {

	public static void main( String[] args ) throws FileNotFoundException{
		File file = null;
		PropertyFileHandler propHandler = new PropertyFileHandler();
		//readProperties could throw an IO Exception 
		propHandler.readProperties(); //read the file, we will still need to retrieve the key values 
		
		int inputSize = propHandler.getDataSize(); 
		int memorySize = propHandler.getMemorySize(); //the limit of each array we can create
		int dataPerLine = propHandler.getDataPerLine(); //
		String type = propHandler.getDataType();
		String resourceDirectory = System.getProperty("user.dir")+File.separator+"resources"+File.separator;
		String Filename = resourceDirectory + "Random"+inputSize+".txt";
		/*the output file has not been created yet, even if it has been created, 
		 * we are over writing it, so it would not make a difference if it exist or not at the moment*/
		String outputFileName = resourceDirectory + "Random"+inputSize+"Result.txt";
		
		if(FileHandler.fileExist(Filename)){
			file = new File(Filename);
			FilePartitionHandler partitioner = new FilePartitionHandler(file, resourceDirectory);
			//let's read this file into segments
			File[] partitionArr = null;
			try {
				partitionArr = partitioner.roundRobinPartition(inputSize, memorySize, dataPerLine);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			ExternalSort.parallelSort(partitionArr, type);
			ExternalSort.parallelMerge(partitionArr, type, memorySize, outputFileName); 
			
		}else{ //if the file doesn't exist, return it
			return;
		}
		
	}

}
