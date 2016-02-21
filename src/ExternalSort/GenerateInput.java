package ExternalSort;

import java.io.IOException;
import java.util.Scanner;
import java.io.File;

//all this does is just generate the random input file
public class GenerateInput {
	public static void main( String[] args) throws IOException{
		File file = null;
		PropertyFileHandler propHandler = new PropertyFileHandler();
		//readProperties could throw an IO Exception 
		propHandler.readProperties(); //read the file, we will still need to retrieve the key values 
		
		int inputSize = propHandler.getDataSize(); 
		int memorySize = propHandler.getMemorySize();
		String type = propHandler.getDataType();
		
		//need a a file handler to check if the directory exist
		FileHandler fileHandler = new FileHandler();
		
		String workingDirectory = System.getProperty("user.dir");
		
		String resourceDirectory = workingDirectory+File.separator+"resources";
		//System.out.println(resourceDirectory);
		
		boolean directoryExist = FileHandler.directoryExist(resourceDirectory);
		if(!directoryExist){
			FileHandler.createDirectory(resourceDirectory);
		}
		//we will have a random generator do this part
		String fullPath = resourceDirectory+File.separator+"Random"+inputSize+".txt";
		
		//the createFile method will return the file handle if its already created
		file = fileHandler.createFile(fullPath);
		
		if(file == null)
			throw new IOException("File could not be accessed");
		GeneratorControl.run(inputSize, memorySize, type, file);
		
	}
}
