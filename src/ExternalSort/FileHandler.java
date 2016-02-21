package ExternalSort;

import java.io.File;
import java.io.IOException;

public class FileHandler {
	
	public FileHandler(){
	}
	
	//we need a function to check if a directory exist
	public static boolean directoryExist(String directoryPath){
		File file = new File(directoryPath);
		if(file.exists() && file.isDirectory()){
			return true;
		}else{
			return false;
		}
	}
	
	//create a directory
	public static void createDirectory(String directoryPath){
		File file = new File(directoryPath);
		if(!file.exists()){ //
			file.mkdir();
		}//don't make the directory if it already exist
	}
	
	public static boolean fileExist(String filePath){
		File file = new File(filePath);
		if(file.exists() && file.isFile()){
			return true;
		}else{
			return false;
		}
	}
	
	//create a file and return its handle
	public static File createFile(String filePath) throws IOException{
		File file = new File(filePath);
		boolean isFile = file.isFile();
		
		if(!file.exists() || !isFile){ //if does not exist yet and already a directory
			if(file.createNewFile()){
				return file;
			} else {
				throw new IOException("fail to create file");
			}
		}else{ //is an existing file and is also a file structure
			return file;
		}
	}
	
	public static File getFile(String filePath) throws IOException{
		File file = new File(filePath);
		if(!file.exists()){
				throw new IOException("no such file");	
		}else{
			return file;
		}
	}
	
}
