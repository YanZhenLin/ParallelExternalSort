package ExternalSort;

import java.io.FileWriter;
import java.io.IOException;
import java.io.FileInputStream;
import java.util.Properties;

public class PropertyFileHandler {
	
	Properties prop = new Properties();
	FileInputStream input = null;
	int dataSize = 0, maximumMemory = 5000, dataPerLine=20; //defaults
	String dataType = "Double"; //default to double
	
	public int getDataSize(){
		return dataSize;
	}
	
	public int getMemorySize(){
		return maximumMemory;
	}
	
	public String getDataType(){
		return dataType;
	}
	
	public int getDataPerLine(){
		return dataPerLine;
	}
	
	public void readProperties(){
		try {
			input = new FileInputStream("config.properties");
			// load a properties file
			prop.load(input);
			// get the property value and print it out
			dataSize = Integer.parseInt(prop.getProperty("DataSize"));
			maximumMemory = Integer.parseInt(prop.getProperty("instanceMemorySize"));
			dataType = prop.getProperty("dataType");
			dataPerLine = Integer.parseInt(prop.getProperty("dataPerLine"));
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public void InitializePropertyFile(){	
		// set the properties value
		prop.setProperty("DataSize", "5000");
		prop.setProperty("instanceMemorySize", "1000");
		prop.setProperty("dataType", "Double");

		FileWriter writer = null;
		try{
			writer = new FileWriter("conf.properties");
			prop.store(writer,"Author: Yan");
		}catch(IOException ex){
			ex.printStackTrace();
		}finally{
			if(writer != null){
				try{
					writer.close();
				}catch(IOException ex){
					ex.printStackTrace();
				}
			}
		}
	}
	
}
