package ExternalSort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.RecursiveAction;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.concurrent.ForkJoinPool;
/*
 * goal of this project is load a large set of data from file which in theory is too 
 * large to be held in memory had must require that only portions of the data be 
 * loaded into memory at a time. A ForkJoin framework will be used to run the process 
 * in parallel. Only the merge sort we will perform in parallel, the segment merging 
 * will be done sequentially.
 * Note that the memory here is completely hypothetical. The actual maximum memory in actual
 * environment must be at least twice as large as the passed in value here, since the merge 
 * sort itself will require a copy storage of the iteration. 
 * 
 */

public class ExternalSort {	
	
	public static void parallelMerge( File[] partitionArr, String type, int maxMemory, String outputFileName ){
		RecursiveAction mainTask = new MergeTask(partitionArr, type, maxMemory, outputFileName, 0); 
		ForkJoinPool pool = new ForkJoinPool(); //create the pool with all available processors
		pool.invoke(mainTask);
	}
	
	//we will recursively call this layer, we merge the files until there's only two files left
	private static class MergeTask extends RecursiveAction{
		private File[] partitionArray;
		private String dataType;
		private String outputFileName;
		private int sizeOfPartition;
		private int level;
		private int memorySize;
		private int partitionCounter;
		private String outputDirectory;
		
		MergeTask( File[] partitionArr, String dataType, int memorySize, String outputFileName, int level ){
			this.partitionArray = partitionArr;
			this.dataType = dataType;
			this.outputFileName = outputFileName;
			this.sizeOfPartition = partitionArr.length;
			this.level = level;
			this.memorySize = memorySize;
			this.partitionCounter = 0;
			this.outputDirectory = outputFileName.substring(0, (outputFileName.lastIndexOf(File.separator)+1)); 
			System.out.println("validating the output directory:"+this.outputDirectory);
		}
		
		@Override
		public void compute(){
			//we will need to iteratively merge pairs of files into one, we can do this in parallel, we save the result of this to a list 
			
			//if the list size is greater than 1, recursively call the recursive Task over again
			System.out.println("Size of partition:"+sizeOfPartition);
			
			if( sizeOfPartition == 1){
				return;
			}else if( sizeOfPartition == 2 ){ //if two, merge it into the result file
				File file1 = partitionArray[0];
				File file2 = partitionArray[1];
				
				//the file outputFileName is just the name of the target file, we will create this file if not created already
				try{
					System.out.println("generate output file:"+outputFileName);
					File targetFile = FileHandler.createFile(outputFileName);
					//we will review this later
					mergeTwo(file1, file2, targetFile, dataType, memorySize); //if only two files, we don't need to divide the task up to more processors
				}catch(Exception ex){
					System.out.println(ex);
				}
			}else{ //more than two files
				//the first part 
				ArrayList<FileTuple> TupleArray = new ArrayList<FileTuple>();
				
				int leftOverCount = sizeOfPartition;
				int currentPosition = 0;
				while( leftOverCount > 0 ){ //always expecting even number of partitions
					if( leftOverCount%2 == 0 ){ 
						File f1 = partitionArray[currentPosition];
						File f2 = partitionArray[currentPosition+1];
						
						System.out.println(f1.getAbsolutePath());
						System.out.println(f2.getAbsolutePath());
						FileTuple newTuple = new FileTuple(f1, f2); 
						TupleArray.add(newTuple);
						currentPosition += 2;
						leftOverCount -= 2;
					}else{
						throw new RuntimeException("Partition error");
					}
				}
				
				File[] subPartitionArray = new File[TupleArray.size()];
				RecursiveAction[] individualMergeTasks = new RecursiveAction[TupleArray.size()];
				//incremental merge step
				for(int i = 0; i < TupleArray.size(); i++){
					ArrayList<File> tupleList = TupleArray.get(i).getList();
					String targetFileName = generateFile();
					File targetFile = null;
					try {
						System.out.println("generating targetFileName:"+targetFileName);
						targetFile = FileHandler.createFile(targetFileName);
					} catch (IOException e) {
						e.printStackTrace();
					}
					individualMergeTasks[i] = new IndividualMergeTask(tupleList.get(0), tupleList.get(1), targetFile, dataType, memorySize);
					subPartitionArray[i] = targetFile; //add the merged result to the subPartition
				}
				
				for(int i = 0; i < TupleArray.size(); i++){
					individualMergeTasks[i].fork();
				}
				
				for(int i = 0; i < TupleArray.size(); i++){
					individualMergeTasks[i].join();
				}
				
				//now we need to recursively call
				RecursiveAction nextTask = new MergeTask(subPartitionArray, dataType, memorySize, outputFileName, level+1);
				nextTask.fork();
				nextTask.join();
			}
		}
		
		//using the level variable and the partitionCounter to generate a unique file
		private String generateFile(){
			//using a combination of the outputDirectory, level value and the partitionCounter to form a filename
			String truncateStr = outputFileName.replaceAll(outputDirectory, "");//to get just the file name
			String ret = outputDirectory+truncateStr.replaceAll(".txt", "")+"_L"+level+"_P"+partitionCounter+".txt";
			partitionCounter++;
			return ret;
		}
		
		private static class FileTuple{
			private ArrayList<File> list;
			private int size;
			public FileTuple( File file1, File file2){
				list = new ArrayList<File>();
				list.add(file1);
				list.add(file2);
				this.size = 2;
			}
			
			public ArrayList<File> getList(){
				return list;
			}
			
			public int getSize(){
				return size;
			}
		}
		
	}
	
	
	//handles the control of the data process
	public static void mergeTwo(File file1, File file2, File TargetFile, String dataType, int maxMemory) 
			throws IOException{
		int segmentSize = maxMemory/2;
		
		Scanner input1 = new Scanner(file1);
		Scanner input2 = new Scanner(file2);
		
		boolean allFinished = false;
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(TargetFile)));
		int currentStreamCounter1 = 0;
		int currentStreamCounter2 = 0;
		
		Number[] list1 = new Number[segmentSize];
		Number[] list2 = new Number[segmentSize];
		
		int nextStoppingPoint = currentStreamCounter1+segmentSize;
		
		while( !allFinished ){
			int StreamSize1 = 0;
			int StreamSize2 = 0;
			
			int currentIndex1 = 0; 
			int currentIndex2 = 0;
			Number[] portionResult = null;
			StringBuffer buffer = new StringBuffer(""); //reset buffer on every
			//we need to add the stream into the array for processing
			if( dataType.equals("Double") ){
				while( input1.hasNext() && currentStreamCounter1 < nextStoppingPoint ){
					list1[currentIndex1++] = input1.nextDouble();//this is erroroneous
					currentStreamCounter1++;
					StreamSize1++;
				}
				
				while( input2.hasNext() && currentStreamCounter2 < nextStoppingPoint ){
					list2[currentIndex2++] = input2.nextDouble();
					currentStreamCounter2++;
					StreamSize2++;
				}
				
				portionResult = new Double[StreamSize1+StreamSize2];
				Double[] properArray1 = new Double[StreamSize1];
				System.arraycopy(list1, 0, properArray1, 0, StreamSize1);
				Double[] properArray2 = new Double[StreamSize2];
				System.arraycopy(list2, 0, properArray2, 0, StreamSize2);
				portionResult = ArrayMergeSort.merge(properArray1, properArray2, (Double[])portionResult);	
				
			}else if( dataType.equals("Integer") ){
				while( input1.hasNext() && currentStreamCounter1 < nextStoppingPoint ){
					list1[currentIndex1++] = input1.nextInt();
					currentStreamCounter1++;
					StreamSize1++;
				}
				
				while( input2.hasNext() && currentStreamCounter2 < nextStoppingPoint ){
					list2[currentIndex2++] = input2.nextInt();
					currentStreamCounter2++;
					StreamSize2++;
				}
				
				portionResult = new Integer[StreamSize1+StreamSize2];
				Integer[] properArray1 = new Integer[StreamSize1];
				System.arraycopy(list1, 0, properArray1, 0, StreamSize1);
				Integer[] properArray2 = new Integer[StreamSize2];
				System.arraycopy(list2, 0, properArray2, 0, StreamSize2);
				portionResult = (Integer[])ArrayMergeSort.merge(properArray1, properArray2, (Integer[])portionResult);
			}
			
			int j = 1;
			int portionSize = portionResult.length;
			for(int i = 0; i < portionSize; i++){
				if(i == portionSize-1){
					buffer.append(portionResult[i] + "\n");
				}else if(j%20 == 0 ){
					buffer.append(portionResult[i] + "\n");
				}else{
					buffer.append(portionResult[i] + " ");
				}
				j++;
			}
			writer.write(buffer.toString());
			
			//update the next stopping point
			nextStoppingPoint = nextStoppingPoint+segmentSize;
			
			if(!input1.hasNext() && !input2.hasNext() ){
				allFinished = true;
			}
		}
		input1.close();
		input2.close();
		writer.close();
	}
	
	//deprecated, and not in use
	//handles the control of the data process
	private static void mergeThree(File file1, File file2, File file3, File TargetFile, String dataType, int maxMemory ) 
			throws IOException{
		int segmentSize = maxMemory/3;
		//the key is to load each segment into an limited array, merge these arrays and then come back 
		boolean allFinished = false;
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(TargetFile)));
		int currentStreamCounter1 = 0;
		int currentStreamCounter2 = 0;
		int currentStreamCounter3 = 0;
		
		Number[] list1 = new Number[segmentSize];
		Number[] list2 = new Number[segmentSize];
		Number[] list3 = new Number[segmentSize];
		
		Scanner input1 = new Scanner(file1);
		Scanner input2 = new Scanner(file2);
		Scanner input3 = new Scanner(file3);
		
		int nextStoppingPoint = currentStreamCounter1+segmentSize;
		
		while( !allFinished ){
			int StreamSize1 = 0;
			int StreamSize2 = 0;
			int StreamSize3 = 0;
			
			Number[] portionResult = null;
			StringBuffer buffer = new StringBuffer(""); //reset buffer on every
			//we need to add the stream into the array for processing
			if( dataType.equals("Double") ){
				while( input1.hasNext() && currentStreamCounter1 < nextStoppingPoint ){
					list1[currentStreamCounter1++] = input1.nextDouble();
					StreamSize1++;
				}
				
				while( input2.hasNext() && currentStreamCounter2 < nextStoppingPoint ){
					list2[currentStreamCounter2++] = input2.nextDouble();
					StreamSize2++;
				}
				
				while( input3.hasNext() && currentStreamCounter3 < nextStoppingPoint){
					list3[currentStreamCounter3++] = input3.nextDouble();
					StreamSize3++;
				}
				
				Double[] properArray1 = new Double[StreamSize1];
				System.arraycopy(list1, 0, properArray1, 0, StreamSize1);
				Double[] properArray2 = new Double[StreamSize2];
				System.arraycopy(list2, 0, properArray2, 0, StreamSize2);
				Double[] properArray3 = new Double[StreamSize3];
				System.arraycopy(list3, 0, properArray3, 0, StreamSize3);
				portionResult = ArrayMergeSort.merge(properArray1, properArray2, properArray3);	
				
			}else if( dataType.equals("Integer") ){
				while( input1.hasNext() && currentStreamCounter1 < nextStoppingPoint ){
					list1[currentStreamCounter1++] = input1.nextInt();
					StreamSize1++;
				}
				
				while( input2.hasNext() && currentStreamCounter2 < nextStoppingPoint ){
					list2[currentStreamCounter2++] = input2.nextInt();
					StreamSize2++;
				}
				
				while( input3.hasNext() && currentStreamCounter3 < nextStoppingPoint){
					list3[currentStreamCounter3++] = input3.nextInt();
					StreamSize3++;
				}
				
				Integer[] properArray1 = new Integer[StreamSize1];
				System.arraycopy(list1, 0, properArray1, 0, StreamSize1);
				Integer[] properArray2 = new Integer[StreamSize2];
				System.arraycopy(list2, 0, properArray2, 0, StreamSize2);
				Integer[] properArray3 = new Integer[StreamSize3];
				System.arraycopy(list3, 0, properArray3, 0, StreamSize3);
				
				portionResult = ArrayMergeSort.merge(properArray1, properArray2, properArray3);
			}
			
			int j = 1;
			int portionSize = portionResult.length;
			for(int i = 0; i < portionSize; i++){
				if(i == portionSize-1){
					buffer.append(portionResult[i] + "\n");
				}else if(j%20 == 0 ){
					buffer.append(portionResult[i] + "\n");
				}else{
					buffer.append(portionResult[i] + " ");
				}
				j++;
			}
			writer.write(buffer.toString());
			
			//update the next stopping point
			nextStoppingPoint = nextStoppingPoint+segmentSize;
			
			if(!input1.hasNext() && !input2.hasNext() && !input3.hasNext() ){
				allFinished = true;
			}
		}
		input1.close();
		input2.close();
		input3.close();
		writer.close();
	}
	
	
	private static class IndividualMergeTask extends RecursiveAction{
		
		int filesToMerge;
		File file1;
		File file2;
		File file3;
		File targetFile;
		int maxMemory;
		String dataType;
		
		IndividualMergeTask(File file1, File file2, File targetFile, String dataType, int maxMemorySize){
			this.filesToMerge = 2;
			this.file1 = file1;
			this.file2 = file2;
			this.targetFile = targetFile;
			this.dataType = dataType;
			this.maxMemory = maxMemorySize;
		}
		
		//deprecated
		private IndividualMergeTask(File file1, File file2, File file3, File targetFile, String dataType, int maxMemorySize){
			this.filesToMerge = 3;
			this.file1 = file1;
			this.file2 = file2;
			this.file3 = file3;
			this.targetFile = targetFile;
			this.dataType = dataType;
			this.maxMemory = maxMemorySize;
		}
		
		@Override
		protected void compute() {
			if(filesToMerge == 2 ){ //should always call this
				if(file1 == null){
					System.out.println("file 1 is null");
				}
				
				if(file2 == null){
					System.out.println("file 2 is null");
				}
				
				if(targetFile == null){
					System.out.println("file result is null");
				}
				try{
					mergeTwo(file1, file2, targetFile, dataType, maxMemory);
				}catch( IOException ex){
					System.out.println(ex);
				}
			}else if( filesToMerge == 3){
				throw new RuntimeException("merging 3 files");
				/*deprecated 
				try{
					mergeThree(file1, file2, file3, targetFile, dataType, maxMemory);
				}catch(IOException ex){
					System.out.println(ex);
				}*/
			}
		}
	}
	
	//this is the parallel part, initially the partitions are unsorted, we will use a fork/join framework
	//we do not return any thing, simply load the partition data into an array, sort the array
	// write the sorted array into the file that we retrieved it from
	public static void parallelSort(File[] partitionArr, String dataType){  
		//we probably need to call the merge from here
		RecursiveAction mainTask = new SortTask(partitionArr, dataType); 
		ForkJoinPool pool = new ForkJoinPool(); //create the pool with all available processors
		pool.invoke(mainTask);
	}
	
	private static class SortTask extends RecursiveAction{
		private File[] partitionArray;
		private String dataType;
		int numberOfProcesses;
		
		SortTask(File[] partitionArr, String dataType){
			this.partitionArray = partitionArr;
			this.dataType = dataType;
			numberOfProcesses = partitionArray.length;
		}
		
		@Override  //we are not returning anything
		protected void compute() {
			//get the partition array size
			RecursiveAction[] individualTasks = new RecursiveAction[numberOfProcesses];
			//initiate them
			System.out.println("generating individual task");
			for(int i = 0; i < numberOfProcesses; i++){
				try{
					individualTasks[i] = new IndividualSortTask( partitionArray[i], dataType );
				}catch(FileNotFoundException ex){
					System.out.println("fail to create individual recursive task");
				}
			}
			
			for(int i = 0; i < numberOfProcesses; i++){
				individualTasks[i].fork();  //if we put the join in here, each individual thread will be created and asked to finished processing
			}
			
			for(int i = 0; i < numberOfProcesses; i++){ //****** THIS HAS TO BE DONE SEPARATELY to be parallel
				individualTasks[i].join();
			}
		}
	}
	
	private static class IndividualSortTask extends RecursiveAction{
		private String dataType;
		private File file;
		private LinkedList<? extends Number> list;
		IndividualSortTask(File file, String dataType) throws FileNotFoundException{
			this.dataType = dataType;
			this.file = file;
			System.out.println("individually processing file:"+file.getName());
			if(dataType.equals("Double")){
				list = readDoubleFile();
			}else if(dataType.equals("Integer")){
				list = readIntegerFile();
			}
		}
		
		public LinkedList<Double> readDoubleFile( ) throws FileNotFoundException{
			Scanner input = new Scanner(file);
			LinkedList<Double> list = new LinkedList<Double>();
			while( input.hasNext() ){
				list.add( input.nextDouble());
			}
			System.out.println("size of list :"+list.size());
			return list;
		}
		
		public LinkedList<Integer> readIntegerFile( ) throws FileNotFoundException{
			Scanner input = new Scanner(file);
			LinkedList<Integer> list = new LinkedList<Integer>();
			while( input.hasNext() ){
				list.add( input.nextInt() );
			}
			return list;
		}
		
		//generic method for reading the a file
		/*public <E> void readFile() throws FileNotFoundException{
			System.out.println("reading file:"+file.getName());
		}*/
		@Override //the actual merge
		protected void compute() {
			System.out.println("computing........");
			//we are not forking anymore
			Number[] listArr = null;
			if(dataType.equals("Double") ){
				listArr = list.toArray(new Double[list.size()]);
				ArrayMergeSort.sort((Double[]) listArr);
			}else if(dataType.equals("Integer")){
				listArr = list.toArray(new Integer[list.size()]);
				ArrayMergeSort.sort((Integer[]) listArr);
			}
				
			StringBuffer buffer = new StringBuffer("");
			
			int j = 1;
			for(int i = 0; i < listArr.length; i++){
				//System.out.println(listArr+" index i value:"+listArr[i] );
				if(j%20 == 0)
					buffer.append(listArr[i]+"\n");
				else
					buffer.append(listArr[i]+" ");
				j++;
			}
			
			try {
				PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));
				writer.write(buffer.toString());
				writer.close();
				System.out.println("write finished");
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("Process Finished"); //the process is not finishing
		}
	}
	
}