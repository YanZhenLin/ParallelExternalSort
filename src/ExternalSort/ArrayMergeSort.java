package ExternalSort;
import java.util.*;

//slightly better memory allocation
public class ArrayMergeSort<E extends Comparable<E>>{	
	@SuppressWarnings("rawtypes")
	public static <E extends Comparable> void sort(E[] list){
		if(list.length > 1){
			mergeSort(list, 0, list.length-1);	
		}
	}
	
	private static <E extends Comparable<E>> void mergeSort(
			E[] list, int startIndex, int endIndex){
		if(startIndex < endIndex){
			int diff = endIndex-startIndex;
			int middleIndex;
			if(diff == 1)
				middleIndex = startIndex;
			else
				middleIndex = startIndex+(diff/2);

			mergeSort(list, startIndex, middleIndex);
			mergeSort(list, middleIndex+1, endIndex);
			merge(list, startIndex, middleIndex, middleIndex+1, endIndex);
		}
	}
	
	private static <E extends Comparable<E>> void merge(E[] list, 
			int firstStartIndex, int firstEndIndex, 
			int secondStartIndex, int secondEndIndex ){
		int originalStart = firstStartIndex; 
		int localSize = (secondEndIndex-firstStartIndex)+1;
		//we have to create a temporary storage here
		@SuppressWarnings("unchecked")
		Object[] tempArr = new Object[localSize];
		int tempIndex = 0;
		while( firstStartIndex <= firstEndIndex && secondStartIndex <= secondEndIndex){
			if(list[firstStartIndex].compareTo(list[secondStartIndex]) < 0){
				tempArr[tempIndex++] = list[firstStartIndex++]; //postfix increment
			}else{
				tempArr[tempIndex++] = list[secondStartIndex++];
			}
		}
		
		while( firstStartIndex <= firstEndIndex ){
			tempArr[tempIndex++] = list[firstStartIndex++];
		}
		while( secondStartIndex <= secondEndIndex ){
			tempArr[tempIndex++] = list[secondStartIndex++];
		}
		
		//we need to copy the tempMatrix into the original matrix
		int counter = 0;
		while(counter < tempArr.length){
			list[originalStart++] = (E)tempArr[counter++];
		}	
	}
	
	//we develop a secondary method that work with any variable number of list item, its not that hard
	public static <E extends Comparable<E>> E[] merge( E[] list1, E[] list2, E[] result ){
		int list1Index = 0;
		int list2Index = 0;
		int resultIndex = 0;
		//intitlaize the result array we will return
		//E[] result = (E[])new Comparable[list1.length+list2.length];
		
		while(list1Index < list1.length && list2Index < list2.length){
			if( list1[list1Index].compareTo(list2[list2Index]) < 0 ){ //list1 item is smaller than list2 item
				result[resultIndex++] = list1[list1Index++]; 
			}else{
				result[resultIndex++] = list2[list2Index++]; 
			}
		}
		
		while(list1Index < list1.length){
			result[resultIndex++] = list1[list1Index++]; 
		}
		
		while(list2Index < list2.length){
			result[resultIndex++] = list2[list2Index++];
		}
		
		return result;
	}
		
		
	/*we will make this module extensible up to any number of pass in list
	although this won't be that efficient as teh numbers increase*/
	//merging three list into one, should be stable
	/*
	public static <E extends Comparable> E[] merge( E[]... args ){
		int numberOfListToCombine = args.length; //number of args
		int resultIndex = 0;
		int[] currentIndexArray = new int[numberOfListToCombine]; //each cell represents the currentIndex position of that particular list
		
		int TotalSize = 0;
		for(int i = 0; i < numberOfListToCombine; i++){
			TotalSize += args[i].length;
		}
		
		E[] result = (E[])new Object[TotalSize];
		
		while(resultIndex < TotalSize){ //while smaller
			E currentSmallest = null; //on every loop, its set to null
			int indexOfListContainingSmallest = 0; //the index of the list that holds the current smallest
			for(int i = 0; i < numberOfListToCombine; i++){
				//ArrayList<E> currentComparisons = new ArrayList<E>();
				//we don't need another arrayList for this.
				int currentListIndex = currentIndexArray[i];
				
				if( currentListIndex < args[i].length ){ //could have ran out
					if(currentSmallest == null ){
						currentSmallest = args[i][currentListIndex];
						indexOfListContainingSmallest = i;
					}else{
						if( args[i][currentListIndex].compareTo(currentSmallest) < 0 ){ //if smaller than the current
							currentSmallest = args[i][currentListIndex];
							indexOfListContainingSmallest = i;
						}
					}
				}//if the index on this list has ran out, there's nothing for us to do
			}
			result[resultIndex++] = currentSmallest;
			int updatedValue = currentIndexArray[indexOfListContainingSmallest]+1;
			currentIndexArray[indexOfListContainingSmallest] = updatedValue;
		}
		return result;
	}*/
	
}