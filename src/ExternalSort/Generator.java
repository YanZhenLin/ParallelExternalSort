package ExternalSort;

//has either interger or Double concrete classes
public abstract class Generator<E>{

	public abstract E generateNext();
	public <E> E[] generate( int size ){
		E[] elements = (E[])new Object[size];
		for(int i = 0; i < size; i++){
			elements[i] = (E)generateNext();
		}
		return elements;
	}
	
}
