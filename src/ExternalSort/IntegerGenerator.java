package ExternalSort;

public class IntegerGenerator extends Generator<Integer>{
	private int max = 1000000;  
	
	public IntegerGenerator(int max){
		this.max = max;
	}
	
	@Override
	public Integer generateNext() {
		return new Integer((int) (Math.random()*max));
	}
	
}
