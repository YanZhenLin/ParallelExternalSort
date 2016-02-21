package ExternalSort;

import java.util.Random;

public class DoubleGenerator extends Generator<Double> {
	Random r = new Random();
	private int rangeMin = 0;
	private int rangeMax = 1000000; // one million is default but we can change that is need be

	public DoubleGenerator(int max){
		rangeMax = max;
	}
	
	@Override
	public Double generateNext() {
		return new Double(rangeMin + (rangeMax - rangeMin) * r.nextDouble());
	}
	
}
