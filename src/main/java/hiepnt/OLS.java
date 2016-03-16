package hiepnt;

import org.jblas.*;

/*
 * Origin Least Square Linear Regression
 * @author hiepnt
 */
public class OLS {
	public static DoubleMatrix beta(DoubleMatrix x, DoubleMatrix y) throws IllegalArgumentException{
		if(x.rows != y.length){
			throw new IllegalArgumentException("Matrixs length do not match");
		}
		DoubleMatrix xtx = (x.transpose()).mmul(x);
		System.out.println(xtx);
		DoubleMatrix xty = (x.transpose()).mmul(y);
		DoubleMatrix result = Solve.pinv(xtx).mmul(xty);
		
		return result;
	}
	
	
	public static void main(String[] args){
		DoubleMatrix x = DoubleMatrix.ones(8, 2);
		DoubleMatrix y = DoubleMatrix.ones(8);
		for(int i = 2; i < 10; i++){
			x.put(i - 2, i);
			x.put(i-2 + 8, i + 1);
			y.put(i - 2, i + 1);
		}
		
		DoubleMatrix beta = OLS.beta(x, y);
		System.out.println(beta);
	}
}
