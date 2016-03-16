package hiepnt;

import java.io.IOException;

import org.jblas.*;

public class GWR {
	DoubleMatrix x;
	DoubleMatrix y;
	DoubleMatrix position;
	
	public GWR(DoubleMatrix x, DoubleMatrix y, DoubleMatrix position) {
		super();
		this.x = x;
		this.y = y;
		this.position = position;
	}

	public GWR(DoubleMatrix x, DoubleMatrix y) {
		super();
		int[] xIndices = new int[x.columns - 2];
		int[] posIndices = new int[2];
		for(int i = 0; i < x.columns - 2; i++){
			xIndices[i] = i;
		}
		for(int i = x.columns - 2; i < x.columns; i++){
			posIndices[i - x.columns + 2] = i;
		}
		this.y = y;
		this.x = DoubleMatrix.concatHorizontally(DoubleMatrix.ones(x.rows), x.getColumns(xIndices));
		this.position = x.getColumns(posIndices);
	}
	
	public GWR(DoubleMatrix data){
		/*
		 * Constructor from data matrix
		 */
		super();
		int[] yIndices = new int[1];
		yIndices[0] = 0;
		int[] xIndices = new int[data.columns - 3];
		for(int i = 1; i < data.columns - 2; i++)
			xIndices[i - 1] = i;
		int[] posIndices = new int[2];
		posIndices[0] = data.columns - 2;
		posIndices[1] = data.columns - 1;
		this.x = DoubleMatrix.concatHorizontally(DoubleMatrix.ones(data.rows), data.getColumns(xIndices));
		this.y = data.getColumns(yIndices);
		this.position = data.getColumns(posIndices);
	}
	
	public double getBw(double a, double b){
		return goldenSearch(a, b);
	}
	
	public DoubleMatrix beta(double bw, Coord pos){
		//Create kernel matrix w
		DoubleMatrix w = DoubleMatrix.zeros(x.rows, x.rows);
		for(int i = 0; i < x.rows; i++){
			Coord current = new Coord(position.get(i, 0), position.get(i, 1));
			double dis = pos.distance(current);
			w.put(i, i, Math.exp(-0.5 * (dis/bw) * (dis/bw)) );
		}
		
		//Compute beta
		DoubleMatrix xtx = (x.transpose()).mmul(w).mmul(x);
		DoubleMatrix xty = (x.transpose()).mmul(w).mmul(y);
		DoubleMatrix result = Solve.pinv(xtx).mmul(xty);	
		
		return result;
	}
	
	/*
	 * Compute beta with known weighted matrix w
	 */
	public DoubleMatrix beta(double bw, DoubleMatrix w){
		DoubleMatrix xtx = (x.transpose()).mmul(w).mmul(x);
		DoubleMatrix xty = (x.transpose()).mmul(w).mmul(y);
		DoubleMatrix result = Solve.pinv(xtx).mmul(xty);	
		
		return result;
	}
	
	
	private double goldenSearch(double a, double b){
		/*
		 * Golden section search
		 * https://en.wikipedia.org/wiki/Golden_section_search
		 * return bandwidth 
		 * Note: not work in some case T_T
		 */
		double gr = (Math.sqrt(5) - 1) / 2;
		double tol = 0.000001;
		double c = b - gr * (b - a);
		double d = a + gr * (b - a);
		while(Math.abs(c - d) > tol){
			double fc = f(c);
			double fd = f(d);
			if(fc < fd){
				b = d;
				d = c;
				c = b - gr * (b - a);
				System.out.println(c);
			}else{
				a = c;
				c = d;
				d = a + gr * (b - a);
				System.out.println(d);
			}
		}
		return (b + a)/2 ;
	}
	
	private double f(double bw){
		/*
		 * Compute function's value for golden ratio search
		 */
		double f = 0;
		for(int i = 0; i < x.rows; i++){
			double yHat = x.getRow(i).mmul(beta(bw, i)).get(0);
			double y0 = y.get(i);
			f += Math.pow(yHat - y0, 2);
		}
		return f;
	}
	
	private DoubleMatrix beta(double bw, int i){
		/*
		 * Compute beta matrix with point i omitted from calibration process
		 * Using for compute cv score
		 */
		Coord pos = new Coord(position.getRow(i).get(0), position.getRow(i).get(1));
		int[] index = new int[x.rows - 1];
		for(int j = 0; j < x.rows - 1; j++){
			if(j < i)
				index[j] = j;
			if(j >= i)
				index[j] = j + 1;
		}
		DoubleMatrix x1 = x.getRows(index);
		DoubleMatrix y1 = y.getRows(index);
		DoubleMatrix pos1 = position.getRows(index);
		GWR gwr = new GWR(x1, y1, pos1);
		return gwr.beta(bw, pos);
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		DoubleMatrix data = DoubleMatrix.loadCSVFile("./data/columbus.csv");
		GWR gwr = new GWR(data);
		Coord pos = new Coord(35.62, 42.38);
		DoubleMatrix beta = gwr.beta(2.275032, pos);
		System.out.println(beta);
		System.out.println(gwr.f(2.275032));
		System.out.println("golden ratio search ");
		System.out.println(gwr.goldenSearch(0, 50));
	}

}
