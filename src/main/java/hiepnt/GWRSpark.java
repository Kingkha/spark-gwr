package hiepnt;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.jblas.DoubleMatrix;



public class GWRSpark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Coord point = new Coord(35.62, 42.38);
		String path = "./data/columbus.csv";
		SparkConf conf = new SparkConf().setAppName("Spark GWR").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(path);
		
		//map from line to row of input matrix, first line is y, last two line is coordinates
		JavaRDD<ArrayList<Double>> rows = textFile.map(new Function<String, ArrayList<Double>>(){
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public ArrayList<Double> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Double> result = new ArrayList<Double>();
				String[] tok = arg0.split(",");
				for(String token : tok){
					result.add(Double.parseDouble(token));
				}
				return result;
			}
		});
		
		//add distance field to each row above
		JavaRDD<ArrayList<Double>> distancedRows = rows.map(new Function<ArrayList<Double>, ArrayList<Double>>(){
			/**
			 * 
			 */			
			private static final long serialVersionUID = 1L;

			public ArrayList<Double> call(ArrayList<Double> arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Double> result = new ArrayList<Double>(arg0);
				Coord o = new Coord(arg0.get(arg0.size() - 2), arg0.get(arg0.size() - 1));
				result.add(point.distance(o));
				return result;
			}
			
		});
		
		List<ArrayList<Double>> matrixRows = distancedRows.collect();
		int row = matrixRows.size();
		int column = matrixRows.get(0).size();
		System.out.println("row = " + row + " column = " + column);
		DoubleMatrix data = DoubleMatrix.ones(row, column - 1);
		DoubleMatrix w = DoubleMatrix.zeros(row, row);
		int count = 0;
		for(ArrayList<Double> line : matrixRows){
			for(int i = 0; i < column - 1; i++){
				data.put(count, i, line.get(i));
			}
			w.put(count, count, line.get(column - 1));
			count++;
		}
		System.out.println("data = ");
		System.out.println(data);
				
		GWR gwr = new GWR(data);
		double bw = gwr.getBw(0, 15);
		System.out.println("bw = " + bw);
		for(int i = 0; i < row; i++){
			double dis = w.get(i, i);
			w.put(i, i,  Math.exp(-0.5 * (dis/bw) * (dis/bw)));
		}
		DoubleMatrix beta = gwr.beta(bw, w);
		System.out.println("w = ");
		System.out.println(w.diag());
		System.out.println("beta = ");
		System.out.println(beta);
		

		
		sc.close();
	}

}
