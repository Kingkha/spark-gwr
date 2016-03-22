package hiepnt;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.jblas.DoubleMatrix;

import scala.Tuple2;



public class GWRSpark {
	public static JavaRDD<ArrayList<Double>> parseData(JavaRDD<String> textFile){
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
		
		return rows;
	}
	
	public static JavaRDD<ArrayList<Double>> computeDistances(JavaRDD<ArrayList<Double>> rows, Coord point){
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
		
		return distancedRows;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		final Coord point = new Coord(35.62, 42.38);
		List<Coord> points = new ArrayList<Coord>();
		points.add(point);
		String path = "./data/columbus.csv";
		SparkConf conf = new SparkConf().setAppName("Spark GWR").setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(path);
		Broadcast<List<Coord>> bPoints = sc.broadcast(points);
		
		/*
		 * map (string) -> (coordinate, row)
		 */
		JavaPairRDD<Coord, ArrayList<Double>> rowCoords = textFile.flatMapToPair(new PairFlatMapFunction<String, Coord, ArrayList<Double>>(){

			@Override
			public Iterable<Tuple2<Coord, ArrayList<Double>>> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Double> row = new ArrayList<Double>();
				String[] toks = arg0.split(",");
				for(String tok: toks){
					row.add(Double.parseDouble(tok));
				}
				ArrayList<Tuple2<Coord, ArrayList<Double>>> result = new ArrayList<Tuple2<Coord, ArrayList<Double>>>();
				for(int i = 0; i < bPoints.getValue().size(); i++){
					result.add(new Tuple2<Coord, ArrayList<Double>>(bPoints.getValue().get(i), row));	
				}
				return result;
			}
			
		});
		
		/*
		 * map (coordinate, row) -> (coordinate, row + distance)
		 */
		JavaPairRDD<Coord, ArrayList<Double>> rowDistances = rowCoords.mapToPair(new PairFunction<Tuple2<Coord, ArrayList<Double>>, Coord, ArrayList<Double>>(){

			@Override
			public Tuple2<Coord, ArrayList<Double>> call(Tuple2<Coord, ArrayList<Double>> arg0) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Double> result2 = new ArrayList<Double>(arg0._2);
				Coord o = new Coord(arg0._2.get(arg0._2.size() - 2), arg0._2.get(arg0._2.size() - 1));
				result2.add(o.distance(arg0._1));
				return new Tuple2<Coord, ArrayList<Double>>(arg0._1, result2);
			}
			
		});
		
		/*
		 * group (coordinate, row + distance) -> (coordinate, data matrix) 
		 */
		JavaPairRDD<Coord, Iterable<ArrayList<Double>>> data = rowDistances.groupByKey();
		System.out.println("data size = " + rowDistances.count());
		
		/*
		 * map (coordinate, data) -> (coordinate, beta)
		 */
		JavaPairRDD<Coord, DoubleMatrix> result = data.mapToPair(new PairFunction<Tuple2<Coord,Iterable<ArrayList<Double>>>, Coord, DoubleMatrix>(){

			@Override
			public Tuple2<Coord, DoubleMatrix> call(Tuple2<Coord, Iterable<ArrayList<Double>>> arg0) throws Exception {
				// TODO Auto-generated method stub
				int row = 0, column = 0;
				for(ArrayList<Double> tmp : arg0._2){
					row++;
					column = tmp.size();
				}
				DoubleMatrix data = DoubleMatrix.ones(row, column - 1);
				DoubleMatrix w = DoubleMatrix.zeros(row, row);
				int count = 0;
				for(ArrayList<Double> line : arg0._2){
					for(int i = 0; i < column - 1; i++){
						data.put(count, i, line.get(i));
					}
					w.put(count, count, line.get(column - 1));
					count++;
				}
				GWR gwr = new GWR(data);
				double bw = gwr.getBw(0, 10);
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
				return new Tuple2<Coord, DoubleMatrix>(arg0._1, beta);
			}
			
		});
		List<Tuple2<Coord, DoubleMatrix>> results = result.collect();
		System.out.println("size = " + results.size());
		for(Tuple2<Coord, DoubleMatrix> one: results){
			System.out.print(one._1 + " ");
			System.out.println(one._2);
		}
		
		/*		
		//map from line to row of input matrix, first line is y, last two line is coordinates
		JavaRDD<ArrayList<Double>> rows = parseData(textFile);
		
		//add distance field to each row above
		JavaRDD<ArrayList<Double>> distancedRows = computeDistances(rows, point);
		
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
				points.add(new Coord(line.get(column - 3), line.get(column - 2)));
			}
			w.put(count, count, line.get(column - 1));
			count++;
		}*/
	
		
		/*
		 *Convert points to RDD
		 */
		JavaRDD<Coord> pointsRDD = sc.parallelize(points);
				
		/*GWR gwr = new GWR(data);
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
		*/

		
		sc.close();
	}

}
