import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;


public class HeatMap {

	public static double max(double n, double m)

	{
		return (n>m)? n : m;
	}
	public static double min(double n, double m)

	{
		return (n>m)? m : n;
	}


		 
		public static void main(String[] args) 
		{
			/* creating spark context */
			SparkConf conf=new SparkConf().setAppName("SPATIAL6").setMaster("spark://malik:7077");
			JavaSparkContext sc=new JavaSparkContext(conf);
			sc.addJar("/home/udit/Downloads/HM.jar");
			
				List<String> target = new ArrayList<String>();
				JavaRDD<String> in=sc.textFile("hdfs://master:54310/inputfiles/heatmap/aid.csv");
				String[] input1;
				target=in.collect();
			  
				input1=target.toArray(new String[0]);
			
				Broadcast<String[]> br=sc.broadcast(input1); // Broadcasting the file as a string array
				final String[] broad;
				broad=br.value();
			
		   
		    /* 
			Creating a RDD of second input file 
			Passing the RDD to a mapToPair function and creating a Pair RDD as output of this transformation			
		    */
				
			JavaRDD<String> l2=sc.textFile("hdfs://master:54310/inputfiles/heatmap/bid.csv");
			JavaPairRDD<String,Integer> j=l2.mapToPair(new PairFunction<String,String,Integer>()
			{
				
				

				public Tuple2<String, Integer> call(String data)
				{
					/* Splitting each line of the RDD passed  */
					
					String y=null;
					int count=0;
					
					String parts[]=data.split(",");
					
					double x1=Double.parseDouble(parts[0]);
					double y1=Double.parseDouble(parts[1]);
					double x2=Double.parseDouble(parts[2]);
					double y2=Double.parseDouble(parts[3]);
					
					y=parts[0]+parts[1]+parts[2]+parts[3];
					
					for(String part: broad)
					{
						/* Splitting each line of broadcast variable in the loop */						
						String str[]=part.split(",");
						
						double a1=Double.parseDouble(str[0]);
						double b1=Double.parseDouble(str[1]);
						double a2=Double.parseDouble(str[2]);
						double b2=Double.parseDouble(str[3]);
						
						/* Condition whether one rectangle contains other rectangle or not */ 
						if((max(x1,x2) > max(a1,a2)) && (max(y1,y2) > max(b1,b2)) && (min(x1,x2) < min(a1,a2)) && (min(y1,y2) < min(b1,b2)))
						{
							count+=1;
							
						}
						
							
					}
					/* Returning tuple of query polygon and count as the output */
					return new Tuple2<String, Integer>(y,count); 
				}
			});
			
			
			List <Tuple2<String,Integer>> output=j.collect();
			for(Tuple2<?,?> tuple: output)
			{
				System.out.println("Query Polygon:"+tuple._1()+" count:"+tuple._2());
				
			}
			
			try
				{
					j.repartition(1).saveAsTextFile("hdfs://master:54310/outputfiles/HeatMap/output.txt");
				}
			
			catch(Exception e)
				{
					e.printStackTrace();
				}
			
			sc.close();
		}
	}
