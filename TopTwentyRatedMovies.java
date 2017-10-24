
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class TopTwentyRatedMovies extends Configured implements Tool{

	public static void main(String[] args) throws Exception
	{
		if (args.length != 3 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
			System.exit(0);
		}
		int res = ToolRunner.run(new Configuration(), new TopTwentyRatedMovies(), args);
		System.exit(res);

	}
	public int run(String[] args) throws Exception {
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path p1=new Path(files[0]);
		Path p2=new Path(files[1]);
		Path p3=new Path(files[2]);
		FileSystem fs = FileSystem.get(c);
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		Job job = new Job(c,"Multiple Job");
		job.setJarByClass(TopTwentyRatedMovies.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap1.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MultipleMap2.class);
		job.setReducerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, p3);
		boolean success = job.waitForCompletion(true);
		return success?0:1;
	}
	

}

// This is to process ratings file
class MultipleMap1 extends Mapper<Object, Text, Text, Text> {

    
    private Text word = new Text();
    private Text word2 = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		
    		String strs [] = value.toString().split("::");
        
            word.set(strs[1].trim());
            word2.set(strs[2].trim()+"::");
            context.write(word, word2);
        // key movie id and value is rating
    }
}
// This is to process movies file
class MultipleMap2 extends Mapper<Object, Text, Text, Text> {

    
    private Text word1 = new Text();
    private Text word2 = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		
    	String strs [] = value.toString().split("::");
            
    	  word1.set(strs[0].trim());
          word2.set("::"+strs[1].trim());
          context.write(word1, word2);
    	// key- movie id and value - movie name
    }
}
class MultipleReducer extends Reducer<Text,Text,Text,Text>
{
	Text valEmit = new Text();
	
	int totalRating = 0;
	int count = 0;
	String movieName = "";
	private Map<Text, Movie> movieMap = new HashMap<Text, Movie>();
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException , InterruptedException
			{
		
		for(Text value:values)
		{
			
			String[] strs = value.toString().split("::");
			if(strs.length==1)
			{	
				int val = Integer.parseInt(strs[0].trim());
				totalRating+=val;
				count++;
			}
			else
				movieName = strs[1].trim();
		
		}
		// this is to calculate avg  rating for movie
		double avgRating = (totalRating/count);
		Movie movie = new Movie(key.toString(), avgRating, movieName);
		movieMap.put(key, movie);
		
	}
	@Override
     protected void cleanup(Context context) throws IOException, InterruptedException {

         List<Movie> movies = new ArrayList<Movie>();
         for(Text text : movieMap.keySet())
         {
         	movies.add(movieMap.get(text));
         }
         // Sort list in deceasing order based on avg rating
         Collections.sort(movies, new Comparator<Movie>() {

				public int compare(Movie o1, Movie o2) {
					if(o2.rating < o1.rating)
						return -1;
					if(o2.rating > o1.rating)
						return 1;
					return 0;
				}
			});
         int counter = 0;
         // This is to write top 20 rated movies to output file.
         for (Movie movie : movies) 
         {
         	
        	Text key = new Text();
            Text value = new Text();
            key.set(movie.movieName);
            value.set(movie.rating+"");
            context.write(key, value);
         	if(++counter==20)
         		break;
         }
       
         
     }
	 class Movie
     {
  	   String movieId;
  	   double rating;
  	   String movieName;
  	   public Movie(String movieId, double rating, String movieName)
  	   {
  		   this.movieId = movieId;
  		   this.rating = rating;
  		   this.movieName = movieName;
  	   }
     }
}