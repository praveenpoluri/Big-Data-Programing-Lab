import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FacebookFriends {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	//starts here
        	String[] array = value.toString().split("\\t");
        	 Text outputKeyValues = new Text();
        		Text outputValue = new Text();
        	  String[] friendDetails = new String[15]; 
            	 String[] friendDetails2 = new String[15]; 
            	
                
                 String keys = "";
          for(int i=0;i<array.length;i++)
          {
        	  
          	for(int k=i+1;k<array.length;k++)
          	{
          		//fst array
          		String x = "";
          		
            		friendDetails = array[i].split(":");
            		
            		keys =  friendDetails[0];
            		String[] f = new String[20];
            		f = friendDetails[1].split(",");
            		for(int j=0;j<friendDetails[1].split(",").length;j++)
                	{
            			if(x == "")
            			{
            				x= f[j];
            			}
            			else
                		x = x + ","+ f[j];
                	}
            	
          		
          		//second array
          		
            		friendDetails2 = array[k].split(":");
            		keys = keys + "-" +  friendDetails2[0];
            		String[] f1 = new String[20];
            		f1 = friendDetails2[1].split(",");
            		for(int j=0;j<friendDetails2[1].split(",").length;j++)
                	{
            			
                		x = x + ","+ f1[j];
                	}
            	
          		outputValue.set(x);
        		outputKeyValues.set(keys);
        		context.write(outputKeyValues, outputValue);
          	}
          }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
       
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
        	
        	Text result = new Text();
        	String friends = "";
          
         for(Text value: values)
         {
        	 
        	 String[] Array = value.toString().split(",");
        	  for(int i=0;i <Array.length;i++)
        		  {
        		 // friends = Array[i];
        				for(int k=i+1;k<Array.length;k++)
        		        	{
        					
        		        		if(Array[i].equals(Array[k]))
        		        		{
        		        			if(friends == "")
        		        			{
        		        				friends = Array[i];
        		        			}else
        		        			friends = friends + "," + Array[i];
        		        			break;
        		        		}
        		        		
        		        	}
        		  }
        	 result.set(friends);
        	 context.write(key, result);
         }
        	
          
          
           
            
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mutual friends");
        job.setJarByClass(FacebookFriends.class);
        job.setMapperClass(TokenizerMapper.class);
       // job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}