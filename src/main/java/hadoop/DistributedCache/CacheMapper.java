package hadoop.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;





public class CacheMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
 private HashMap<Integer, String> department= new HashMap<Integer, String>();
	private static final Log LOG = LogFactory.getLog(CacheDriver.class);
	protected void setup(
		Mapper<LongWritable, Text, IntWritable, Text>.Context context)
		throws IOException, InterruptedException 
	{
	// TODO Auto-generated method stub
		Path[] cacheFiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br=new BufferedReader(new FileReader(cacheFiles[0].toString()));
		String line;
		while((line=br.readLine())!=null)
		{
			String parts[]=line.split(",");
			department.put(Integer.parseInt(parts[0]), parts[1]);
			
		}
		
		br.close();
		LOG.info("HADOOOPPAPA " + department.size());
	}
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException 
	{
		// TODO Auto-generated method stub
		String record = value.toString();
		String[] arr=record.split(",");
		String id=arr[0];
		String name=arr[1];
		
		LOG.info("MAPPERMAPABC " + record);
		String deptName=(department.get(Integer.parseInt(arr[2].trim()))).toString();
		context.write(new IntWritable(Integer.parseInt(id)), new Text(name+","+deptName));
				
	}
}
