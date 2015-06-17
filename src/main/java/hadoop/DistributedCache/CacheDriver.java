package hadoop.DistributedCache;



import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CacheDriver extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new CacheDriver(), args);
        System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
		
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJarByClass(CacheDriver.class);
        job.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
        //DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);
        
        
        job.setMapperClass(CacheMapper.class);
        //job.setReducerClass(CountR.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0);
        
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
	}

}
