package ejercicios.amigosdeamigos;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import writables.AmigosWritable2;
import constantes.Constantes;
/** @author Álvaro Sánchez Blasco */
public class CopyOfAmigosJob extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.out.println(Constantes.HELP_AMIGOS);
			return -1;
		}
		String input = args[0];
		String output = args[1];
		
		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);
		
		Job job = Job.getInstance(getConf(), "Amigos de mis amigos Job");
		job.setJarByClass(CopyOfAmigosJob.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AmigosWritable2.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(CopyOfAmigosMap.class);
		job.setReducerClass(CopyOfAmigosRed.class); 		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		System.out.println(Constantes.FIN);
		
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CopyOfAmigosJob(), args);
	}

}
