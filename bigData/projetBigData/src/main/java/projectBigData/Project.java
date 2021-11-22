package projectBigData;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Project {
    
    public static class ProjectMapper extends Mapper<LongWritable, Text, LongWritable, TweetWritable>{

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                System.out.println("AAAAAAAAAAAAAAAA");
                JSONObject tweet = new JSONObject(value.toString());
                String created_at = tweet.getString("created_at");
                System.out.println(created_at + " 1");
                Long id = tweet.getLong("id");
                System.out.println("2 : " + id);
                String text = tweet.getString("text");
                System.out.println("3 : " + text);
                JSONObject user = tweet.getJSONObject("user");
                Long userId = user.getLong("id");
                int followers_count = user.getInt("followers_count");
                int retweet_count = tweet.getInt("retweet_count");
                System.out.println("BBBBBBBBBBBBbb");
                JSONObject entities = tweet.getJSONObject("entities");
                JSONArray hashtagsJson = entities.getJSONArray("hashtags");
                int nb_hashtags = hashtagsJson.length();
                ArrayList<String> hashtags = new ArrayList<>();
                System.out.println("CCCCCCCCCCCCCCCCCCCCC");
                for (int i=0; i<nb_hashtags; i++) {
                   hashtags.add(hashtagsJson.getString(i));
                }
                boolean retweeted = tweet.getBoolean("retweeted");
                String lang = tweet.getString("lang");
                System.out.println("WRIIIIIIIIIIIITE");
                context.write(key,
                        new TweetWritable(created_at, id, text, userId, followers_count, retweet_count, nb_hashtags, hashtags, retweeted, lang));
                System.out.println("DDDDDDDDDDDDDDDDDDDDDDDDD");
            } catch (JSONException e) {
                e.printStackTrace();
            }


        }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Projet");
            job.setNumReduceTasks(0);
            job.setJarByClass(Project.class);
            job.setMapperClass(ProjectMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(TweetWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(TweetWritable.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job, new Path(args[0]));
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }



}