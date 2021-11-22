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
                JSONObject tweet = new JSONObject(value.toString());
                String created_at = tweet.getString("created_at");
                Long id = tweet.getLong("id");
                String text = tweet.getString("text");
                JSONObject user = tweet.getJSONObject("user");
                Long userId = user.getLong("id");
                int followers_count = user.getInt("followers_count");
                int retweet_count = tweet.getInt("retweet_count");
                JSONObject entities = tweet.getJSONObject("entities");
                JSONArray hashtagsJson = entities.getJSONArray("hashtags");
                int nb_hashtags = hashtagsJson.length();
                ArrayList<String> hashtags = new ArrayList<>();
                for (int i=0; i<nb_hashtags; i++) {
                   hashtags.add(hashtagsJson.getString(i));
                }
                boolean retweeted = tweet.getBoolean("retweeted");
                String lang = tweet.getString("lang");
                context.write(key,
                        new TweetWritable(created_at, id, text, userId, followers_count, retweet_count, nb_hashtags, hashtags, retweeted, lang));
            } catch (JSONException e) {
                e.printStackTrace();
            }


        }



    }
    public static void main(String[] args) throws Exception {
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