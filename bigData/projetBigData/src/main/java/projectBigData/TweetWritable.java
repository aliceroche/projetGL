package projectBigData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.annotate.JsonProperty;

public class TweetWritable implements Writable {
    private String created_at;
    private Long id;
    private String text;
    private Long userId;
    private int followers_count;
    private int retweet_count;
    private int nb_hashtags;
    private ArrayList<String> hashtags;
    private boolean retweeted;
    private String lang;

    public TweetWritable() {
    }

    public TweetWritable(
            String created_at,
            Long id, String text,
            Long userId,
            int followers_count,
            int retweet_count,
            int nb_hashtags,
            ArrayList<String> hashtags,
            boolean retweeted,
            String lang) {
        this.created_at = created_at;
        this.id = id;
        this.text = text;
        this.userId = userId;
        this.followers_count = followers_count;
        this.retweet_count = retweet_count;
        this.nb_hashtags = nb_hashtags;
        this.hashtags = hashtags;
        this.retweeted = retweeted;
        this.lang = lang;
    }

    public void write(DataOutput out) throws IOException {
        out.writeChars(created_at);
        out.writeLong(id);
        out.writeChars(text);
        out.writeLong(userId);
        out.writeInt(followers_count);
        out.writeInt(retweet_count);
        out.writeInt(nb_hashtags);
        for (String hashtag: hashtags) {
            out.writeChars(hashtag);
        }
        out.writeBoolean(retweeted);
        out.writeChars(lang);
    }

    public void readFields(DataInput in) throws IOException {
        created_at = in.readLine();
        id = in.readLong();
        text = in.readLine();
        userId = in.readLong();
        followers_count = in.readInt();
        retweet_count = in.readInt();
        nb_hashtags = in.readInt();
        for (int i=0; i<nb_hashtags; i++) {
            hashtags.add(in.readLine());
        }
        retweeted = in.readBoolean();
        lang = in.readLine();
    }
    
}