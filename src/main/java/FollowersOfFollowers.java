import java.io.IOException;
import java.util.*;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FollowersOfFollowers {
    private static final String FOLLOWED_BY_CODE = "0";
    private static final String FOLLOWS_CODE = "1";

    public static class FirstIterationMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
            StringTokenizer tokenizer  = new StringTokenizer(value.toString());
            if (tokenizer.hasMoreTokens()) {
                String userId = tokenizer.nextToken();
                if (tokenizer.hasMoreTokens()) {
                    String followerId = tokenizer.nextToken();
                    context.write(new Text(userId), new Text(followerId + " " + FOLLOWED_BY_CODE));
                    context.write(new Text(followerId), new Text(userId + " " + FOLLOWS_CODE));
                }
            }
        }
    }

    public static class FirstIterationReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            HashSet<String> hash = new HashSet<String>();
            for (Text value : values) {
                String[] input = value.toString().split(" ");
                if (input[1].equals(FOLLOWS_CODE)) {
                    hash.add(input[0]);
                } else {
                    builder.append(" " + input[0]);
                }
            }
            context.write(key, new Text(builder.toString().trim()));
            for (String val : hash) {
                context.write(new Text(val), new Text(builder.toString().trim()));
            }
        }
    }

    public static class SecondIterationMapper extends Mapper<Object, Text, Text, Text> {
        private static Text userId = new Text();
        private static Text followerId = new Text();
        public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
            StringTokenizer tokenizer  = new StringTokenizer(value.toString());
            if (tokenizer.hasMoreTokens()) {
                userId.set(tokenizer.nextToken());
                while (tokenizer.hasMoreTokens()) {
                    followerId.set(tokenizer.nextToken());
                    context.write(userId, followerId);
                }
            }
        }
    }

    public static class User {
        public Text userId;
        public int followerCount;

        public User(Text uid, int count) {
            userId = new Text(uid.toString());
            followerCount = count;
        }

        public static Comparator<User> getComparatorByCount() {
            Comparator<User> comp = new Comparator<User>() {
                @Override
                public int compare(User u1, User u2) {
                    return u1.followerCount > u2.followerCount ? -1 : u1.followerCount == u2.followerCount ? 0 : 1;
                }
            };
            return comp;
        }
    }

    public static class SecondIterationReducer extends Reducer<Text,Text,Text,Text> {
        private static ArrayList<User> userList = new ArrayList<User>();
        private static Text count = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> values2 = new HashSet<String>();
            for (Text val : values) {
                values2.add(val.toString().trim());
            }
            userList.add(new User(key, values2.size()));
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(userList, User.getComparatorByCount());
            int counter = 0;
            for (User u : userList) {
                if (counter == 10) {
                    break;
                }
                count.set(Integer.toString(u.followerCount));
                context.write(u.userId, count);
                counter++;
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "First Iteration");
        job.setJarByClass(FollowersOfFollowers.class);
        job.setMapperClass(FirstIterationMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // job.setCombinerClass(FirstIterationReducer.class);
        job.setReducerClass(FirstIterationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Second Iteration");
        job2.setJarByClass(FollowersOfFollowers.class);
        job2.setMapperClass(SecondIterationMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        // job2.setCombinerClass(SecondIterationReducer.class);
        job2.setReducerClass(SecondIterationReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
    }
}