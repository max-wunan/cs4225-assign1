/*
ENTER YOUR NAME HERE
NAME: WU NAN
MATRICULATION NUMBER: A0205048L
*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.util.*;
import static java.lang.Integer.parseInt;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import static org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class TopkCommonWords {

    public static int k_value;
    public static String stopwordsPath;
    
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopkCommonWords");
        job.setJarByClass(TopkCommonWords.class);

        // Set mapper, combiner and reducer class
        //job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        //FileInputFormat.addInputPath(job, new Path(args[1]));
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TokenizerMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TokenizerMapper2.class);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Set k_value & path of stopwords.txt
        k_value = Integer.parseInt(args[4]);
        stopwordsPath = args[2];

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }

    // Function to check whether word is stopword
    public static boolean isStopWord(Text word) throws IOException, FileNotFoundException {
        File stopwords = new File(stopwordsPath);
        Scanner stopwordsScanner = new Scanner(stopwords);
        while (stopwordsScanner.hasNextLine()) {
            String currLine = stopwordsScanner.nextLine();
            String[] words = currLine.split(" ");
            if (Arrays.asList(words).contains(word)) {
                return true;
            }
        }
        return false;
    }
    
    // Mapper class
    public static class TokenizerMapper1
    extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            // Get the input file name to distinguish between input from 2 files
            // String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // int file_id;

            // if (fileName == "task1-input.txt") {
            //     file_id = 1;
            // } else {
            //     file_id = 2;
            // }
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // we only want words greater than 4
                if (word.toString().length() > 4) {
                    // check whether the word is stopword
                    if (!isStopWord(word)) {
                        context.write(word,one);
                        //System.out.print(word);
                        //System.out.println(file_id);
                    }
                }
            }
        }
    }

    // Mapper class
    public static class TokenizerMapper2
    extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable two = new IntWritable(2);

        public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            // Get the input file name to distinguish between input from 2 files
            // String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // int file_id;

            // if (fileName == "task1-input.txt") {
            //     file_id = 1;
            // } else {
            //     file_id = 2;
            // }
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // we only want words greater than 4
                if (word.toString().length() > 4) {
                    // check whether the word is stopword
                    if (!isStopWord(word)) {
                        context.write(word,two);
                        //System.out.print(word);
                        //System.out.println(file_id);
                    }
                }
            }
        }
    }

    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, IntWritable, Text> {

        //private int result;
        private TreeSet<WordPair> WordFreq = new TreeSet<WordPair>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int sum_f1 = 0;
            int sum_f2 = 0;

            for (IntWritable fid : values) {
                if (fid.get() == 1) {
                    sum_f1++;
                } else {
                    sum_f2++;
                }
            }

            //context.write(new IntWritable(sum_f1), key);

            // If the word is a common word in both files
            if (sum_f1 > 0 && sum_f2 > 0) {
                int result = Math.min(sum_f1, sum_f2);
                //System.out.println(result);
                WordFreq.add(new WordPair(key.toString(), result));
                context.write(new IntWritable(result), key);
            }

        }

        @Override
        protected void cleanup(Context context)
        throws IOException, InterruptedException {
            // Sort the WordFreq in descending order
            // TreeSet<WordPair> sortedPairs = new TreeSet<>(WordFreq);
            System.out.println("cleanup function for reducer is called.");
            int k = 0;
            
            while (!WordFreq.isEmpty()) {
                if (k == k_value) {
                    break;
                }
                WordPair currPair = WordFreq.pollLast();
                System.out.print(currPair.freq);
                System.out.println(currPair.word);
                context.write(new IntWritable(currPair.freq), new Text(currPair.word));
                k++;
            }

        }
    }

    // Creating a comparable class to sort the hashset storing (word, freq) pairs
    public static class WordPair implements Comparable<WordPair> {
        public String word;
        public int freq;

        WordPair(String word, int freq) {
            this.word = word;
            this.freq = freq;
        }

        @Override
        public int compareTo(WordPair wordpair) {
            if (this.freq > wordpair.freq) {
                return 1;
            } else if (this.freq == wordpair.freq) {
                return this.word.compareTo(wordpair.word);
            } else {
                return -1;
            }
        }
    }
}
