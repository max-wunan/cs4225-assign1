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


public class TopkCommonWords {

    public static int k_value;
    public static String stopwordsPath;
    
    public static void main(String[] args){
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TopkCommonWords");
        job.setJarByClass(TopkCommonWords.class);

        // Set mapper, combiner and reducer class
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
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
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            // Get the input file name to distinguish between input from 2 files
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            int file_id;

            if (fileName == "task1-input.txt") {
                file_id = 1;
            } else {
                file_id = 2;
            }
            
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                // we only want words greater than 4
                if (word.toString().length() > 4) {
                    // check whether the word is stopword
                    if (isStopWord(word) == false) {
                        context.write(word,new IntWritable(file_id));
                    }
                }
            }
        }
    }

    
    public static class IntSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

        private int result;
        //private Map<String, Integer> WordFreq = new TreeMap<String, Integer>();
        private HashSet<WordPair> WordFreq = new HashSet<WordPair>();

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

            // If the word is a common word in both files
            if (sum_f1 > 0 && sum_f2 > 0) {
                result = Math.min(sum_f1, sum_f2);
                WordFreq.add(new WordPair(key.toString(), result));
            }

        }

        public void cleanup(Context context)
        throws IOException, InterruptedException {
            // Sort the WordFreq in descending order
            TreeSet<WordPair> sortedPairs = new TreeSet<>(WordFreq);
            while (k_value > 0 && sortedPairs.isEmpty() == false) {
                WordPair currPair = sortedPairs.pollLast();
                context.write(new Text(currPair.word), new IntWritable(currPair.freq));
                k_value--;
            }

        }
    }

    // Creating a comparable class to sort the hashset storing (word, freq) pairs
    public static class WordPair implements Comparable<WordPair> {
        String word;
        int freq;

        WordPair(String word, int freq) {
            this.word = word;
            this.freq = freq;
        }

        @Override
        public int compareTo(WordPair wordpair) {
            if (this.freq > wordpair.freq) {
                return -1;
            } else if (this.freq == wordpair.freq) {
                return (0 - this.word.compareTo(wordpair.word));
            } else {
                return 1;
            }
        }
    }
}
