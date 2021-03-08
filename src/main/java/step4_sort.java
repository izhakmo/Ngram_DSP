import java.io.IOException;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class step4_sort {



    public static class Mapper_pass_to_reducer extends Mapper<LongWritable, Text, Text, Text> {


        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            String[] line_as_string = line.toString().split("\\s+");

            if (line_as_string.length > 3) {
                double prob_ascending = 1.0 - Double.parseDouble(line_as_string[3]);

//                sort first two words ascending
//                        and then sort probability descending

                Text key = new Text(line_as_string[0] + " " + line_as_string[1] + " " + prob_ascending + " " + line_as_string[2]);
                Text val = new Text();
                context.write(key,val);
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {



        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {


            String [] key_string = key.toString().split("\\s+");
            String three_gram = key_string[0] + " " + key_string[1] + " " + key_string[3];
            double prob = 1.0 - Double.parseDouble(key_string[2]);
            for (Text value : values) {
                context.write(new Text(three_gram), new Text(String.valueOf(prob)));

            }

        }

    }

}