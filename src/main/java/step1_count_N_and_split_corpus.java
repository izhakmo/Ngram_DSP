import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data


import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class step1_count_N_and_split_corpus {
    public static Counter counter;




    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Pattern pattern;
        Text big_counter_N;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) {
//          hash alphabetically comes before any char
            pattern = Pattern.compile("[א-ת]+");
            big_counter_N = new Text("#");
        }

        @Override
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            Text val;
            String[] line_as_string = line.toString().split("\\s+");
            Text key ;

            if (line_as_string.length > 4) {

                Matcher m1 = pattern.matcher(line_as_string[0]);
                Matcher m2 = pattern.matcher(line_as_string[1]);
                Matcher m3 = pattern.matcher(line_as_string[2]);
                boolean w1_len = line_as_string[0].length()>1;
                boolean w2_len = line_as_string[1].length()>1;
                boolean w3_len = line_as_string[2].length()>1;

                boolean b = m1.matches() && m2.matches() && m3.matches() && w1_len && w2_len && w3_len;
                if (b) {
                    String occurrences = line_as_string[4];
                    key = new Text(line_as_string[0] + " " + line_as_string[1] + " " + line_as_string[2]);
                    Text oc = new Text(occurrences);

//                    split corpus to 2 parts
                    if (lineId.get() % 2 == 0) {

                        val = new Text(occurrences + " 0");
                    } else {
                        val = new Text("0 " + occurrences);
                    }
                    context.write(big_counter_N, oc);
                    context.write(key, val);
                }
            }
        }

    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {


        @Override
        protected void setup(Reducer<Text, Text, Text, Text>.Context context) {
             counter = context.getCounter(all_steps.NCounter.N_COUNTER);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long sum_of_first_corpus = 0;
            long sum_of_second_corpus = 0;

//            counter
            if(key.toString().equals("#")){
                long big_counter_N = CombinerClass.counter_func(values);
                long old_counter = counter.getValue();

//                update counter
                counter.setValue(old_counter + big_counter_N);
                counter = context.getCounter(all_steps.NCounter.N_COUNTER);


            }
            else {
                reducer_combiner_two_corpuses(key, values, context, sum_of_first_corpus, sum_of_second_corpus);
            }
        }

        private static void reducer_combiner_two_corpuses(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context, long sum_of_first_corpus, long sum_of_second_corpus) throws IOException, InterruptedException {
            for (Text value : values) {
                String [] occ_in_each_corpus = value.toString().split("\\s+");
                try {
                    sum_of_first_corpus += Long.parseLong(occ_in_each_corpus[0]);
                    sum_of_second_corpus += Long.parseLong(occ_in_each_corpus[1]);

                }
                catch (NumberFormatException e){
                    System.out.println("NumberFormatException " + value.toString());
                    System.out.println("occ_in_each_corpus[0] " + occ_in_each_corpus[0]);
                    System.out.println("occ_in_each_corpus[1] " + occ_in_each_corpus[1]);

                }
            }
            String corpuses = sum_of_first_corpus + " " + sum_of_second_corpus;

            context.write(key, new Text(corpuses));
        }


    }
    public static class CombinerClass extends Reducer<Text,Text,Text,Text> {


        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text,Text,Text,Text>.Context context) throws IOException, InterruptedException {
            long sum_of_first_corpus = 0;
            long sum_of_second_corpus = 0;
            if(key.toString().equals("#")){
                long big_counter_N = counter_func(values);
                String combined_counter = Long.toString(big_counter_N);

                context.write(key,new Text(combined_counter));


            }
            else {
                ReducerClass.reducer_combiner_two_corpuses(key, values, context, sum_of_first_corpus, sum_of_second_corpus);
            }
        }

        private static long counter_func(Iterable<Text> values) {
            long big_counter_N = 0;
            for (Text value : values) {

                long add_to_counter;
                try {
                    add_to_counter = Long.parseLong(value.toString());
                }
                catch (NumberFormatException e){
                    add_to_counter = 0;
                    System.out.println("NumberFormatException " + value.toString());
                }
                big_counter_N += add_to_counter;


            }
            return big_counter_N;
        }


    }



}