# Ngram


### Running instructions

insret the args as follow:
1. jar_path
2. output_path (bucket)
3. ec2 key pair
4. write "yes" or "no" if you want to run the program with a combiner

## info
N-grams are fixed size tuples of items.

- Given a corpus from google three-gram, the program do as follow:

	- Counts the legal words and divide the corpus into two parts.
	- Calculate the probability of each three-gram.
	- Sort the Ngrams alphabetically, and by probability.


The project has 4 map reduce jobs:

1. job 1 filters illegal three grams, divide the corpus, sums the occurrences of each three gram at each corpus, counts the number of three-grams in the corpus.

2. job 2 maps the occurrences of each corpus as a key, and sends the relevant Nr Tr to the reducer,
job 2 also sends to the reducer with the occurrences of each corpus as a key, the three gram as a val

base on the r given as a key, the reducer sums and calculate  Nr Tr for each r
job 2 has a Partitioner that takes care that Nr Tr will be calculated first, and then do the join of each calculated Nr Tr of each corpus to all three grams that has the relevant r occurrences (mapper tags the key-val of the trigram with "zzz", and key-val with occurrences with "xxx" ==> therefore all of the "xxx" will be calculated before the reducer will receive the three gram that was tagged with "zzz")

so for each three gram - reducer write the three gram as a key for each corpus (once with Nr Tr of corpus one , and once for corpus two)

3. job 3 mapper just passes the key vals that was received from job 2
reducer saves the last three gram, if the three gram has not changed - reducer sums the Nr Tr
when three gram has changed - reducer calculate probabilty from the given Nr Tr and big_counter_N, and writes three gram with the probability

4. job 4 is executing the sort - (w1w2 alphabetically ascending) and then (w1w2w3 probabiliy descending).
