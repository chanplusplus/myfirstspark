## Installation
Assume that in your local machine, apache spark is install and `PATH` is configured correctly. If not so, please follow the below instruction:
```sh
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.0-bin-hadoop2.6.tgz -P .
sudo tar zxvf ./spark-* -C /usr/local
sudo mv /usr/local/spark-* /usr/local/spark
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```
The exact location of the installation isn't important. What is necessary is that `spark-submit` should be able to be called on your system. To check if this is the case, try running:
```sh
$ which spark-submit
/usr/local/spark/bin/spark-submit
```
Again, it is totally fine if the output is not `/usr/local/spark/bin/spark-submit`, so long as it is some directory's name.

### Setup the Web Application
```sh
$ git clone https://github.com/arabbig/myfirstspark.git
$ cd myfirstspark
$ mv java/out/artifacts/TweetTopLanguageByTag_jar/TweetTopLanguageByTag.jar ~
```
### Running the NodeJS server
```sh
$ node node_js/TwitDashBoard/server.js
```
Then, go to http://localhost:3000/dashboard.

Internally, the server.js will fire up the shell command
```sh
spark-submit --class TopLanguageByTag --master local[4] ~/TweetTopLanguageByTag.jar
```


## Highlighted Code Section


![hi][slide1]
![hi][slide2]
![hi][slide3]
```java
        JavaDStream<String> words = tweets.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterable<String> call(Status s) throws Exception {
                return Arrays.asList(s.getText().split("\\s+"));
            }
        });

        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>()  {
            @Override
            public Boolean call(String word) throws Exception {
                return word.startsWith("#");
            }
        });

        JavaPairDStream<String, Integer> hashTagCount = hashTags.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        // leave out the # character
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });

        Function2<Integer, Integer, Integer> addition = new Function2<Integer, Integer, Integer>()  {
            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        };

        JavaPairDStream<String, Integer> hashTagTotals = hashTagCount
                .reduceByKeyAndWindow(addition, outputWindow, outputSlide);
```
![hi][slide4]
![hi][slide5]
```java
        JavaPairDStream<String,String> langWordPairs = tweets.flatMapToPair(
                new PairFlatMapFunction<Status, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Status s) throws Exception {
                        String[] words = s.getText().split("\\s+");
                        ArrayList<Tuple2<String, String>> pairs = new ArrayList<Tuple2<String, String>>(words.length);
                        for (int i = 0; i != words.length; ++i) {
                            pairs.add(new Tuple2<String, String>(s.getLang(), words[i]));
                        }
                        return pairs;
                    }
                }
        );

        JavaPairDStream<String,String> langAndHashTags = langWordPairs.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String,String> lt) throws Exception {
                        return lt._2().startsWith("#");
                    }
                });


        JavaPairDStream<Tuple2<String,String>, Integer> langAndTagCounts = langAndHashTags.mapToPair(
                new PairFunction<Tuple2<String, String>, Tuple2<String,String>, Integer>(){
                    @Override
                    public Tuple2<Tuple2<String,String>, Integer> call(Tuple2<String, String> lt) throws Exception {
                        return new Tuple2<Tuple2<String,String>, Integer>(lt, 1);
                    }
                }
        );

        JavaPairDStream<Tuple2<String,String>, Integer> langAndTagTotals = langAndTagCounts
                .reduceByKeyAndWindow(addition,outputWindow, outputSlide);
```
![hi][slide6]
![hi][slide7]
![hi][slide8]
![hi][slide9]
![hi][slide10]
![hi][slide11]
```java
        /*
            This part perform "Build priority queues, containing top-K hash tags, indexed by language"
         */

        class MinQStringPair extends PriorityQueue<StringIntPair> {
            int maxSize;
            MinQStringPair(int maxSize) {
                super(maxSize);
                this.maxSize = maxSize;
            }
            public MinQStringPair putIntoTopK(StringIntPair newPair) {
                if(size() < maxSize )  super.add(newPair);
                else {
                    if(newPair.compareTo(super.peek()) > 0) {
                        super.poll();
                        super.add(newPair);
                    }
                }
                return this;
            }
        };


        Function<StringIntPair, MinQStringPair> createCombiner = new Function<StringIntPair, MinQStringPair>() {
            public MinQStringPair call(StringIntPair langCnt) throws Exception{
                MinQStringPair minQ = new MinQStringPair(topNTags);
                minQ.putIntoTopK(langCnt);
                return minQ;
            }
        };
        Function2<MinQStringPair, StringIntPair, MinQStringPair> mergeValue =
                new Function2<MinQStringPair, StringIntPair, MinQStringPair>() {
                    public MinQStringPair call(MinQStringPair minQ, StringIntPair langCnt) throws Exception {
                        minQ.putIntoTopK(langCnt);
                        return minQ;
                    }
                };
        Function2<MinQStringPair, MinQStringPair, MinQStringPair> mergeCombiners =
                new Function2<MinQStringPair, MinQStringPair, MinQStringPair>() {
                    public MinQStringPair call(MinQStringPair qa, MinQStringPair qb) throws Exception {
                        MinQStringPair qc, qd;
                        if(qa.size() > qb.size()) { qc = qa; qd = qb; }
                        else { qc = qb; qd = qa; }
                        while(qd.size() != 0)
                            qc.putIntoTopK(qd.poll());
                        return qc;
                    }
                };

        JavaPairDStream<String, MinQStringPair> langAndTogNTags =
                langAndTagTotals2.combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(4), true);
```
[slide1]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.001.jpg "Fig 1"
[slide2]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.002.jpg "Fig 2"
[slide3]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.003.jpg "Fig 3"
[slide4]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.004.jpg "Fig 4"
[slide5]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.005.jpg "Fig 5"
[slide6]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.006.jpg "Fig 6"
[slide7]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.007.jpg "Fig 7"
[slide8]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.008.jpg "Fig 8"
[slide9]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.009.jpg "Fig 9"
[slide10]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.010.jpg "Fig 10"
[slide11]: http://arabbig.github.io/myfirstspark/slide/MyFirstSparkSlide.011.jpg "Fig 11"

