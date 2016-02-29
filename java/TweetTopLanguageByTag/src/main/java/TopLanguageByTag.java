import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;

import com.github.nkzawa.socketio.client.IO;
import com.github.nkzawa.socketio.client.Socket;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.Serializable;
import java.util.*;
import java.net.URISyntaxException;

import scala.Tuple2;
import twitter4j.Status;
/**
 * Created by Chan Pruksapha on 2/9/2016 AD.
 */
public class TopLanguageByTag {

    static Duration streamSlide, outputSlide, outputWindow;
    static Integer topKLangs;
    static Integer topNTags;

    static void initParams(int N, int K, int ss, int os, int ow){
        topNTags = N;
        topKLangs = K;

        streamSlide = new Duration(ss);
        outputSlide = new Duration(os);
        outputWindow = new Duration(ow);

        // Configuring Twitter credentials
        String apiKey = "On4lmf0wFOQ72qpZLMdJHYOXQ";
        String apiSecret = "tskmQAEwFJSEDGntnkRGgjOxX8nVr8JyTbwtiliJ0G3eumuyFR";
        String accessToken = "35429495-V3UmzdTxQX3Wqh8Zfh8BsjlGGcxdeWSELTCh4VLBe";
        String accessTokenSecret = "vgKiJPoPVui2baXZCAW878IothSD5qYMO6K3INfbMXD7q";
        System.setProperty("twitter4j.oauth.consumerKey", apiKey);
        System.setProperty("twitter4j.oauth.consumerSecret", apiSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        Logger.getLogger("org").setLevel(Level.OFF);
    }

    static Socket getSocket(){
        Socket tmp_socket;
        try {
            tmp_socket = IO.socket("http://localhost:3000");

        } catch (URISyntaxException e) {
            tmp_socket = null;
        }
        return tmp_socket;
    }


    // Create a local StreamingContext with two working thread and batch interval of 1 second
    public static void main(String[] args) throws URISyntaxException {

        initParams( 5, 10, 5000, 5000, 60000);

        final Socket socket = getSocket();
        if(socket != null) {
            socket.connect();
        } else {
            System.err.println("Can't reach Node.js Server!!!");
            System.exit(0);
        }

        final SparkConf sparkConf = new SparkConf().setAppName("TopLanguageByTag");
        final JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, streamSlide);


        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc);
        tweets.persist();

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

        hashTagCount.print();


        /*****   Get top N tags   *****/

        JavaPairDStream<Integer,String> totalHashtags = hashTagTotals.mapToPair(
                new PairFunction<Tuple2<String, Integer>, Integer, String>(){
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String,Integer> lt) throws Exception{
                        return new Tuple2<Integer, String>(lt._2(),lt._1());
                    }
                });


        class MyComparable implements Comparator<Tuple2<Integer, String>>, Serializable {
            public int compare(Tuple2<Integer, String> o1, Tuple2<Integer, String> o2) {
                return o2._1() - o1._1();
            }
        };



        JavaPairDStream<String, Integer> topHashTags = totalHashtags.transformToPair(new Function<JavaPairRDD<Integer, String>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<Integer, String> totalHashTag) throws Exception {

                List<Tuple2<Integer, String>> topList = totalHashTag.takeOrdered(topNTags, new MyComparable());
                List<Tuple2< String, Integer>> topList_rev = new ArrayList<Tuple2< String, Integer>>(topList.size());

                // List of (tag, cnt)
                JSONArray jsonArr = new JSONArray();

                for(Tuple2<Integer,String> e: topList) {
                    topList_rev.add(new Tuple2<String,Integer>(e._2(), e._1()));

                    // Single (tag, cnt)
                    JSONObject tagcnt = new JSONObject();
                    jsonArr.put(tagcnt.put("tag", e._2()).put("count", e._1()));
                }

                socket.emit("topTags",jsonArr);
                return jssc.sparkContext().parallelizePairs(topList_rev);
            }
        });

        topHashTags.print();


        /*
           Get Popular hashtags in each country.
           Perform only on the most four popular langauges
         */

        JavaPairDStream<String,String> wordAndLangs = tweets.flatMapToPair(
                new PairFlatMapFunction<Status, String, String>() {
                    @Override
                    public Iterable<Tuple2<String, String>> call(Status s) throws Exception {
                        String[] words = s.getText().split("\\s+");
                        ArrayList<Tuple2<String, String>> pairs = new ArrayList<Tuple2<String, String>>(words.length);
                        for (int i = 0; i != words.length; ++i) {
                            pairs.add(new Tuple2<String, String>(words[i],s.getLang()));
                        }
                        return pairs;
                    }
                }
        );

        JavaDStream<Tuple2<String,String>> topTagAndLangs = wordAndLangs.window(outputWindow, outputSlide).join(topHashTags).map(
                new Function<Tuple2<String, Tuple2<String, Integer>>,Tuple2<String,String>>() {
                    @Override
                    public Tuple2<String,String> call(Tuple2<String, Tuple2<String, Integer>> lsc) throws Exception {
                        return new Tuple2<String,String>(lsc._1(),lsc._2()._1());
                    }
                });


        JavaPairDStream<Tuple2<String,String>, Integer> topTagAndLangCounts = topTagAndLangs.mapToPair(
                new PairFunction<Tuple2<String, String>, Tuple2<String,String>, Integer>(){
                    @Override
                    public Tuple2<Tuple2<String,String>, Integer> call(Tuple2<String, String> lt) throws Exception {
                        return new Tuple2<Tuple2<String,String>, Integer>(lt, 1);
                    }
                }
        );

        JavaPairDStream<Tuple2<String,String>, Integer> topTagAndLangTotals = topTagAndLangCounts.reduceByKey(addition);

        class StringIntPair implements Comparable<StringIntPair>, Serializable {
            String str; Integer num;
            StringIntPair(String str, Integer num) {
                this.str = str; this.num = num;
            }
            public int compareTo(StringIntPair o) {
                return this.num - o.num;
            }
        };

        PairFunction<Tuple2<Tuple2<String,String>, Integer>, String, StringIntPair> makePair4 = new PairFunction<Tuple2<Tuple2<String,String>, Integer>, String, StringIntPair>(){
            @Override
            public Tuple2<String, StringIntPair> call(Tuple2<Tuple2<String,String>, Integer> topTagAndLangTotal) throws Exception {
                Tuple2<String,String> tagAndLang = topTagAndLangTotal._1();
                String tag = tagAndLang._1();
                String lang = tagAndLang._2();
                Integer total = topTagAndLangTotal._2();
                return new Tuple2<String, StringIntPair>(tag, new StringIntPair(lang, total));
            }
        };


        JavaPairDStream<String, StringIntPair> topTagAndLangTotals2 = topTagAndLangTotals.mapToPair(makePair4);


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
            public MinQStringPair call(StringIntPair tagCnt) throws Exception{
                MinQStringPair minQ = new MinQStringPair(topKLangs);
                minQ.putIntoTopK(tagCnt);
                return minQ;
            }
        };
        Function2<MinQStringPair, StringIntPair, MinQStringPair> mergeValue =
                new Function2<MinQStringPair, StringIntPair, MinQStringPair>() {
                    public MinQStringPair call(MinQStringPair minQ, StringIntPair tagCnt) throws Exception {
                        minQ.putIntoTopK(tagCnt);
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

        JavaPairDStream<String, MinQStringPair> topTagAndTopLang =
                topTagAndLangTotals2.combineByKey(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(4), true);

        topTagAndTopLang.foreachRDD(new VoidFunction<JavaPairRDD<String, MinQStringPair>>() {
            @Override
            public void call(JavaPairRDD<String, MinQStringPair> langAndMinQ) {
                List<Tuple2<String, MinQStringPair>> topList = langAndMinQ.collect();

                JSONArray jsonArr1 = new JSONArray();

                for (Tuple2<String, MinQStringPair> pair : topList) {

                    JSONObject jsonObj = new JSONObject();

                    System.out.println(String.format("%s :", pair._1()));
                    jsonObj.put("tag", pair._1());

                    ArrayList<StringIntPair> langCntList = new ArrayList<StringIntPair>(pair._2());
                    Collections.sort(langCntList);
                    Collections.reverse(langCntList);
                    JSONArray jsonArr2 = new JSONArray();
                    for(StringIntPair e: langCntList){
                        System.out.print(String.format(" (%s,%d)",e.str,e.num));
                        jsonArr2.put(new JSONObject().put("lang",e.str).put("count",e.num));
                    }

                    System.out.println();
                    jsonObj.put("topLangs", jsonArr2);
                    jsonArr1.put(jsonObj);
                }

                socket.emit("topLangsByTag",jsonArr1);
            }
        });



        jssc.start();
        jssc.awaitTermination();

    }

}
