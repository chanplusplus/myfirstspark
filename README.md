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
