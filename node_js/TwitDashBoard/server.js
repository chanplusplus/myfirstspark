/*
 This example code I find it at :
 https://github.com/Blackmist/hdinsight-eventhub-example/blob/master/dashboard/server.js
*/

var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);
var port = process.env.PORT || 3000;

//Serve up static files
//app.use(express.static(__dirname + '/public'));
app.use('/dashboard', express.static('public'));

server.listen(port, function() {
  console.log('server listening at port %d', port)
});

//Handle Socket.io connections
io.on('connection', function(socket) {

  socket.on('topTags', function(data) {
    console.log('got topTags' );
    socket.broadcast.emit('topTags', data);
  });

  socket.on('topLangsByTag', function(data) {
    console.log('got topLangsByTag' );
    socket.broadcast.emit('topLangsByTag', data);
  });

});
