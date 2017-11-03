// 1. node exec.js old_connection new_connection [Redis Instances]
// 1-1. node redisBatch.js '["172.31.9.3:6379", "172.31.9.2:6379"]' '["172.31.9.2:22121", "172.31.9.3:22121"]' '["172.31.9.2:22124","172.31.9.3:22124"]' str:vip:relief_coin y
// 2. Scan the keys from Redis Instances
// 3. Save Keys into the set to filter the duplicate keys.
// 4. Fetch the data from old Redis Proxy.
// 5. Save the data into the new Redis Proxy.
// 6. Print the total getting keys and inserttng key to console.

/*
 * RedisSocket.js
 */

'use strict';

var util = require('util');
var events = require('events');

var _ = require('lodash');
var redis = require('redis');
var async = require('async');
var Promise = require('bluebird');
var fs = require('fs');

var RedisSocket = function RedisSocket(startupConfig) {
  events.EventEmitter.call(this);
  this.startupConfig = startupConfig;
  // this.connect();
  this.changeState('INIT');
};
util.inherits(RedisSocket, events.EventEmitter);

RedisSocket.prototype.isReady = function() {
  return 'READY' === this.state;
};

RedisSocket.prototype.changeState = function(state) {
  this.state = state;
  this.emit('state_changed');
};

RedisSocket.prototype.getConfig = function() {
  var redisProxy = this.startupConfig;
  console.log('RedisSocket.prototype.getConfig:' + redisProxy);
  var parts = redisProxy.split('/');
  var ip_ports = parts[0].split(':');
  this.ip = ip_ports[0];
  this.port = ip_ports[1];
  this.VHOST = parts[1];
};

RedisSocket.prototype.connect = function() {
  this.getConfig();
  var client = redis.createClient(this.port, this.ip, {
    no_ready_check: true,
    max_attempts: 1,
  });

  client.on('ready', function(){
    console.log('RedisSocket.prototype.connect:on:ready');
    this.client = client;
    this.changeState('READY');
  }.bind(this));

  client.on('error', function(error) {
    console.log('RedisSocket.prototype.connect:error:' + error);
  });

  client.on('end', function() {
    console.log('RedisSocket.prototype.connect:on:end');
    client.removeAllListeners();
    setTimeout(this.connect.bind(this), 3000);
    this.changeState('END');
  }.bind(this));
};

RedisSocket.prototype.vhost = function(key) {
  return key;
};

RedisSocket.prototype.sendCommand = function(cmd, args, logFunc, cbFunc) {
  var argsClone;
  if ('MGET' === cmd) {
    argsClone = _.map(args, function(arg){
      return this.vhost(arg);
    }.bind(this));
  } else {
    argsClone = _.isArray(args) ? _.toArray(args) : [args];
    argsClone[0] = this.vhost(argsClone[0]);
  }
  logFunc && logFunc('RedisSocket.prototype.sendCommand:redis> ' +
    cmd + ' ' + _.map(argsClone, function(arg){
      if (!_.isString(arg)) return arg;
      return arg.indexOf(' ') === -1 ? arg : '"' + arg + '"';
    }).join(' ')
  );
  this.client.send_command(cmd, argsClone, function(error, reply){
    (error && logFunc && logFunc(
        'RedisSocket.prototype.sendCommand:error> ' + error));
    (!error && logFunc && (
      !_.isArray(reply)
      ? logFunc('RedisSocket.prototype.sendCommand:reply> ' + reply)
      : _.each(reply, function(line){
        logFunc('RedisSocket.prototype.sendCommand:reply> ' + line);
      })
    ));
    cbFunc && cbFunc(error, reply);
  });
};

var scanKeys = function(redis_nodes, pattern, log, callback){
  async.map(redis_nodes, 
    function(redis, callback){
      var keys = [];
      var iterator = 0;
      async.doWhilst(
        function(callback){
          redis.sendCommand('SCAN', [iterator, 'MATCH', '*' + pattern + '*'], null, 
            function(err, result){
              if(err) return callback(err, null);
              iterator = result[0];
              result = _.reduce(result, function(res, value, key){
                if(key != 0 && _.trim(value).length > 0){
                  res.push(value)
                }   
                return res;
              }, []);
              if(_.isEmpty(result)) return callback(null, iterator);
              keys = _.concat(keys, result);
              return callback(null, iterator);
            });
        },
        function(){ 
          console.log('> scankey:' + iterator);
          return 0 != iterator;
        },
        function(err, n){
          if(err) return callback(err, keys);
          callback(null, keys);
        });
      }, function(err, result){
        if(err){
          return callback(err, result);
        }
        var total_keys = [];
        _.forEach(result, function(value, key){
          total_keys = _.union(total_keys, _.flattenDepth(value));
        });
        console.log(total_keys);
        return callback(null, total_keys);
      });
};

var fetchData = function(redis, keys, log, callback){
  async.map(keys,
    function(key, callback){
      async.waterfall([
        function(callback){
          redis.sendCommand('TYPE', key, log,
            function(err, result){
              if(err) return callback(err, result);
              callback(null, key, result);
            });
        },
        function(key, type, callback){
          console.log('key:' + key);
          console.log('type:' + type);
          if('hash' == type){
            redis.sendCommand('HGETALL', key, log, 
              function(err, result){
                if(err) return callback(err, result);
                callback(null, {'type': type, 'key': key, 'value':result});
              });
          }
          else if('zset' == type){
            redis.sendCommand('ZRANGE', [key, 0, -1, 'WITHSCORES'], log, 
              function(err, result){
                if(err) return callback(err, result);
                callback(null, {'type': type, 'key': key, 'value':result});
              });
          }
          else if('string' == type){
            redis.sendCommand('GET', key, log, 
              function(err, result){
                if(err) return callback(err, result);
                callback(null, {'type': type, 'key': key, 'value':result});
              });
          }
          else if('list' == type){
            redis.sendCommand('LRANGE', [key, 0, -1], log, 
              function(err, result){
                if(err) return callback(err, result);
                callback(null, {'type': type, 'key': key, 'value':result});
              });
          }
          else{
            return callback('Fetch Data Type Err:' + type + ':' + key, null);
          }
        },
        function(obj, callback){
          redis.sendCommand('TTL', key, log,
            function(err, result){
              if(err) return callback(err, result);
              obj.ttl = result;
              callback(null, obj);
            });
        }],
        function(err, result){
          if(err) return callback(err, result);
          callback(null, result);
        });
      },
      function(err, result){
        if(err) callback(err, result);
        callback(null, result);
      });
};

var insertData = function(redis, data, log, callback){
  async.map(data, 
    function(data, callback){
      //console.log('type:' + data.type);
      //console.log('key:' + data.key);
      //console.log('value:' + data.value);
      //console.log('TTL:' + data.ttl);
      async.waterfall([
        function(callback){
          if('string' == data.type){
            redis.sendCommand('SET', [data.key, data.value], log, 
              function(err, result){
                if(err){
                  console.log('set err:' + err);
                  return callback(err, result);
                }
                callback(null, result);
              });
          }
          if('hash' == data.type){
            var insert_data = _.flatten(_.concat(data.key,_.toPairs(data.value)));
            redis.sendCommand('HMSET', insert_data, log, 
              function(err, result){
                if(err){
                  console.log('HMSET err:' + err);
                  return callback(err, result);
                }
                callback(null, result);
              });
          }
          if('zset' == data.type){
            var insert_keys = _.reduce(data.value, function(res, value, index){ 
              if(index % 2 == 0) res.push(value); 
              return res; 
            }, []);
            var insert_values = _.reduce(data.value, function(res, value, index){ 
              if(index % 2 != 0) res.push(value); 
              return res; 
            }, []);
            var insert_data = _.zip(insert_values, insert_keys);
            async.map(insert_data, 
              function(insert_data, callback){
                redis.sendCommand('ZADD', _.concat(data.key, insert_data), log, 
                  function(err, result){
                    if(err) return callback(err, result);
                    callback(null, result);
                  });
              },
              function(err, result){
                if(err) return callback(err, result);
                callback(null, result);
              });
          }
          if('list' == data.type){
            redis.sendCommand('RPUSH', _.concat(data.key, data.value), log, 
              function(err, result){
                if(err) return callback(err, result);
                callback(null, result);
            });
          }
        },
        function(set_result, callback){
          if( 0 > data.ttl) return callback(null, set_result);
          redis.sendCommand('EXPIRE', [data.key, data.ttl], log,
            function(err, expire_result){
              if(err) return callback(err, expire_result);
              callback(null, expire_result);
          });
        }], 
        function(err, result){
          if(err) return callback(err, result);
          callback(null, result);
        });
    }, function(err, result){
      if(err) return callback(err, result);
      callback(null, result);
    });
};

var main = function(config){
  return new Promise(function(resolve, reject){
    fs.readFile(__dirname + '/' + config, function (err, content){
      if(err) return reject(err);
      return resolve(content.toString());
    });
  }).then(function(data){
    var redis = JSON.parse(data);
    return redis;
  }).then(function(redis){
    var redis_data = {
      redises: {
        nodes: _.map(redis.from_nodes, function(config){
          return new RedisSocket(config);
        }),
        from_proxies: _.map(redis.from_proxy, function(config){
          return new RedisSocket(config);
        }),
        to_proxies: _.flow([
          function(redis){
            return _.get(redis, 'to_proxy', []);
          },function(to_proxy){
            return _.map(to_proxy, function(config){
              return new RedisSocket(config);
            });
          }])(redis),
      },
      pattern: redis.pattern,
      output: redis.output
    };
    return _.assign(redis_data, {
      insert: !_.isEmpty(redis_data.to_proxies)
    });
  }).then(function(data){
    return Promise.promisify(fs.stat)(data.output).then(function(){
      return Promise.promisify(fs.unlink)(data.output).then(function(){
        return data;
      });
    }).catch(function(err){
      console.log('delete output file failed');
      return data;
    });
  }).then(function(data){
    return Promise.all(_.map(_.flatten(_.values(data.redises)), function(redis){
      return new Promise(function(resolve){ 
        redis.connect();
        redis.on('state_changed', function(){
          if (redis.isReady()){
            return resolve();
          }
        });
      });
    })).then(function(){
      console.log(data);
      return data;
    });
  }).then(function(data){
    console.log('> FETCH KEYS');
    return Promise.promisify(scanKeys)(
      data.redises.nodes, 
      data.pattern,
      console.log).then(function(keys){
        return _.assign(data, {
          keys: keys
        });
    });
  }).then(function(data){
    console.log('> FETCH DATA');
    var chunk_size = Math.ceil(data.keys.length / data.redises.from_proxies.length);
    return Promise.map(_.chunk(data.keys, chunk_size),
      function(keys){
        var redis = _.sample(data.redises.from_proxies);
        return Promise.promisify(fetchData)(
          redis, 
          keys, 
          console.log);
      }).then(function(result){
        var values = _.flatten(result);
        console.log('>> Total data:' + values.length);
        console.log(values);
        return _.assign(data, {
          values: values
        });
      });
  }).then(function(data){
    // 3. Insert Data to the new Redis nodes.
    if(_.isEmpty(data.redises.to_proxies)){
      console.log('do not insert data');
      return data;
    }
    console.log('> INSERT DATA');
    var chunk_size = Math.ceil(data.values.length / data.redises.to_proxies.length);
    return Promise.map(_.chunk(data.values, chunk_size),
      function(value){
        var redis = _.sample(data.redises.to_proxies);
        return Promise.promisify(insertData)(
          redis, 
          value, 
          console.log);
      }).then(function(result){
        return data;
      });
  }).then(function(data){
    return new Promise(function(resolve){
      console.log('> WRITE TO FILE:' + data.output);
      var values = _.sortBy(data.values, ['key']);
      var wstream = fs.createWriteStream(data.output);
      values.map(function(value){
        var content = 'key:' + value.key + '\n' +
                      'type:' + value.type + '\n' +
                      'ttl:' + value.ttl + '\n';
        if (_.isArrayLikeObject(value.value)){
          value.value.map(function(ele){
            content += ele + '\n';
          });
        } else if(_.isObjectLike(value.value)){
          var fields = _.keys(value.value);
          fields.sort();
          var obj_content = _.map(fields, function(field){
            return field + ',' + value.value[field] + '\n';
          });
          content += _.join(obj_content, '');
        } else if(_.isString(value.value)){
          content += value.value+ '\n';
        } else {
          content += 'unknown type\n';
        }
        content += '========================================================' + '\n';
        wstream.write(content);
      });
      wstream.end();
      wstream.on('finish', function(){
        return resolve(data);
      });
    });
  }).then(function(data){
    console.log('> BATCH FINISH');
    return process.exit(0);
  }).catch(function(err){
    console.log('> BATCH ERROR:' + err);
    return process.exit(1);
  });
}(process.argv[2]);

