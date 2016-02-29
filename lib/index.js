var _ = require('lodash');
var async = require('async');
var Busboy = require('busboy');
var DataSource = require('loopback-datasource-juggler').DataSource;
var debug = require('debug')('loopback:storage:mongo');
var root = process.cwd()
var app = require(root + '/server/server');
var Grid = require('gridfs-stream');
var mongodb = require('mongodb');
var Promise = require('bluebird');
var GridFS = mongodb.GridFS;
var ObjectID = mongodb.ObjectID;
var gContext = {};

var generateUrl = function(options) {
  var database, host, port;
  host = options.host || options.hostname || 'localhost';
  port = options.port || 27017;
  database = options.database || 'test';
  if (options.username && options.password) {
    return "mongodb://" + options.username + ":" + options.password + "@" + host + ":" + port + "/" + database;
  } else {
    return "mongodb://" + host + ":" + port + "/" + database;
  }
};

// WORKAROUND need get context in several methods (eg findOrCreate)
var router = app.loopback.Router();
router.post('*', (req, res, next) => {
  gContext.req = req;
  gContext.res = res;
  next();
});
app.use(router);
// END WORKAROUND! relax

var MongoStorage = (function() {

  function MongoStorage(settings) {
    this.settings = settings;
    if (!this.settings.url) {
      this.settings.url = generateUrl(this.settings);
    }
  }

  MongoStorage.prototype.connect = function(callback) {
    var self;
    self = this;
    if (this.db) {
      return process.nextTick(function() {
        if (callback) {
          return callback(null, self.db);
        }
      });
    } else {
      return mongodb.MongoClient.connect(this.settings.url, this.settings, function(err, db) {
        if (!err) {
          debug('Mongo connection established: ' + self.settings.url);
          self.db = db;
        }
        if (callback) {
          return callback(err, db);
        }
      });
    }
  };

  MongoStorage.prototype.find = function(filter, callback) {
    if(!filter) filter = {}
    if(!filter['where']) filter['where'] = {}
    filter['where']['metadata.mongo-storage'] = true;
    var options = {
      skip: filter['skip'] || 0,
      limit: filter['limit'] || 0
    }

    return this.db.collection('fs.files')
    .find(filter['where'], options)
    .toArray(function(err, files) {
      if (err) {
        return callback(err);
      }
      return callback(null, files);
    });
  };

  MongoStorage.prototype.count = function(filter, callback) {
    this.find(filter, function(err, files) {
      callback(err, files.length)
    })
  }

  MongoStorage.prototype.findOne = function(filter, options, callback) {
    if(!filter) filter = {}
    if(!filter['where']) filter['where'] = {}
    filter['where']['metadata.mongo-storage'] = true;

    return this.db.collection('fs.files')
    .findOne(filter['where'], function(err, file) {
      if (err) {
        return callback(err);
      }
      return callback(null, file);
    })
  }

  MongoStorage.prototype.findOrCreate = function(query, data, options, callback) {
    var context = gContext; // workaround, use gContext other way not found
    this.create(data, context, function(err, file) {
      callback(err, file, true)
    })
  }

  MongoStorage.prototype.findById = function(id, callback) {
    return this.db.collection('fs.files').findOne({
      'metadata.mongo-storage': true,
      '_id': new ObjectID(id)
    }, function(err, file) {
      if (err) {
        return callback(err);
      }
      if (!file) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }
      return callback(null, file);
    });
  };

  MongoStorage.prototype.create = function(data, context, callback) {
    var self = this;
    var busboy = new Busboy({
      headers: context.req.headers
    });
    var promises = [];

    busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
      return promises.push(new Promise(function(resolve, reject) {
        var options = {
          filename: filename,
          metadata: {
            'mongo-storage': true,
            filename: filename,
            mimetype: mimetype
          }
        };

        return self.uploadFile(file, options, function(err, res) {
          if (err) {
            return reject(err);
          }
          return resolve(res);
        });
      }));
    });

    busboy.on('finish', function() {
      return Promise.all(promises).then(function(res) {
        return callback(null, res);
      })["catch"](callback);
    });
    return context.req.pipe(busboy);
  };

  MongoStorage.prototype.uploadFile = function(file, options, callback) {
    var gfs, stream;
    if (callback == null) {
      callback = (function() {});
    }
    options._id = new ObjectID();
    options.mode = 'w';
    gfs = Grid(this.db, mongodb);
    stream = gfs.createWriteStream(options);
    stream.on('close', function(metaData) {
      return callback(null, metaData);
    });
    stream.on('error', callback);
    return file.pipe(stream);
  };

  MongoStorage.prototype.deleteById = function(id, callback) {
    var self;
    self = this;
    return async.parallel([
      function(done) {
        return self.db.collection('fs.chunks').remove({
          files_id: new ObjectID(id)
        }, done);
      }, function(done) {
        return self.db.collection('fs.files').remove({
          _id: new ObjectID(id)
        }, done);
      }
    ], callback);
  };

  MongoStorage.prototype.download = function(id, res, callback) {
    var self;
    if (callback == null) {
      callback = (function() {});
    }
    self = this;

    return this.db.collection('fs.files').findOne({
      'metadata.mongo-storage': true,
      '_id': new ObjectID(id)
    }, function(err, file) {
      if (err) {
        return callback(err);
      }
      if (!file) {
        err = new Error('File not found');
        err.status = 404;
        return callback(err);
      }

      var gfs, read;
      gfs = Grid(self.db, mongodb);
      read = gfs.createReadStream({
        _id: file._id
      });
      res.set('Content-Disposition', "attachment; filename=\"" + file.filename + "\"");
      res.set('Content-Type', file.metadata.mimetype);
      res.set('Content-Length', file.length);
      return read.pipe(res);
    });
  };

  return MongoStorage;

})();

MongoStorage.modelName = 'storage';

MongoStorage.prototype.create.shared = true;

MongoStorage.prototype.create.accepts = [
  {
    arg: 'data',
    type: 'object'
  }, {
    arg: 'context',
    type: 'object',
    http: {
      source: 'context'
    }
  }
];

MongoStorage.prototype.create.returns = {
  arg: 'result',
  type: 'object',
  root: true
};

MongoStorage.prototype.create.http = {
  verb: 'post',
  path: '/'
};

MongoStorage.prototype.find.shared = true;

MongoStorage.prototype.find.accepts = [
  {
    arg: 'filter',
    type: 'object'
  }
];

MongoStorage.prototype.find.returns = {
  arg: 'result',
  type: 'object',
  root: true
};

MongoStorage.prototype.find.http = {
  verb: 'get',
  path: '/'
};

MongoStorage.prototype.findById.shared = true;

MongoStorage.prototype.findById.accepts = [
  {
    arg: 'id',
    type: 'string'
  }
];

MongoStorage.prototype.findById.returns = {
  arg: 'file',
  type: 'array',
  root: true
};

MongoStorage.prototype.findById.http = {
  verb: 'get',
  path: '/:id/detail'
};

MongoStorage.prototype.download.shared = true;

MongoStorage.prototype.download.accepts = [
  {
    arg: 'id',
    type: 'string'
  }, {
    arg: 'res',
    type: 'object',
    http: {
      source: 'res'
    }
  }
];

MongoStorage.prototype.download.returns = {
  arg: 'file',
  type: 'array',
  root: true
};


MongoStorage.prototype.download.http = {
  verb: 'get',
  path: '/:id'
};

MongoStorage.prototype.deleteById.shared = true;

MongoStorage.prototype.deleteById.accepts = [
  {
    arg: 'id',
    type: 'string'
  }
];

MongoStorage.prototype.deleteById.returns = {};

MongoStorage.prototype.deleteById.http = {
  verb: 'delete',
  path: '/:id'
};

exports.initialize = function(dataSource, callback) {
  var connector, k, m, method, opt, ref, settings;
  settings = dataSource.settings || {};
  connector = new MongoStorage(settings);
  dataSource.connector = connector;
  dataSource.connector.dataSource = dataSource;
  connector.DataAccessObject = function() {};
  ref = MongoStorage.prototype;
  for (m in ref) {
    method = ref[m];
    if (_.isFunction(method)) {
      connector.DataAccessObject[m] = method.bind(connector);
      for (k in method) {
        opt = method[k];
        connector.DataAccessObject[m][k] = opt;
      }
    }
  }
  connector.define = function(model, properties, settings) {};
  if (callback) {
    dataSource.connector.connect(callback);
  }
};