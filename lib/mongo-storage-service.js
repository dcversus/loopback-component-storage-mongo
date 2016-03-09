var _ = require('lodash');
var async = require('async');
var Busboy = require('busboy');
var DataSource = require('loopback-datasource-juggler').DataSource;
var debug = require('debug')('loopback:storage:mongo');
var Grid = require('gridfs-stream');
var mongodb = require('mongodb');
var Promise = require('bluebird');
var GridFS = mongodb.GridFS;
var ObjectID = mongodb.ObjectID;
var querystring = require('querystring');
var utils = require('./mongo-storage-utils');

module.exports = MongoStorage;

// WORKAROUND need get context in several methods (eg findOrCreate)
var gContext = {};
var root = process.cwd()
var app = require(root + '/server/server');
var router = app.loopback.Router();
router.all('*', (req, res, next) => {
  gContext.req = req;
  gContext.res = res;
  next();
});
app.use(router);
// END WORKAROUND! relax

/**
 * Mongo storage constructor. Properties of settings object depend on the storage service provider.
 *
 * @settings {Object}
 *
 * @class
 */
function MongoStorage(settings) {
  this.settings = settings;
  if (!this.settings.url) {
    this.settings.url = utils.generateUrl(this.settings);
  }
}

/**
 * connect descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.connect = function(callback) {
  if (this.db) {
    return process.nextTick(() => {
      if (callback) {
        return callback(null, this.db);
      }
    });
  } else {
    return mongodb.MongoClient.connect(this.settings.url, this.settings, (err, db) => {
      if (!err) {
        debug('Mongo connection established: ' + this.settings.url);
        this.db = db;
      }
      if (callback) {
        return callback(err, db);
      }
    });
  }
};

/**
 * find descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.find = function(filter, unknown, callback) {
  if (typeof callback === 'undefined') {
    callback = unknown;
  }

  if(!filter) filter = {}
  if(!filter['where']) filter['where'] = {}
  filter['where'] = utils.renameKeys(filter['where']);

  filter['where'] = _.reduce(filter['where'], function(result, value, key) {
      result['metadata.'+key] = value;
      return result;
  }, {});

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

    return callback(null, utils.clearOutput(files));
  });
};

/**
 * count descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.count = function(filter, callback) {
  this.find(filter, function(err, files) {
    callback(err, files.length)
  })
}

/**
 * update descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.update = function(id, data, callback) {
  data = _.reduce(data, function(result, value, key) {
      result['metadata.'+key] = value;
      return result;
  }, {});


  return this.db.collection('fs.files').updateOne({
    'metadata.mongo-storage': true,
    '_id': new ObjectID(id)
  }, {
    $set: data,
    $currentDate: { "lastModified": true }
  }, (err, file) => {
    if (err) {
      return callback(err);
    }
    return this.findById(id, callback);
  });
}

MongoStorage.prototype.update.shared = true;

MongoStorage.prototype.update.accepts = [
  {
    arg: 'id',
    type: 'string'
  }, {
    arg: 'data',
    type: 'object',
    http: {
      source: 'body'
    }
  }
];

MongoStorage.prototype.update.returns = {};

MongoStorage.prototype.update.http = {
  verb: 'put',
  path: '/:id'
};

/**
 * findOne descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.findOne = function(filter, options, callback) {
  var context = gContext; // workaround, use gContext other way not found

  this.find(filter, (err, files) => {
    if (err || !files.length) {
      return callback(err);
    }

    if(context.req.method == 'DELETE') {
      return this.deleteById(files[0]._id, function() {
        context.res.send();
      })
    }

    if(context.req.method == 'PUT') {
      return this.update(files[0]._id, context.req.body, () => {
        this.findById(files[0]._id, function(err, file) {
          context.res.send(file);
        })
      })
    }

    if("download" in context.req.query) {
      return utils.download(files[0]._id, context.res, callback)
    }

    // THUMBS here

    context.res.send(utils.clearOutput(files[0]));
  })
}

/**
 * findOrCreate descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.findOrCreate = function(filter, data, options, callback) {
  var context = gContext; // workaround, use gContext other way not found

  this.find(filter, (err, files) => {
    if (err) {
      return callback(err);
    }

    if(files.length) {
      return callback(null, utils.clearOutput(files[0]))
    }

    this.create(data, context, function(err, file) {
      callback(err, utils.clearOutput(file), true)
    })

  })
}

/**
 * findById descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.findById = function(id, filter, callback) {
  var context = gContext; // workaround, use gContext other way not found
  if (typeof callback === 'undefined') {
    callback = filter;
  }

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

    if("download" in context.req.query) {
      return utils.download(file._id, context.res, callback)
    }

    return callback(null, utils.clearOutput(file));
  });
};

MongoStorage.prototype.findById.shared = true;

MongoStorage.prototype.findById.accepts = [
  {
    arg: 'id',
    type: 'string'
  }
];

MongoStorage.prototype.findById.returns = {
  arg: 'result',
  type: 'object',
  root: true
};

MongoStorage.prototype.findById.http = {
  verb: 'get',
  path: '/:id'
};

/**
 * create descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.create = function(data, context, callback) {
  try {
    var busboy = new Busboy({
      headers: context.req.headers
    });
  } catch (err) {
      return callback(err);
  }

  var promises = [];
  if(!data) data = {};
  busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
    return promises.push(new Promise(function(resolve, reject) {
      var options = {
        filename: filename,
        metadata: _.extend(data, {
          'mongo-storage': true,
          filename: querystring.unescape(filename),
          mimetype: mimetype
        })
      };

      return utils.upload(file, options, function(err, res) {
        if (err) {
          return reject(err);
        }
        return resolve(res);
      });
    }));
  });

  busboy.on('finish', function() {
    return Promise.all(promises).then(function(res) {
      return callback(null, utils.clearOutput(res));
    })["catch"](callback);
  });

  return context.req.pipe(busboy);
};

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

/**
 * deleteById descr
 * @param {Object} filter object descr
 * @callback {Function} callback Callback function
 */
MongoStorage.prototype.deleteById = function(id, callback) {
  return async.parallel([(done) => {
    return this.db.collection('fs.chunks').remove({
      files_id: new ObjectID(id)
    }, done);
  }, (done) => {
    return this.db.collection('fs.files').remove({
      _id: new ObjectID(id)
    }, done);
  }], callback);
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