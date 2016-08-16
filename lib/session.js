var Async = require('async');

var internals = {};

internals.defaultOptions = {
    beforeCommit : function(next){
        next();
    },
    afterCommit : function(err, results, next) {
        next(err, results);
    }
};

module.exports = internals.Session = function(options){
    var self = this;

    options = options || {};

    self._repositories = {};

    self.beforeCommit = options.beforeCommit || internals.defaultOptions.beforeCommit;
    self.afterCommit = options.afterCommit || internals.defaultOptions.afterCommit;
};

internals.Session.prototype.repository = function(type, repository){
    var self = this;

    if (!repository){
        if (!self._repositories[type]) {
            throw new Error('Repository type', type, 'was not registered');
        }

        return self._repositories[type];
    }

    if (self._repositories[type]) {
        throw new Error('Repository type', type, 'already registered');
    }

    return self._repositories[type] = repository;
};

internals.Session.prototype.commit = function(callback){
    var self = this;

    self.beforeCommit(function(err){
        if (err) return callback(err);

        Async.map(Object.keys(self._repositories), function(currRepositoryKey, next){
            self._repositories[currRepositoryKey].flush(function(err, result){
                if (err){
                    err.repository = currRepositoryKey;
                }

                return next(err, result);
            });
        }, function (err, results) {
            self.afterCommit(err, results, callback);
        });
    });
};

internals.Session.prototype.rollback = function(callback){
    var self = this;

    Async.map(Object.keys(self._repositories), function(currRepositoryKey, next){
        self._repositories[currRepositoryKey].clear(function(err, result){
            if (err){
                err.repository = currRepositoryKey;
            }

            return next(err, result);
        });
    }, function (err, results) {
        return callback(err, results);
    });
};
