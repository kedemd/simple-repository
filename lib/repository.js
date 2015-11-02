var Async = require('async');
var Hoek = require('hoek');
var Uuid = require('node-uuid');

var internals = {};

// Extension points
internals.defaultOptions = {
    adapter : {
        generateKey : function(data, callback){
            return callback(null, Uuid.v1());
        },
        create : function(key, data, callback){
            return callback(new Error("Not Implemented"));
        },
        update : function(key, data, original, callback){
            return callback(new Error("Not Implemented"));
        },
        remove : function(key, callback){
            return callback(new Error("Not Implemented"));
        },
        find : function(key, callback){
            return callback(new Error("Not Implemented"));
        }
    },
    validate : function (data, callback) {
        return callback();
    },
    beforeAdd : function (key, data, next) {
        next(null, data);
    },
    afterAdd : function (key, data, next) {
        next(null, data);
    },
    beforeValidate : function (data, next) {
        next(null, data);
    },
    afterValidate : function (data, next) {
        next(null, data);
    },
    beforeRemove : function (key, next) {
        next(null);
    },
    afterRemove : function (key, next) {
        next(null);
    },
    beforeUpdate : function (key, data, next) {
        next(null, data);
    },
    afterUpdate : function (key, data, next) {
        next(null, data);
    }
};

module.exports = internals.Repository = function(options){
    this._items = {};

    this.adapter = options.adapter || internals.defaultOptions.adapter;

    this.adapter.generateKey    = this.adapter.generateKey  || internals.defaultOptions.adapter.generateKey;
    this.adapter.create         = this.adapter.create       || internals.defaultOptions.adapter.create;
    this.adapter.update         = this.adapter.update   	|| internals.defaultOptions.adapter.update;
    this.adapter.remove         = this.adapter.remove       || internals.defaultOptions.adapter.remove;
    this.adapter.find           = this.adapter.find         || internals.defaultOptions.adapter.find;

    this.validate        	= options.validate         	    || internals.defaultOptions.validate;
    this.beforeAdd       	= options.beforeAdd        	    || internals.defaultOptions.beforeAdd;
    this.afterAdd        	= options.afterAdd         	    || internals.defaultOptions.afterAdd;
    this.beforeValidate  	= options.beforeValidate   	    || internals.defaultOptions.beforeValidate;
    this.afterValidate   	= options.afterValidate    	    || internals.defaultOptions.afterValidate;
    this.beforeRemove    	= options.beforeRemove     	    || internals.defaultOptions.beforeRemove;
    this.afterRemove     	= options.afterRemove      	    || internals.defaultOptions.afterRemove;
    this.beforeUpdate    	= options.beforeUpdate     	    || internals.defaultOptions.beforeUpdate;
    this.afterUpdate     	= options.afterUpdate      	    || internals.defaultOptions.afterUpdate;
};

internals.add = function(key, data, callback){
    var self = this;

    self.beforeAdd(key, data, function(err, data){
        if (err){
            var error = new Error("Failed to add item to the session");
            error.name = "extension";
            error.innerException = err;
            return callback(error);
        }

        self._validate(data, function(validationError, validatedData){
            if(validationError){
                var error = new Error("Data doesn't pass schema validation");
                error.name = "validation";
                error.validationError = validationError;
                return callback(error);
            }

            self._findItem(key, function(err, item){
                if (err) {
                    var error = new Error("Failed to add item to the session");
                    error.name = "internal";
                    error.innerException = err;
                    return callback(error);
                }
                if (item.data) {
                    var alreadyExistError = new Error("Item already exist");
                    alreadyExistError.name = "conflict";
                    return callback(alreadyExistError);
                }

                if (item.action == "remove"){
                    item.action = "update";
                } else {
                    item.action = "add";
                }
                item.data = validatedData;

                self.find(key, function(err, data){
                    self.afterAdd(key, data, callback);
                });
            });
        });
    });
};

internals.Repository.prototype.add = function(key, data, callback){
    var self = this;

    if (key) {
        internals.add.call(self, key, data, callback);
    } else {
        self.adapter.generateKey(data, function(err, key){
            if (err) {
                var error = new Error("Failed to generate a key");
                error.name = "extension";
                error.innerException = err;
                return callback(error);
            }

            internals.add.call(self, key, data, callback);
        });
    }
};

internals.Repository.prototype.remove = function(key, callback){
    var self = this;

    self.beforeRemove(key, function(err){
        if (err) {
            var error = new Error("Failed to remove item to the session");
            error.name = "extension";
            error.innerException = err;
            return callback(error);
        }

        self._findItem(key, function(err, item) {
            if (err) {
                var findError = new Error("Failed to remove item from the session");
                findError.name = "internal";
                findError.innerException = err;
                return callback(findError);
            }

            if (!item.data) {
                var notFound = new Error("data was not found");
                notFound.name = "notFound";
                return callback(notFound);
            }

            if (!item.action || item.action == "update") {
                item.action = "remove";
            } else if (item.action == "add") {
                delete item.action;
            }
            delete item.data;

            return self.find(key, function(err, data){
                if (err) return callback(err);
                self.afterRemove(key, callback);
            });
        });
    });
};

internals.Repository.prototype.update = function(key, data, callback){
    var self = this;

    // Set the data after validation had passed
    self.beforeUpdate(key, data, function(err, data){
        if (err){
            var error = new Error("Failed to update item in the session");
            error.name = "extension";
            error.innerException = err;
            return callback(error);
        }

        self._validate(data, function(validationError, validatedData) {
            if (validationError) {
                var error = new Error("Data doesn't pass schema validation");
                error.name = "validation";
                error.validationError = validationError;
                return callback(error);
            }
            self._findItem(key, function(err, item){
                if (err) {
                    var findError = new Error("Failed to update item in the session");
                    findError.name = "internal";
                    findError.innerException = err;
                    return callback(findError);
                }

                if (!item.action) {
                    if (!item.data){
                        var notFound = new Error("data was not found");
                        notFound.name = "notFound";
                        return callback(notFound);
                    } else {
                        // Action should be update because there is data
                        item.action = "update";
                        item.original = item.data;
                    }
                } else if (item.action == "remove"){
                    var itemWasRemoved = new Error("Item was removed");
                    itemWasRemoved.name = "notFound";
                    return callback(itemWasRemoved);
                } else if (item.action != "add"){
                    item.action = "update";
                }

                item.data = validatedData;

                self.find(key, function(err, data){
                    if (err) return callback(err);
                    self.afterUpdate(key, data, callback);
                });
            });
        });
    });

};

internals.Repository.prototype._findItem = function(key, callback){
    var self = this;

    var item = self._items[key];
    if (item) return callback(null, item);

    self.adapter.find(key, function(err, data){
        if (err){
            var findError = new Error("Failed to find item from the data store");
            findError.name = "internal";
            findError.innerException = err;
            return callback(findError);
        }

        item = self._items[key] = { data : data };
        return callback(null, item);
    });
};

internals.Repository.prototype.find = function(key, callback){
    var self = this;

    self._findItem(key, function(err, item){
        if (err){
            var findError = new Error("Failed to find item in the session");
            findError.name = "internal";
            findError.innerException = err;
            return callback(findError);
        }

        return callback(null, Hoek.clone(item.data));
    });
};

internals.Repository.prototype._validate = function(data, callback){
    var self = this;

    self.beforeValidate(data, function(err, preValidatedData){
        if (err) return callback(err);

        self.validate(preValidatedData, function(err, validatedData){
            if (err) return callback(err);

            return self.afterValidate(validatedData, callback);
        });
    });
};

internals.Repository.prototype.flush = function(callback){
    var self = this;

    // Use the adapter to save the data to the underlying data store
    Async.map(Object.keys(self._items), function(currItemKey, next){
        var currItem = self._items[currItemKey];
        switch (currItem.action){
            case 'add' :
                self.adapter.create(currItemKey, currItem.data, next);
                break;
            case 'remove' :
                self.adapter.remove(currItemKey, next);
                break;
            case 'update' :
                self.adapter.update(currItemKey, currItem.data, currItem.original, next);
                break;
            default :
                next();
                break;
        }
    }, function(err, results){
        return callback(err, results);
    });
};

internals.Repository.prototype.clear = function(callback){
    self._items = {};
    return callback(null);
};



