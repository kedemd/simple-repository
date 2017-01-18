const Async = require('async');
const Hoek = require('hoek');
const Uuid = require('node-uuid');
const NestedError = require('nested-error-stacks');

const internals = {};

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
        return callback(null, data);
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
            var error = new NestedError("Failed to add item to the session", err);
            error.name = "extension";
            error.action = "add";
            error.step = "beforeAdd";
            error.key = key;

            return callback(error);
        }

        self._validate(data, function(validationError, validatedData){
            if(validationError){
                var error = new NestedError("Validation failed", validationError);
                error.action = "add";
                error.step = "_validate";
                error.name = "validation";
                error.key = key;

                return callback(error);
            }

            self._findItem(key, function(err, item){
                if (err) {
                    var error = new NestedError("Failed to add item to the session", err);
                    error.action = "add";
                    error.name = "internal";
                    error.step = "_findItem";
                    error.key = key;
                    return callback(error);
                }
                if (item.data) {
                    var alreadyExistError = new Error(key + " already exist");
                    alreadyExistError.action = "add";
                    alreadyExistError.name = "conflict";
                    alreadyExistError.key = key;
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
                var error = new NestedError("Failed to generate a key", err);
                error.name = "extension";
                error.action = "add";
                error.step = "generateKey";

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
            var error = new NestedError("Failed to remove item to the session", err);
            error.action = "remove";
            error.name = "extension";
            error.step = "beforeRemove";
            error.key = key;

            return callback(error);
        }

        self._findItem(key, function(err, item) {
            if (err) {
                var findError = new NestedError("Failed to remove item from the session", err);
                findError.action = "remove";
                findError.name = "internal";
                findError.step = "_findItem";
                findError.key = key;

                return callback(findError);
            }

            if (!item.data) {
                var notFound = new Error("data was not found");
                notFound.name = "remove";
                notFound.code = "notFound";
                notFound.action = "remove";
                notFound.key = key;

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
            var error = new NestedError("Failed to update item in the session", err);
            error.action = "update";
            error.step = "beforeUpdate";
            error.name = "extension";
            error.key = key;

            return callback(error);
        }

        self._validate(data, function(validationError, validatedData) {
            if (validationError) {
                var error = new NestedError("Data doesn't pass schema validation", validationError);
                error.name = "validation";
                error.step = "_validate";
                error.key = key;
                error.action = "update";

                return callback(error);
            }
            self._findItem(key, function(err, item){
                if (err) {
                    var findError = new NestedError("Failed to update item in the session", err);
                    findError.action = "update";
                    findError.step = "_findItem";
                    findError.name = "internal";

                    return callback(findError);
                }

                if (!item.action) {
                    if (!item.data){
                        var notFound = new Error("data was not found");
                        notFound.name = "notFound";
                        notFound.key = key;
                        notFound.action = "update";

                        return callback(notFound);
                    } else {
                        // Action should be update because there is data
                        item.action = "update";
                        item.original = item.data;
                    }
                } else if (item.action == "remove"){
                    var itemWasRemoved = new Error("Item was removed");
                    itemWasRemoved.name = "notFound";
                    itemWasRemoved.key = key;
                    itemWasRemoved.action = "update";

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
            var findError = new NestedError("Failed to find item from the data store", err);
            findError.name = "internal";
            findError.action = "find";

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
            var findError = new NestedError("Failed to find item in the session", err);
            findError.name = "internal";

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

/**
 * Apply repository actions to the database
 * @param {string|string[]} keys Optional key or array of keys to apply changes to the database
 * @param callback
 */
internals.Repository.prototype.flush = function(keys, callback){
    var self = this;

    callback = callback || keys;
    keys = keys == callback ? null : keys;
    if (typeof keys === 'string'){
        keys = [keys];
    }
    var targetKeys = keys || Object.keys(self._items);

    // Use the adapter to save the data to the underlying data store
    Async.map(targetKeys, function(currItemKey, next){
        var currItem = self._items[currItemKey];
        switch (currItem.action){
            case 'add' :
                self.adapter.create(currItemKey, currItem.data, function(err, item){
                    if (err) {
                        var error = new NestedError('Failed to create item while flushing', err);
                        error.key = currItemKey;
                        error.action = 'add';
                        return next(err);
                    }
                    delete currItem.action;
                    delete currItem.original;
                    return next(null, item);
                });
                break;
            case 'remove' :
                self.adapter.remove(currItemKey, function(err){
                    if (err) {
                        var error = new NestedError('Failed to remove item while flushing', err);
                        error.action = 'remove';
                        error.key = currItemKey;
                        return next(err);
                    }
                    delete self._items[currItemKey];
                    return next(null);
                });
                break;
            case 'update' :
                self.adapter.update(currItemKey, currItem.data, currItem.original, function(err, item){
                    if (err) {
                        var error = new NestedError('Failed to update item while flushing', err);
                        error.action = 'update';
                        error.key = currItemKey;

                        return next(err);
                    }
                    delete currItem.action;
                    delete currItem.original;
                    return next(null, item);
                });
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
    var self = this;
    
    self._items = {};
    return callback(null);
};



