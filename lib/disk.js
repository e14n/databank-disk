// diskdatabank.js
//
// On-disk implementation of Databank interface
//
// Copyright 2011,2012 StatusNet Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var databank = require('../databank'),
    rimraf = require('rimraf'),
    Step = require('step'),
    mkdirp = require('mkdirp'),
    Queue = require('jankyqueue'),
    Schlock = require('schlock'),
    Databank = databank.Databank,
    DatabankError = databank.DatabankError,
    AlreadyExistsError = databank.AlreadyExistsError,
    NoSuchThingError = databank.NoSuchThingError,
    NotConnectedError = databank.NotConnectedError,
    AlreadyConnectedError = databank.AlreadyConnectedError,
    crypto = require('crypto'),
    fs = require('fs-ext'),
    path = require('path'),
    os = require('os');

var DiskDatabank = function(params) {
    this.mktmp = params.mktmp || false;
    this.dir = params.dir || '/var/lib/diskdatabank/';
    this.mode = params.mode || 0660;
    this.hashDepth = params.hashDepth || 3;
    this.schema = params.schema || {};
    this.noLock = params.noLock || false; // Optionally lock
    this.queueSize = params.queueSize || 32;
    this.q = new Queue(this.queueSize);
    this.connected = false;
};

// All banks use same lock manager

DiskDatabank.schlock = new Schlock();

DiskDatabank.prototype = new Databank();

DiskDatabank.prototype.toFilename = function(type, id) {
    var hash = this.toHash(id);

    return path.join(this.toDirname(type, id), hash + '.json');
};

DiskDatabank.prototype.toHash = function(id) {

    var hash = crypto.createHash('md5'),
        str, data;

    data = "" + id;

    hash.update(data);
    str = hash.digest('base64');

    // Make it a little more FS-safe

    str = str.replace(/\+/g, '-');
    str = str.replace(/\//g, '_');
    str = str.replace(/=/g, '');

    return str;
};

DiskDatabank.prototype.toDirname = function(type, id) {
    var n;
    var dirname = path.join(this.dir, type);
    var hash = this.toHash(id);

    for (n = 0; n < Math.min(this.hashDepth, hash.length); n++) {
        dirname = path.join(dirname, hash.substr(0, n + 1));
    }

    return dirname;
};

DiskDatabank.prototype.connect = function(params, onCompletion) {
    var bank = this,
        checkTree = function() {
            bank.ensureTree(function(err) {
                if (err) {
                    onCompletion(err);
                } else {
                    bank.connected = true;
                    onCompletion(null);
                }
            });
        };

    if (bank.connected) {
        onCompletion(new AlreadyConnectedError());
        return;
    }

    if (bank.mktmp) {
        bank.makeTempDir(function(err) {
            if (err) {
                onCompletion(err);
            } else {
                checkTree();
            }
        });
    } else {
        checkTree();
    }
};

DiskDatabank.prototype.makeTempDir = function(callback) {
    var bank = this,
        tmp = os.tmpDir(),
        tryAgain = function() {
            randomString(16, function(err, rs) {
                var dirname;
                if (err) {
                    callback(err);
                } else {
                    dirname = path.join(tmp, rs);
                    bank.q.enqueue(fs.stat, [dirname], function(err, stats) {
		        if (err && err.code == 'ENOENT') {
                            bank.dir = dirname;
                            callback(null);
                        } else if (err) {
                            callback(err);
                        } else {
                            // XXX: bounded retries; 10?
                            tryAgain();
                        }
                    });
                }
            });
        },
        randomString = function(bytes, cb) {
            crypto.randomBytes(bytes, function(err, buf) {
                var str;
                if (err) {
                    cb(err, null);
                } else {
                    str = buf.toString("base64");

                    str = str.replace(/\+/g, "-");
                    str = str.replace(/\//g, "_");
                    str = str.replace(/=/g, "");

                    cb(null, str);
                }
            });
        };


    tryAgain();
};

DiskDatabank.prototype.disconnect = function(onCompletion) {
    var bank = this;
    
    if (!bank.connected) {
        onCompletion(new NotConnectedError());
        return;
    }
    
    if (bank.mktmp) {
        bank.q.enqueue(rimraf, [bank.dir], function(err) {
            bank.connected = false;
            onCompletion(err);
        });
    } else {
        bank.connected = false;
        onCompletion(null);
    }
};

DiskDatabank.prototype.create = function(type, id, value, callback) {

    var bank = this,
        dirname = this.toDirname(type, id),
        filename = this.toFilename(type, id),
        fd = null,
        schlocked = false;

    if (!bank.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.ensureDir(dirname, this);
        },
        function(err) {
            if (err) throw err;
            DiskDatabank.schlock.writeLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.stat, [filename], this);
        },
        function(err, stats) {
            if (err && (!err.code || err.code != 'ENOENT')) throw err;
            if (!err) throw new AlreadyExistsError(type, id);
            bank.q.enqueue(fs.open, [filename, 'wx', bank.mode], this);
        },
        function(err, result) {
            if (err) throw err; // Check for O_EXCL err and make into AlreadyExistsError?
            fd = result;
            // XXX: skip locking if bank.noLock
            bank.q.enqueue(fs.flock, [fd, 'ex'], this);
        },
        function(err) {
            var b;
            if (err) throw err;
            b = new Buffer(JSON.stringify(value), 'utf8');
            bank.q.enqueue(fs.write, [fd, b, 0, b.length, null], this);
        },
        function(err, written, buffer) {
            if (err) throw err;
            bank.q.enqueue(fs.close, [fd], this);
        },
        function(err) {
            if (err) throw err;
            fd = null;
            DiskDatabank.schlock.writeUnlock(filename, this);
        },
        function(err) {
            if (err) {
                if (fd) {
                    bank.q.enqueue(fs.close, [fd], function(err2) {
                        DiskDatabank.schlock.writeUnlock(filename, function(err3) {
                            callback(err, null);
                        });
                    });
                } else if (schlocked) {
                    DiskDatabank.schlock.writeUnlock(filename, function(err2) {
                        callback(err, null);
                    });
                } else {
                    callback(err, null);
                }
            } else {
                callback(null, value);
            }
        }
    );
};

DiskDatabank.prototype.save = function(type, id, value, callback)
{
    var bank = this,
        dirname = this.toDirname(type, id),
        filename = this.toFilename(type, id),
        fd,
        schlocked = false;

    if (!this.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            bank.ensureDir(dirname, this);
        },
        function(err) {
            if (err) throw err;
            DiskDatabank.schlock.writeLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.open, [filename, 'w', bank.mode], this);
        },
        function(err, result) {
            if (err) throw err;
            fd = result;
            // XXX: skip locking if bank.noLock
            bank.q.enqueue(fs.flock, [fd, 'ex'], this);
        },
        function(err) {
            if (err) throw err;
            // Locked; now we truncate
            bank.q.enqueue(fs.truncate, [fd, 0], this);
        },
        function(err) {
            var b;
            if (err) throw err;
            b = new Buffer(JSON.stringify(value), 'utf8');
            bank.q.enqueue(fs.write, [fd, b, 0, b.length, null], this);
        },
        function(err, written, buffer) {
            if (err) throw err;
            bank.q.enqueue(fs.close, [fd], this);
        },
        function(err) {
            if (err) throw err;
            fd = null;
            DiskDatabank.schlock.writeUnlock(filename, this);
        },
        function(err) {
            if (err) {
                if (fd) {
                    bank.q.enqueue(fs.close, [fd], function(err2) {
                        DiskDatabank.schlock.writeUnlock(filename, function(err3) {
                            callback(err, null);
                        });
                    });
                } else if (schlocked) {
                    DiskDatabank.schlock.writeUnlock(filename, function(err2) {
                        callback(err, null);
                    });
                } else {
                    callback(err, null);
                }
            } else {
                callback(null, value);
            }
        }
    );
};

DiskDatabank.prototype.update = function(type, id, value, callback) {
    var bank = this,
        filename = this.toFilename(type, id),
        fd,
        schlocked = false;

    if (!this.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            DiskDatabank.schlock.writeLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.stat, [filename], this);
        },
        function(err, stats) {
            if (err) {
                if (err.code == 'ENOENT') {
                    throw new NoSuchThingError(type, id);
                } else {
                    throw err;
                }
            }
            // open in r+ so we don't truncate existing file before locking
            bank.q.enqueue(fs.open, [filename, 'r+', bank.mode], this);
        },
        function(err, result) {
            if (err) throw err;
            fd = result;
            // XXX: skip locking if bank.noLock
            bank.q.enqueue(fs.flock, [fd, 'ex'], this);
        },
        function(err) {
            if (err) throw err;
            // Locked; now we truncate
            bank.q.enqueue(fs.truncate, [fd, 0], this);
        },
        function(err) {
            var b;
            if (err) throw err;
            b = new Buffer(JSON.stringify(value), 'utf8');
            bank.q.enqueue(fs.write, [fd, b, 0, b.length, null], this);
        },
        function(err, written, buffer) {
            if (err) throw err;
            bank.q.enqueue(fs.close, [fd], this);
        },
        function(err) {
            if (err) throw err;
            fd = null;
            DiskDatabank.schlock.writeUnlock(filename, this);
        },
        function(err) {
            if (err) {
                if (fd) {
                    bank.q.enqueue(fs.close, [fd], function(err2) {
                        DiskDatabank.schlock.writeUnlock(filename, function(err3) {
                            callback(err, null);
                        });
                    });
                } else if (schlocked) {
                    DiskDatabank.schlock.writeUnlock(filename, function(err2) {
                        callback(err, null);
                    });
                } else {
                    callback(err, null);
                }
            } else {
                callback(null, value);
            }
        }
    );
};

DiskDatabank.prototype.read = function(type, id, callback) {
    var bank = this,
        filename = this.toFilename(type, id),
        fsize,
        buf,
        fd,
        schlocked = false;

    if (!this.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            DiskDatabank.schlock.readLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.open, [filename, 'r', bank.mode], this);
        },
        function(err, result) {
            if (err) {
                if (err.code == 'ENOENT') {
                    throw new NoSuchThingError(type, id);
                } else {
                    throw err;
                }
            }
            fd = result;
            bank.q.enqueue(fs.flock, [fd, 'sh'], this);
        },
        function(err) {
            if (err) throw err;
            bank.q.enqueue(fs.seek, [fd, 0, 2], this);
        },
        function(err, pos) {
            if (err) throw err;
            fsize = pos;
            buf = new Buffer(fsize);
            bank.q.enqueue(fs.read, [fd, buf, 0, fsize, 0], this);
        },
        function(err, count, b) {
            var parsed;
            if (err) throw err;
            bank.q.enqueue(fs.close, [fd], this);
        },
        function(err) {
            if (err) throw err;
            fd = null;
            DiskDatabank.schlock.readUnlock(filename, this);
        },
        function(err) {
            var parsed;
            if (err) {
                if (fd) {
                    bank.q.enqueue(fs.close, [fd], function(err2) {
                        DiskDatabank.schlock.readUnlock(filename, function(err3) {
                            callback(err, null);
                        });
                    });
                } else if (schlocked) {
                    DiskDatabank.schlock.readUnlock(filename, function(err2) {
                        callback(err, null);
                    });
                } else {
                    callback(err, null);
                }
            } else {
                try {
                    parsed = JSON.parse(buf.toString('utf8'));
                    callback(null, parsed);
                } catch (e) {
                    callback(e, null);
                }
            }
        }
    );
};

DiskDatabank.prototype.del = function(type, id, callback) {
    var bank = this,
        filename = this.toFilename(type, id),
        schlocked = false;

    if (!this.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            DiskDatabank.schlock.writeLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.unlink, [filename], this); 
        },
        function(err) {
            if (err) throw err;
            DiskDatabank.schlock.writeUnlock(filename, this);
        },
        function(err) {
            if (err) {
                if (err.code == 'ENOENT') {
                    err = new NoSuchThingError(type, id);
                }
                if (schlocked) {
                    DiskDatabank.schlock.writeUnlock(filename, function(err2) {
                        callback(err);
                    });
                } else {
                    callback(err);
                }
            } else {
                callback(null);
            }
        }
    );
};

DiskDatabank.prototype.ensureDir = function(dir, callback) {

    var bank = this;

    Step(
        function() {
	    bank.q.enqueue(fs.stat, [dir], this);
        },
        function(err, stat) {
            if (err) {
                if (err.code == 'ENOENT') {
                    bank.q.enqueue(mkdirp, [dir, (bank.mode | 0111)], this);
                } else {
                    throw err;
                }
            } else {
                // no error
                if (stat.isDirectory()) {
                    callback(null);
                } else {
                    callback(new Error(dir + " exists and is not a directory."));
                }
            }
        },
        function(err, made) {
            if (err) {
                callback(err);
            } else {
                callback(null);
            }
        }
    );
};

DiskDatabank.prototype.search = function(type, criteria, onResult, onCompletion) {
    var bank = this,
	counter = 0,
        checkOrWalk = function(path, callback) {
            Step(
                function() {
		    bank.q.enqueue(fs.stat, [path], this);
                },
                function(err, stat) {
                    if (err) {
                        if (err.code == 'ENOENT') {
                            // silently ignore missing directory entries
                            callback(null);
                        } else {
                            throw err;
                        }
                    } else if (stat.isDirectory()) {
			walk(path, this);
                    } else {
                        check(path, this);
                    }
                },
                callback
            );
        },
	walk = function(dirname, callback) { // originally from https://gist.github.com/514983
            Step(
                function() {
	            bank.q.enqueue(fs.readdir, [dirname], this);
                },
                function(err, relnames) {
                    var group = this.group(), i;
		    if (err) throw err;
                    for (i = 0; i < relnames.length; i++) {
                        checkOrWalk(path.join(dirname, relnames[i]), group());
                    }
		},
                callback
            );
        },
        check = function(path, callback) {
            var fd, buf, fsize, schlocked = false;
            Step(
                function() {
                    DiskDatabank.schlock.readLock(path, this);
                },
                function(err) {
                    if (err) throw err;
                    schlocked = true;
                    bank.q.enqueue(fs.open, [path, 'r', bank.mode], this);
                },
                function(err, result) {
                    if (err) throw err;
                    fd = result;
                    bank.q.enqueue(fs.flock, [fd, 'sh'], this);
                },
                function(err) {
                    if (err) throw err;
                    bank.q.enqueue(fs.seek, [fd, 0, 2], this);
                },
                function(err, pos) {
                    if (err) throw err;
                    fsize = pos;
                    buf = new Buffer(fsize);
                    bank.q.enqueue(fs.read, [fd, buf, 0, fsize, 0], this);
                },
                function(err, count, b) {
                    if (err) throw err;
                    bank.q.enqueue(fs.close, [fd], this);
                },
                function(err) {
                    if (err) throw err;
                    fd = null;
                    DiskDatabank.schlock.readUnlock(path, this);
                },
                function(err) {
                    var parsed;
                    if (err) {
                        if (fd) {
                            bank.q.enqueue(fs.close, [fd], function(err2) {
                                DiskDatabank.schlock.readUnlock(path, function(err3) {
                                    callback(err, null);
                                });
                            });
                        } else if (schlocked) {
                            DiskDatabank.schlock.readUnlock(path, function(err2) {
                                callback(err, null);
                            });
                        } else {
                            callback(err, null);
                        }
                    } else {
                        try {
                            parsed = JSON.parse(buf.toString('utf8'));
		            if (bank.matchesCriteria(parsed, criteria)) {
			        onResult(parsed);
		            }
                            callback(null);
                        } catch (e) {
                            callback(e);
                        }
                    }
                }
	    );
	};

    if (!this.connected) {
        onCompletion(new NotConnectedError());
        return;
    }
    
    walk(path.join(bank.dir, type), onCompletion);
};

DiskDatabank.prototype.ensureTree = function(callback) {
    var bank = this;
    
    Step(
        function() {
            bank.ensureDir(bank.dir, this);
        },
        function(err) {
            var type, group = this.group();
            if (err) throw err;
            for (type in bank.schema) {
                if (bank.schema.hasOwnProperty(type)) {
                    bank.ensureDir(path.join(bank.dir, type), group());
                }
            }
        },
        function(err) {
            callback(err);
        }
    );
};

DiskDatabank.prototype.readAndModify = function(type, id, def, modify, callback) {
    var bank = this,
        filename = this.toFilename(type, id),
        fd,
        fsize,
        buf,
        newValue,
        schlocked = false;

    if (!this.connected) {
        callback(new NotConnectedError());
        return;
    }

    Step(
        function() {
            DiskDatabank.schlock.writeLock(filename, this);
        },
        function(err) {
            if (err) throw err;
            schlocked = true;
            bank.q.enqueue(fs.open, [filename, 'r+', bank.mode], this);
        },
        function(err, result) {
            if (err) {
                if (err.code == 'ENOENT') {
                    DiskDatabank.schlock.writeUnlock(filename, function(err) {
                        bank.create(type, id, def, callback);
                    });
                    return;
                } else {
                    throw err;
                }
            }
            fd = result;
            bank.q.enqueue(fs.flock, [fd, 'ex'], this);
        },
        function(err) {
            if (err) throw err;
            bank.q.enqueue(fs.seek, [fd, 0, 2], this);
        },
        function(err, pos) {
            if (err) throw err;
            fsize = pos;
            buf = new Buffer(fsize);
            bank.q.enqueue(fs.read, [fd, buf, 0, fsize, 0], this);
        },
        function(err, count, b) {
            var oldValue;
            if (err) throw err;
            // May throw an error; let it!
            oldValue = JSON.parse(buf.toString('utf8'));
            newValue = modify(oldValue);
            bank.q.enqueue(fs.truncate, [fd, 0], this);
        },
        function(err) {
            var newBuf;
            if (err) throw err;
            newBuf = new Buffer(JSON.stringify(newValue), 'utf8');
            bank.q.enqueue(fs.write, [fd, newBuf, 0, newBuf.length, 0], this);
        },
        function(err) {
            if (err) throw err;
            bank.q.enqueue(fs.close, [fd], this);
        },
        function(err) {
            if (err) throw err;
            fd = null;
            DiskDatabank.schlock.writeUnlock(filename, this);
        },
        function(err) {
            if (err) {
                if (fd) {
                    bank.q.enqueue(fs.close, [fd], function(err2) {
                        DiskDatabank.schlock.readUnlock(path, function(err3) {
                            callback(err, null);
                        });
                    });
                } else if (schlocked) {
                    DiskDatabank.schlock.readUnlock(path, function(err2) {
                        callback(err, null);
                    });
                } else {
                    callback(err, null);
                }
            } else {
                callback(null, newValue);
            }
        }
    );
};


module.exports = DiskDatabank;
