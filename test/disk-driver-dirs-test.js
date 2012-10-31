// disk-driver-test.js
//
// Testing the disk driver
//
// Copyright 2012, StatusNet Inc.
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

var assert = require('assert'),
    vows = require('vows'),
    databank = require('../lib/index'),
    Databank = databank.Databank,
    os = require('os'),
    fs = require('fs'),
    path = require('path');

var dir = path.join(os.tmpDir(), "/disk-driver-dirs-test");

var schema = {
    person: {
        pkey: "email"
    },
    house: {
        pkey: "streetAddress",
        indices: ["neighborhood", "owner.lastName"]
    },
    car: {
        pkey: "vin",
        indices: ["licensePlate", "model"]
    }
};

var haveDir = function(rel) {
    return {
        topic: function(db) {
            fs.stat(path.join(db.dir, rel), this.callback);
        },
        "it exists": function(err, stat) {
            assert.ifError(err);
            assert.isTrue(stat.isDirectory());
        }
    };
};

var suite = vows.describe('disk directories');

suite.addBatch({
    'When we create a disk databank': {
        topic: function() {
            return Databank.get("disk", {dir: dir, schema: schema});
        },
        "it works": function(db) {
            assert.ok(db);
        },
        "and we connect": {
            topic: function(db) {
                db.connect({}, this.callback);
            },
            teardown: function(db) {
                db.disconnect(function(err) {});
            },
            "it works": function(err) {
                assert.ifError(err);
            },
            "and we check the main directory": haveDir(""),
            "and we check the person directory": haveDir("person"),
            "and we check the house directory": haveDir("house"),
            "and we check the car directory": haveDir("car")
        }
    }
});

suite.addBatch({
    'When we create a disk databank': {
        topic: function() {
            return Databank.get("disk", {mktmp: true, schema: schema});
        },
        "it works": function(db) {
            assert.ok(db);
        },
        "and we connect": {
            topic: function(db) {
                db.connect({}, this.callback);
            },
            teardown: function(db) {
                db.disconnect(function(err) {});
            },
            "it works": function(err) {
                assert.ifError(err);
            },
            "and we check the main directory": haveDir(""),
            "and we check the person directory": haveDir("person"),
            "and we check the house directory": haveDir("house"),
            "and we check the car directory": haveDir("car")
        }
    }
});

suite['export'](module);
