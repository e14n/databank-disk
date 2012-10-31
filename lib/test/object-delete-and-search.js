// test/object.js
//
// Testing DatabankObject basic functionality
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
    databank = require('../databank'),
    Step = require('step'),
    Databank = databank.Databank,
    NoSuchThingError = databank.NoSuchThingError,
    DatabankObject = require('../databankobject').DatabankObject;

var objectContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {

        topic: function() {
            if (!params.hasOwnProperty('schema')) {
                params.schema = {};
            }
            params.schema.person = {
                pkey: 'username',
                fields: ['name'],
                indices: ['name.last']
            };
            return Databank.get(driver, params);
        },
        'We can connect to it': {
            topic: function(bank) {
                bank.connect(params, this.callback);
            },
            teardown: function(bank) {
                var callback = this.callback;
                // Workaround for vows bug
                process.nextTick(function() {
                    bank.disconnect(function(err) {
                        callback();
                    });
                });
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            'and we can initialize the Person class': {
                topic: function(bank) {
                    var Person = DatabankObject.subClass('person');
                    Person.bank = function() {
                        return bank;
                    };
                    return Person;
                },
                'which is valid': function(Person) {
                    assert.ok(Person);
                },
                'and we create and delete a Person': {
                    topic: function(Person, bank) {
                        var callback = this.callback,
                            maj = {username: 'maj',
                                   name: {last: 'Jenkins', first: 'Michele'}};

                        Step(
                            function() {
                                Person.create(maj, this);
                            },
                            function(err, maj) {
                                if (err) throw err;
                                maj.del(this);
                            },
                            callback
                        );
                    },
                    "it works": function(err) {
                        assert.ifError(err);
                    },
                    "and we try to get the Person": {
                        topic: function(Person, bank) {
                            var callback = this.callback;
                            Person.get("maj", function(err, p) {
                                if (err && err instanceof NoSuchThingError) {
                                    callback(null);
                                } else if (err) {
                                    callback(err);
                                } else {
                                    callback(new Error("Unexpected success"));
                                }
                            });
                        },
                        "it returns a NoSuchThingError": function(err) {
                            assert.ifError(err);
                        }
                    },
                    "and we search for the Person": {
                        topic: function(Person, bank) {
                            Person.search({"name.last": "Jenkins"}, this.callback);
                        },
                        "it works": function(err, results) {
                            assert.ifError(err);
                            assert.isArray(results);
                        },
                        "there are no results": function(err, results) {
                            assert.ifError(err);
                            assert.isArray(results);
                            assert.lengthOf(results, 0);
                        }
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectContext;
