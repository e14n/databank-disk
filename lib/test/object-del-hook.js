// test/object-del-hook.js
//
// Test del hooks for Databank objects
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
    Databank = databank.Databank,
    DatabankObject = require('../databankobject').DatabankObject;

var objectDelHookContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {

        topic: function() {
            params.schema = {
                person: {
                    pkey: 'username'
                }
            };
            return Databank.get(driver, params);
        },
        'We can connect to it': {
            topic: function(bank) {
                bank.connect(params, this.callback);
            },
            teardown: function(bank) {
                bank.disconnect(function(err) {});
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            'and we can initialize the Person class': {
                topic: function(bank) {
                    var Person;

                    DatabankObject.bank = bank;

                    Person = DatabankObject.subClass('person');

                    return Person;
                },
                'which is valid': function(Person) {
                    assert.isFunction(Person);
                },
                'and we can create and delete a Person': {
                    topic: function(Person) {

                        var cb = this.callback,
                            called = {
                                before: false,
                                after: false
                            };

                        Person.prototype.beforeDel = function(callback) {
                            called.before = true;
                            callback(null);
                        };

                        Person.prototype.afterDel = function(callback) {
                            called.after = true;
                            callback(null);
                        };

                        Person.create({username: "evan"}, function(err, person) {
                            if (err) {
                                cb(err, null);
                            } else {
                                person.del(function(err) {
                                    if (err) {
                                        cb(err, null);
                                    } else {
                                        // note: not the person
                                        cb(null, called);
                                    }
                                });
                            }
                        });
                    },
                    'without an error': function(err, called) {
                        assert.ifError(err);
                        assert.isObject(called);
                    },
                    'and the before hook is called': function(err, called) {
                        assert.isTrue(called.before);
                    },
                    'and the after hook is called': function(err, called) {
                        assert.isTrue(called.after);
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectDelHookContext;
