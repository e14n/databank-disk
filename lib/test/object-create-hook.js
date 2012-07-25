// test/object-create-hook.js
//
// Test create hooks for Databank objects
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

var objectCreateHookContext = function(driver, params) {

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
                    var Person = DatabankObject.subClass('person');
                    Person.bank = function() {
                        return bank;
                    };
                    return Person;
                },
                'which is valid': function(Person) {
                    assert.isFunction(Person);
                },
                'which has default beforeCreate()': function(Person) {
                    assert.isFunction(Person.beforeCreate);
                },
                'which has default afterCreate()': function(Person) {
                    assert.isFunction(Person.prototype.afterCreate);
                },
                'and we can create a Person': {
                    topic: function(Person, bank) {

                        var cb = this.callback,
                            called = {
                                before: false,
                                after: false,
                                person: null
                            };

                        Person.beforeCreate = function(props, callback) {
                            called.before = true;
                            props.addedByBefore = 42;
                            callback(null, props);
                        };

                        Person.prototype.afterCreate = function(callback) {
                            called.after = true;
                            this.addedByAfter = 23;
                            callback(null, this);
                        };

                        Person.create({username: "evan"}, function(err, person) {
                            if (err) {
                                cb(err, null);
                            } else {
                                called.person = person;
                                // note: not the person
                                cb(null, called);
                            }
                        });
                    },
                    teardown: function(called) {
                        if (called.person) {
                            called.person.del(function(err) {});
                        }
                    },
                    'without an error': function(err, called) {
                        assert.ifError(err);
                    },
                    'and the before hook is called': function(err, called) {
                        assert.isTrue(called.before);
                    },
                    'and the after hook is called': function(err, called) {
                        assert.isTrue(called.after);
                    },
                    'and the before hook modification happened': function(err, called) {
                        assert.equal(called.person.addedByBefore, 42);
                    },
                    'and the after hook modification happened': function(err, called) {
                        assert.equal(called.person.addedByAfter, 23);
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectCreateHookContext;
