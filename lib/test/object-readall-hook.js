// test/object-readall-hook.js
//
// Testing DatabankObject readAll() hooks
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
    Step = require('step'),
    vows = require('vows'),
    databank = require('../databank'),
    Databank = databank.Databank,
    DatabankObject = require('../databankobject').DatabankObject,
    nicknames = ['fred', 'wilma', 'pebbles', 'barney', 'betty', 'bammbamm'];

var objectReadallHookContext = function(driver, params) {

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
                'and we can create a few people': {
                    topic: function(Person) {
                        var cb = this.callback;

                        Step(
                            function() {
                                var i, group = this.group();
                                for (i = 0; i < nicknames.length; i++) {
                                    Person.create({username: nicknames[i]}, group());
                                }
                            },
                            function(err, people) {
                                cb(err, people);
                            }
                        );
                    },
                    teardown: function(people) {
                        var i;

                        for (i = 0; i < people.length; i++) {
                            people[i].del(function(err) {});
                        }
                    },
                    'it works': function(err, people) {
                        assert.ifError(err);
                        assert.isArray(people);
                    },
                    'and we read a few back': {
                        topic: function(people, Person) {
                            var cb = this.callback,
                                called = {
                                    before: 0,
                                    after: 0,
                                    people: {}
                                };

                            Person.beforeGet = function(username, callback) {
                                called.before++;
                                callback(null, username);
                            };

                            Person.prototype.afterGet = function(callback) {
                                called.after++;
                                this.addedByAfter = 23;
                                callback(null, this);
                            };

                            Person.readAll(nicknames, function(err, ppl) {
                                called.people = ppl;
                                cb(err, called);
                            });
                        },
                        'without an error': function(err, called) {
                            assert.ifError(err);
                            assert.isObject(called);
                        },
                        'and the before hook is called': function(err, called) {
                            assert.isObject(called);
                            assert.equal(called.before, nicknames.length);
                        },
                        'and the after hook is called': function(err, called) {
                            assert.equal(called.after, nicknames.length);
                        },
                        'and the after hook modification happened': function(err, called) {
                            var nick;
                            for (nick in called.people) {
                                assert.isObject(called.people[nick]);
                                assert.equal(called.people[nick].addedByAfter, 23);
                            }
                        }
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectReadallHookContext;
