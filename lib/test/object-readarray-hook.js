// test/object-readarray-hook.js
//
// Testing DatabankObject readArray()
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
    Step = require('step'),
    databank = require('../databank'),
    Databank = databank.Databank,
    DatabankObject = require('../databankobject').DatabankObject,
    data = [
        {name: "blender", action: "blend"},
        {name: "dishwasher", action: "wash"},
        {name: "toaster", action: "toast"},
        {name: "refrigerator", action: "refrigerate"},
        {name: "mixer", action: "mix"},
        {name: "waffle iron", action: "iron"}
    ],
    names = ['blender', 'dishwasher', 'toaster', 'refrigerator', 'mixer', 'waffle iron'];

var objectReadArrayHookContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {

        topic: function() {
            params.schema = {
                appliance: {
                    pkey: 'name'
                }
            };
            return Databank.get(driver, params);
        },
        'We can connect to it': {
            topic: function(bank) {
                bank.connect(params, this.callback);
            },
            teardown: function(bank) {
                if (bank && bank.disconnect) {
                    bank.disconnect(this.callback);
                }
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            'and we can initialize the Appliance class': {
                topic: function(bank) {
                    var Appliance;

                    Appliance = DatabankObject.subClass('appliance');

                    Appliance.bank = function() {
                        return bank;
                    };

                    return Appliance;
                },
                'which is valid': function(Appliance) {
                    assert.isFunction(Appliance);
                },
                'and we can create a few appliances': {
                    topic: function(Appliance) {
                        var cb = this.callback;

                        Step(
                            function() {
                                var i, group = this.group();
                                for (i = 0; i < data.length; i++) {
                                    Appliance.create(data[i], group());
                                }
                            },
                            function(err, appliances) {
                                cb(err, appliances);
                            }
                        );
                    },
                    teardown: function(appliances) {
                        Step(
                            function() {
                                var i, group = this.group();
                                for (i = 0; i < appliances.length; i++) {
                                    appliances[i].del(group());
                                }
                            },
                            this.callback
                        );
                    },
                    'it works': function(err, appliances) {
                        assert.ifError(err);
                        assert.isArray(appliances);
                    },
                    'and we read a few back': {
                        topic: function(appliances, Appliance) {
                            var cb = this.callback,
                                called = {
                                    before: 0,
                                    after: 0,
                                    appliances: {}
                                };

                            Appliance.beforeGet = function(name, callback) {
                                called.before++;
                                callback(null, name);
                            };

                            Appliance.prototype.afterGet = function(callback) {
                                called.after++;
                                this.addedByAfter = 23;
                                callback(null, this);
                            };

                            Appliance.readArray(names, function(err, appl) {
                                called.appliances = appl;
                                cb(err, called);
                            });
                        },
                        'without an error': function(err, called) {
                            assert.ifError(err);
                            assert.isObject(called);
                        },
                        'and the before hook is called': function(err, called) {
                            assert.isObject(called);
                            assert.equal(called.before, names.length);
                        },
                        'and the after hook is called': function(err, called) {
                            assert.equal(called.after, names.length);
                        },
                        'and the after hook modification happened': function(err, called) {
                            var i = 0;
                            assert.isArray(called.appliances);
                            assert.lengthOf(called.appliances, names.length);
                            for (i = 0; i < called.appliances.length; i++) {
                                assert.isObject(called.appliances[i]);
                                assert.equal(called.appliances[i].addedByAfter, 23);
                            }
                        }
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectReadArrayHookContext;
