// test/object-readall.js
//
// Testing DatabankObject readAll()
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

var vows = require('vows'),
    assert = require('assert'),
    databank = require('../databank'),
    Databank = databank.Databank,
    Step = require('step'),
    DatabankObject = require('../databankobject').DatabankObject;

var objectReadallContext = function(driver, params) {

    var context = {};
    var bank = null;
    var people = null; 

    var data = [
        {name: "Mercury", moons: 0},
        {name: "Venus", moons: 0},
        {name: "Earth", moons: 1},
        {name: "Mars", moons: 2},
        {name: "Jupiter", moons: 66},
        {name: "Saturn", moons: 62},
        {name: "Uranus", moons: 27},
        {name: "Neptune", moons: 13}
    ];

    var ids = ['Venus', 'Mars', 'Saturn', 'invalid'];

    var Planet = DatabankObject.subClass('planet');

    // Override so there's not a global causing grief.

    Planet.bank = function() {
        return bank;
    };

    context["When we create a " + driver + " databank"] = {
        topic: function() {
            params.schema = {
                planet: {
                    pkey: 'name'
                }
            };
            bank = Databank.get(driver, params);
            return bank;
        },
        'We can connect to it': {
            topic: function() {
                bank.connect(params, this.callback);
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            teardown: function() {
                var callback = this.callback;
                Step(
                    function() {
                        var i, group = this.group();
                        for (i = 0; i < data.length; i++) {
                            bank.del('planet', data[i].name, group());
                        }
                    },
                    function(err) {
                        if (err) throw err;
                        bank.disconnect(this);
                    },
                    callback
                );
            },
            'and we can create some people': {
                topic: function() {
                    var that = this;
                    Step(
                        function() {
                            var i, group = this.group();
                            for (i = 0; i < data.length; i++) {
                                Planet.create(data[i], group());
                            }
                        },
                        function(err, ppl) {
                            people = ppl;
                            that.callback(err, ppl);
                        }
                    );
                },
                'without an error': function(err, ppl) {
                    assert.ifError(err);
                },
                'and we can read a few of them': {
                    topic: function() {
                        Planet.readAll(ids, this.callback);
                    },
                    'without an error': function(err, pplMap) {
                        assert.ifError(err);
                        assert.isObject(pplMap);
                        assert.isObject(pplMap.Venus);
                        assert.isObject(pplMap.Mars);
                        assert.isObject(pplMap.Saturn);
                        assert.isNull(pplMap.invalid);
                    }
                }
            }
        }
    };

    return context;
};

module.exports = objectReadallContext;
