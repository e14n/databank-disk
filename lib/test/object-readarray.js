// test/object-readarray.js
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

var vows = require('vows'),
    assert = require('assert'),
    databank = require('../databank'),
    Databank = databank.Databank,
    Step = require('step'),
    DatabankObject = require('../databankobject').DatabankObject;

var data = [
    {abbr: 'MA', name: 'Massachussetts'},
    {abbr: 'CA', name: 'California'},
    {abbr: 'NY', name: 'New York'},
    {abbr: 'MO', name: 'Missouri'},
    {abbr: 'WY', name: 'Wyoming'} 
];

var ids = ['CA', 'NY', 'MO', 'invalid'];

var objectReadarrayContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {
        topic: function() {

            var bank;

            params.schema = {
                state: {
                    pkey: 'abbr'
                }
            };
            bank = Databank.get(driver, params);
            return bank;
        },
        'We can connect to it': {
            topic: function(bank) {
                DatabankObject.bank = bank;
                bank.connect(params, this.callback);
            },
            teardown: function(bank) {
                if (bank && bank.disconnect) {
                    bank.disconnect(function(err) {});
                }
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            'and we can initialize the State class': {
                topic: function(bank) {
                    var State = DatabankObject.subClass('state');

                    // Override so there's not a global causing grief.

                    State.bank = function() {
                        return bank;
                    };
                    
                    return State;
                },
                'and we can create some states': {
                    topic: function(State, bank) {
                        var that = this;
                        Step(
                            function() {
                                var i, group = this.group();
                                for (i = 0; i < data.length; i++) {
                                    State.create(data[i], group());
                                }
                            },
                            function(err, states) {
                                that.callback(err, states);
                            }
                        );
                    },
                    'without an error': function(err, states) {
                        assert.ifError(err);
                    },
                    'and we can read a few of them': {
                        topic: function(states, State, bank) {
                            State.readArray(ids, this.callback);
                        },
                        'without an error': function(err, statesArray) {
                            var i;
                            assert.ifError(err);
                            assert.isArray(statesArray);
                            assert.lengthOf(statesArray, ids.length);
                            for (i = 0; i < 3; i++) {
                                assert.isObject(statesArray[i]);
                                assert.equal(statesArray[i].abbr, ids[i]);
                            }
                            assert.isNull(statesArray[3]);
                        }
                    },
                    teardown: function(states) {
                        var that = this;
                        Step(
                            function() {
                                var i, group = this.group();
                                for (i = 0; i < states.length; i++) {
                                    states[i].del(group());
                                }
                            },
                            function(err) {
                                that.callback(err);
                            }
                        );
                    }
                }
            }
        }
    };
    
    return context;
};

module.exports = objectReadarrayContext;
