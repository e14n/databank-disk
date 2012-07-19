// test/search.js
//
// Testing search() method
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
    Databank = databank.Databank;

var searchContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {

        topic: function() {
            params.schema = {
                person: {
                    pkey: 'username',
                    indices: ['name.last']
                }
            };
            return Databank.get(driver, params);
        },
        'We can connect to it': {
            topic: function(bank) {
                bank.connect(params, this.callback);
            },
            'without an error': function(err) {
                assert.ifError(err);
            },
            'and we can add a person': {
                topic: function(bank) {
                    bank.create('person', 'evanp', {name: {last: 'Prodromou', first: 'Evan'}, age: 43}, this.callback);
                },
                'without an error': function(err, person) {
                    assert.ifError(err);
                },
                'and we can add another person': {
                    topic: function(evan, bank) {
                        bank.create('person', 'stav', {name: {last: 'Prodromou', first: 'Stav'}, age: 67}, this.callback);
                    },
                    'without an error': function(err, person) {
                        assert.ifError(err);
                    },
                    'and we can add yet another person': {
                        topic: function(stav, evan, bank) {
                            bank.create('person', 'abe', {name: {last: 'Lincoln', first: 'Abraham'}, age: 202}, this.callback);
                        },
                        'without an error': function(err, person) {
                            assert.ifError(err);
                        },
                        'and we can search by an indexed value': {
                            topic: function(abe, stav, evan, bank) {
                                var results = [], onResult = function(result) { results.push(result); };

                                bank.search('person', {'name.last': 'Prodromou'}, onResult, this.callback);
                            },
                            'without an error': function(err) {
                                assert.ifError(err);
                            },
                            'and we can search by an non-indexed value': {
                                topic: function(abe, stav, evan, bank) {
                                    var results = [], onResult = function(result) { results.push(result); };

                                    bank.search('person', {'age': 43}, onResult, this.callback);
                                },
                                'without an error': function(err) {
                                    assert.ifError(err);
                                },
				'and we can search with no expected results': {
                                    topic: function(abe, stav, evan, bank) {
					var results = [], onResult = function(result) { results.push(result); };

					bank.search('person', {'age': -1}, onResult, this.callback);
                                    },
                                    'without an error': function(err) {
					assert.ifError(err);
                                    },
                                    'and we can delete the last person': {
					topic: function(abe, stav, evan, bank) {
                                            bank.del('person', 'abe', this.callback);
					},
					'without an error': function(err) {
                                            assert.ifError(err);
					},
					'and we can delete the second person': {
                                            topic: function(abe, stav, evan, bank) {
						bank.del('person', 'stav', this.callback);
                                            },
                                            'without an error': function(err) {
						assert.ifError(err);
                                            },
                                            'and we can delete the first person': {
						topic: function(abe, stav, evan, bank) {
                                                    bank.del('person', 'evanp', this.callback);
						},
						'without an error': function(err) {
                                                    assert.ifError(err);
						},
						'and we can disconnect': {
                                                    topic: function(abe, stav, evan, bank) {
							bank.disconnect(this.callback);
                                                    },
                                                    'without an error': function(err) {
							assert.ifError(err);
                                                    }
						}
                                            }
					}
                                    }
				}
                            }
			}
                    }
		}
            }
	}
    };
    
    return context;
};

module.exports = searchContext;
