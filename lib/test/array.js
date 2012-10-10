// test/array.js
//
// Builds a test context for arrays
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

var arrayContext = function(driver, params) {

    var context = {};

    context["When we create a " + driver + " databank"] = {

        topic: function() {
            if (!params.hasOwnProperty('schema')) {
                params.schema = {};
            }
            params.schema.inbox = {
                pkey: 'username'
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
            'and we append to an uninitialized array': {
                topic: function(bank) {
                    bank.append('inbox', 'ericm', 14, this.callback); 
                },
                teardown: function(inbox, bank) {
                    if (bank && bank.del) {
                        bank.del('inbox', 'ericm', function(err) {});
                    }
                },
                "it works": function(err, inbox) {
                    assert.ifError(err);
                    assert.isArray(inbox);
                },
                "it has the right data": function(err, inbox) {
                    assert.ifError(err);
                    assert.isArray(inbox);
                    assert.lengthOf(inbox, 1);
                    assert.equal(inbox[0], 14);
                }
            },
            'and we prepend to an uninitialized array': {
                topic: function(bank) {
                    bank.prepend('inbox', 'horaceq', 42, this.callback); 
                },
                teardown: function(inbox, bank) {
                    if (bank && bank.del) {
                        bank.del('inbox', 'horace', function(err) {});
                    }
                },
                "it works": function(err, inbox) {
                    assert.ifError(err);
                    assert.isArray(inbox);
                },
                "it has the right data": function(err, inbox) {
                    assert.ifError(err);
                    assert.isArray(inbox);
                    assert.lengthOf(inbox, 1);
                    assert.equal(inbox[0], 42);
                }
            },
            'and we can insert an array': {
                topic: function(bank) {
                    bank.create('inbox', 'evanp', [1, 2, 3], this.callback); 
                },
                'without an error': function(err, value) {
                    assert.ifError(err);
                    assert.isArray(value);
                    assert.equal(value.length, 3);
		    assert.deepEqual(value, [1, 2, 3]);
                },
                'and we can fetch it': {
                    topic: function(created, bank) {
                        bank.read('inbox', 'evanp', this.callback);
                    },
                    'without an error': function(err, value) {
			assert.ifError(err);
			assert.isArray(value);
			assert.equal(value.length, 3);
			assert.deepEqual(value, [1, 2, 3]);
                    },
                    'and we can update it': {
                        topic: function(read, created, bank) {
                            bank.update('inbox', 'evanp', [1, 2, 3, 4], this.callback);
                        },
                        'without an error': function(err, value) {
			    assert.ifError(err);
			    assert.isArray(value);
			    assert.equal(value.length, 4);
			    assert.deepEqual(value, [1, 2, 3, 4]);
                        },
                        'and we can read it again': {
                            topic: function(updated, read, created, bank) {
				bank.read('inbox', 'evanp', this.callback);
                            },
                            'without an error': function(err, value) {
				assert.ifError(err);
				assert.isArray(value);
				assert.equal(value.length, 4);
				assert.deepEqual(value, [1, 2, 3, 4]);
                            },
                            'and we can prepend to it': {
                                topic: function(readAgain, updated, read, created, bank) {
				    bank.prepend('inbox', 'evanp', 0, this.callback);
                                },
                                'without an error': function(err, value) {
				    assert.ifError(err);
				    assert.isArray(value);
				    assert.equal(value.length, 5);
				    assert.deepEqual(value, [0, 1, 2, 3, 4]);
				},
				'and we can append to it': {
                                    topic: function(prepended, readAgain, updated, read, created, bank) {
					bank.append('inbox', 'evanp', 5, this.callback);
                                    },
                                    'without an error': function(err, value) {
					assert.ifError(err);
					assert.isArray(value);
					assert.equal(value.length, 6);
					assert.deepEqual(value, [0, 1, 2, 3, 4, 5]);
				    },
				    'and we can get a single item': {
					topic: function(appended, prepended, readAgain, updated, read, created, bank) {
					    bank.item('inbox', 'evanp', 2, this.callback);
					},
					'without an error': function(err, value) {
					    assert.ifError(err);
					    assert.equal(value, 2);
					},
					'and we can get a slice': {
					    topic: function(item, appended, prepended, readAgain, updated, read, created, bank) {
					        bank.slice('inbox', 'evanp', 1, 3, this.callback);
					    },
					    'without an error': function(err, value) {
						assert.ifError(err);
					        assert.isArray(value);
					        assert.equal(value.length, 2);
					        assert.deepEqual(value, [1, 2]);
					    },
                                            'and we can get the indexOf an item': {
                                                topic: function(slice, item, appended, prepended, readAgain, updated, read, created, bank) {
                                                    bank.indexOf('inbox', 'evanp', 2, this.callback);
                                                },
                                                'without an error': function(err, index) {
                                                    assert.ifError(err);
                                                    assert.equal(index, 2);
                                                },
                                                'and we can remove an item': {
					            topic: function(index, slice, item, appended, prepended, readAgain, updated, read, created, bank) {
                                                        bank.remove('inbox', 'evanp', 3, this.callback);
                                                    },
                                                    'without an error': function(err) {
                                                        assert.ifError(err);
                                                    },
                                                    'and we can read again': {
					                topic: function(index, slice, item, appended, prepended, readAgain, updated, read, created, bank) {
                                                            bank.read('inbox', 'evanp', this.callback);
                                                        },
                                                        'without an error': function(err, box) {
                                                            assert.ifError(err);
                                                            assert.deepEqual(box, [0, 1, 2, 4, 5]);
                                                        },
					                'and we can delete it': {
					                    topic: function(readAgainAgain, index, slice, item, appended, prepended, readAgain, updated, read, created, bank) {
					                        bank.del('inbox', 'evanp', this.callback);
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
            }
        }
    };

    return context;
};

module.exports = arrayContext;
