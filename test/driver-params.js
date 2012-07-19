exports.driverParams = {
    'memory': {
    },
    'disk': {
        mktmp: true
    },
    'caching': {
	'cache': {driver: 'memory', params: {}},
	'source': {driver: 'disk', params: {mktmp: true}}
    }
};
