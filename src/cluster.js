/** Our cluster implementation */

"use strict";

var debug = require('nor-debug');
var is = require('nor-is');
var $Q = require('q');
var _cluster = require('cluster');
var debug = require('nor-debug');
var HTTP = require('http');
var OS = require('os');
var ARRAY = require('nor-array');

var WORKERS = require('./workers.js');

var CLUSTER = module.exports = {};

/** Application instances that has been started */
CLUSTER.apps = [];

/** HTTP instances that has been started */
CLUSTER.https = [];

/** Returns a promise of express port listen method */
function listen_port(app, port) {
	debug.assert(app).is('function');
	debug.assert(port).is('number');
	var server = HTTP.createServer(app);
	var defer = $Q.defer();
	server.once('error', function(err) {
		defer.reject(err);
	});
	server.listen(port, function(){
		defer.resolve(server);
	});
	return defer.promise;
}

/** */
CLUSTER.start_http = function start_http_servers(get_app, port, shared_ports) {

	if(!process.env.WORKER_PORT) {
		process.env.WORKER_PORT = port;
	}

	if(arguments.length >= 3) {
		shared_ports = is.array(shared_ports) ? shared_ports : [shared_ports];
	} else {
		shared_ports = [];
	}

	return $Q.when(get_app(port)).then(function(app) {

		var ports = [port].concat(shared_ports);

		debug.assert(ports).is('array').minLength(1);

		app.set("worker-port", port);
		app.set("port", port);
		app.set("ports", ports);
		app.set("shared-ports", shared_ports);

		CLUSTER.apps.push(app);

		var https = [];

		return ARRAY(ports).map(function create_step(p) {
			return function step() {
				return listen_port(app, p).then(function(http) {
					debug.assert(http).is('object');
					https.push(http);
					CLUSTER.https.push(http);
				});
			};
		}).reduce($Q.when, $Q()).then(function() {

			debug.assert(https).is('array').minLength(1);

			app.set('http-servers', https);

			debug.info('HTTP server listening on port(s): ' + ports.join(' ') + ' with worker port as ', port);

			return app;
		});
	});
};

function is_argument(arg) {
	return function is_argument_(s) {
		return (s === arg) || (s.substr(0, arg.length+1) === arg+'=');
	};
}

function not_argument(arg) {
	var f = is_argument(arg);
	return function not_argument_(s) {
		return !f(s);
	};
}

function and(a, b) {
	return function and_(s) {
		return a(s) && b(s);
	};
}

/*function or(a, b) {
	return function and_(s) {
		return a(s) && b(s);
	};
}*/

var debug_flag;
var debug_ports_enabled;
var debug_ports = 0;
if(_cluster.isMaster) {
	debug_flag = ARRAY(process.execArgv).some(is_argument('--debug-brk')) ? '--debug-brk' : '--debug';
	debug_ports_enabled = ARRAY(process.execArgv).some(is_argument(debug_flag));
	_cluster.setupMaster({
		execArgv: ARRAY(process.execArgv).filter( and(not_argument('--debug-brk'), not_argument('--debug')) ).valueOf()
	});
}

/** Returns a promise of started cluster node */
CLUSTER.start_node = function cluster_start_node(env) {
	var defer = $Q.defer();
	var worker;
	_cluster.once('error', function(err) {
		defer.reject(err);
	});

	var debug_port;
	if(debug_ports_enabled) {

		if (env && env.WORKER_DEBUG_PORT) {
			debug_port = parseInt(env.WORKER_DEBUG_PORT, 10);
		} else {
			debug_port = 5859 + debug_ports;
			debug_ports += 1;
			if(!env) {
				env = {};
			}
			env.WORKER_DEBUG_PORT = debug_port;
		}

		debug.info('Setting cluster worker debug port as ' + debug_port);
		_cluster.settings.execArgv.push(debug_flag + '=' + debug_port);
	}

	if(arguments.length >= 1) {
		worker = _cluster.fork(env);
	} else {
		worker = _cluster.fork();
	}

	if (debug_ports_enabled) {
		_cluster.settings.execArgv.pop();
	}

	_cluster.once('online', function() {
		defer.resolve(worker);
	});
	return defer.promise;
};

/** Initialize config object for cluster configurations */
CLUSTER.initConfig = function cluster_init_config(config) {
	if(config.cluster) {

		if(config.cluster === true) {
			config.cluster = {};
		}

		if(config.cluster.shared) {
			config.cluster.shared = is.array(config.cluster.shared) ? [].concat(config.cluster.shared) : [config.cluster.shared];
		} else {
			config.cluster.shared = is.array(config.port) ? [].concat(config.port) : [config.port];
		}

		if(config.cluster.workers) {
			config.cluster.workers = is.array(config.cluster.workers) ? [].concat(config.cluster.workers) : [config.cluster.workers];
		} else if(config.cluster.shared.length >= 1) {
			config.cluster.workers = [ config.cluster.shared[config.cluster.shared.length-1] + 1 ];
		} else {
			config.cluster.workers = [];
		}

	} else {
		config.cluster = null;
	}

	//debug.log('config.cluster = ', config.cluster);
};

/** Returns array which is X elements long */
function get_cpu_count() {
	return OS.cpus().length;
}

/** Returns array which is `num` elements long */
function get_worker_array(num) {
	var cpus = [];
	for (var i=0; i < num; i += 1) {
		cpus.push(i);
	}
	return cpus;
}

/** Start clusters
 * @returns {Function} The Express application which was started, otherwise undefined which means it was the master process.
 */
CLUSTER.start = function cluster_start_all(get_app, config) {

	CLUSTER.initConfig(config);

	debug.assert(config).is('object');
	debug.assert(config.cluster).is('object');
	debug.assert(config.cluster.workers).is('array').minLength(1);
	debug.assert(config.cluster.size).ignore(undefined).is('number');

	var hostname = config.hostname || process.env.HOSTNAME || OS.hostname() || 'localhost';

	var db_workers = WORKERS({'pg':config.pg});

	var workers = ARRAY([].concat(config.cluster.workers)).map(function(n) {
		return parseInt(n, 10);
	}).valueOf();

	var port;

	if (_cluster.isWorker) {
		port = parseInt(process.env.WORKER_PORT, 10);
		debug.assert(port).is('number');
		return $Q.when(CLUSTER.start_http(get_app, port, config.cluster.shared)).then(function(app) {
			return app;
		});
	}

	var numCPUs = get_cpu_count();
	var worker_array = get_worker_array(config.cluster.size || numCPUs);
	debug.assert(worker_array).is('array').minLength(1);

	var worker_ports = ARRAY(worker_array).map(function() {
		var p = workers.shift();

		if(is.number(p)) {
			port = p;
			return p;
		}

		if(!port) {
			throw new TypeError("no port for first worker detected!");
		}

		port = port+1;
		return port;
	}).valueOf();

	debug.assert(worker_ports).is('array').minLength(1);

	//debug.log('worker_ports = ', worker_ports);

	_cluster.on('exit', function(worker/*, code, signal*/) {
		debug.error('worker ' + worker.process.pid + ' died');
	});

	return ARRAY(worker_ports).map(function(port) {
		debug.assert(port).is('integer');

		return db_workers.register(hostname, port).then(function(worker_obj) {
			debug.info('Registered worker ' + hostname + ':' + port + ' as ' + worker_obj.$id + '...');

			return CLUSTER.start_node({
				'WORKER_HOSTNAME': hostname,
				'WORKER_PORT': port,
				'WORKER_UUID': worker_obj.$id
			}).then(function(worker) {

				worker.on('exit', function worker_on_exit() {
					debug.info('Unregistering worker ' + hostname + ':' + port + '...');
					db_workers.unregister(hostname, port).fail(function(err) {
						debug.error(err);
					}).done();
				});

			});
		});
	}).reduce($Q.when, $Q()).then(function() {
		return;
	});
};

/* EOF */
