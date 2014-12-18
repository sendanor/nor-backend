"use strict";

/* NewRelic Setup */
require('nor-newrelic').init();

var _cluster = require('cluster');

// Setup worker debug port if defined
if(_cluster.isWorker && process.env.WORKER_DEBUG_PORT) {
	process._debugPort = process.env.WORKER_DEBUG_PORT;
}

// Assert that we are not bundling this file to browserify
var debug = require('nor-debug');
debug.assert(process.browser).is('undefined');

var $Q = require('q');
$Q.longStackSupport = true;

var PATH = require('path');

var project_root_path = PATH.resolve(__dirname, '../..');
debug.setProjectRoot( project_root_path );

var WORKER_PORT = process.env.WORKER_PORT;
var WORKER_HOSTNAME = process.env.WORKER_HOSTNAME;

debug.setPrefix(function(prefix) {
	if(!WORKER_PORT) {
		return '[#' + process.pid + '] ' + prefix;
	}

	return '[#' + process.pid + '@' + WORKER_HOSTNAME + ':' + WORKER_PORT +'] ' + prefix;
});

process.env.PROJECT_ROOT = project_root_path;

require('ejs'); // This must be defined even if not used, since it registers require handler for .ejs files

var NoPg = require('nor-nopg');

if(process.env.ENABLE_NOPG_DEBUG) {
	NoPg.debug = true;
}

//var log_format_clusterdev = require('./log-format-clusterdev.js');

var CLUSTER = require('./cluster.js');

/* */
module.exports = function core(app) {

	var config = app.config;
	var cluster = config.cluster ? _cluster : undefined;

	return $Q.fcall(function init_if_master() {

		if( (!config.cluster) && app.init) {
			return app.init();
		}

		if(app.init && cluster && cluster.isMaster) {
			return app.init();
		} else if(app.initWorker && cluster && cluster.isWorker) {
			return app.initWorker();
		}

	}).then(function start_servers() {

		if(!app.get) {
			return;
		}

		if(!config.cluster) {
			//debug.log('here');
			return CLUSTER.start_http(app.get, config.port).then(function(a) {
				debug.info('Single node started.');
				return a;
			});
		}

		//debug.log('config.cluster = ', config.cluster);
		return CLUSTER.start(app.get, config).then(function(a) {
			if(a) {
				debug.info('Cluster node started.');
				return a;
			} else {
				debug.info('Master node started.');
			}
		});

	}).then(function after_start_servers(a) {
		if(a && app.post) {
			return app.post(a);
		}
	});
};

/* EOF */
