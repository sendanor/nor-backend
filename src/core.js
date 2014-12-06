"use strict";

/* NewRelic Setup */
require('nor-newrelic').init();

// Assert that we are not bundling this file to browserify
var debug = require('nor-debug');
debug.assert(process.browser).is('undefined');

var $Q = require('q');
$Q.longStackSupport = true;

var PATH = require('path');
var project_root_path = PATH.resolve(__dirname, '../..');
debug.setProjectRoot( project_root_path );
process.env.PROJECT_ROOT = project_root_path;

require('ejs'); // This must be defined even if not used, since it registers require handler for .ejs files

var express = require('express');
var NoPg = require('nor-nopg');

if(process.env.ENABLE_NOPG_DEBUG) {
	NoPg.debug = true;
}

var _cluster = require('cluster');
var log_format_clusterdev = require('./log-format-clusterdev.js');

var CLUSTER = require('./cluster.js');

/* */
module.exports = function core(app) {

	var config = app.config;

	var cluster = config.cluster ? _cluster : undefined;
	express.logger.format('clusterdev', log_format_clusterdev({'cluster':cluster}) );

	return $Q.fcall(function init_if_master() {

		if(!config.cluster) {
			debug.log('Going to call app.init() for single process');
			return app.init();
		}

		if(cluster && cluster.isMaster) {
			debug.log('Going to call app.init() in master process');
			return app.init();
		}

	}).then(function start_servers() {

		if(!config.cluster) {
			debug.log('Starting single HTTP...');
			return CLUSTER.start_http(app.get, config.port);
		}

		debug.log('Starting cluster HTTP...');
		return CLUSTER.start(app.get, config).then(function(a) {
			if(a) {
				debug.info('Cluster node started.');
				return a;
			} else {
				debug.info('Master node started.');
			}
		});

	}).then(function after_start_servers(a) {
		if(a) {
			debug.log('Calling app.post() in child...');
			return app.post(a);
		} else {
			debug.log('Master node done.');
		}
	});
};

/* EOF */
