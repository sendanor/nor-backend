"use strict";

var debug = require('nor-debug');
var bytes = require('bytes');

/** Returns color code */
function get_color(status) {
	if (status >= 500) { return 31; }
	if (status >= 400) { return 33; }
	if (status >= 300) { return 36; }
	return 32;
}

/** Our cluster dev implementation. */
function clusterdev(cluster, tokens, req, res) {
	var status = res.statusCode;
	var len = parseInt(res.getHeader('Content-Length'), 10);
	var color = get_color(status);

	var remote_addr = req.headers['x-forwarded-for'] || req.connection.remoteAddress;

	var now = new Date();

	len = isNaN(len) ? '' : len = ' - ' + bytes(len);

	return '\x1b[90m' +
		'[' + now.toISOString() + '] ' +
		((cluster && cluster.worker) ? '[worker#' + cluster.worker.id + '] ' : '') +
		'[' + remote_addr + '] ' +
		((req && req.id) ? '[' + req.id + '] ' : '') +
		req.method +
		' ' + req.originalUrl + ' ' +
		'\x1b[' + color + 'm' + res.statusCode +
		' \x1b[90m' +
		((now) - req._startTime) +
		'ms' + len +
		'\x1b[0m';
}

/** The try-catch is wrapped in this function because otherwhise Google v8 would not optimize it. */
function try_wrapper(cluster, tokens, req, res) {
	try {
		return clusterdev(cluster, tokens, req, res);
	} catch(err) {
		debug.error(err);
	}
}

/** The module builder */
module.exports = function clusterdev_format(opts) {
	opts = opts || {};
	return try_wrapper.bind(undefined, opts.cluster);
};

/* EOF */
