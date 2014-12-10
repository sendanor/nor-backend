/** Interface for Worker objects in NoPG database */

"use strict";

var ARRAY = require('nor-array');
var $Q = require('q');
var is = require('nor-is');
var debug = require('nor-debug');
var NoPg = require('nor-nopg');

module.exports = function workers(opts) {

	opts = opts || {};

	debug.assert(opts.pg).is('string');

	/** Register worker */
	workers.register = function workers_register(hostname, port) {

		var input;
		if(arguments.length >= 2) {
			input = {
				"hostname": hostname,
				"port": port
			};
		} else if(hostname && hostname.hostname && hostname.port) {
			input = {'hostname': hostname.hostname, 'port': hostname.port};
		} else {
			throw new TypeError("Unknown arguments for workers_register");
		}

		debug.log('input = ', input);

		var doc;
		return NoPg.start(opts.pg).search("Worker")(input).then(function workers_register_remove_if_necessary(db) {
			var docs = db.fetch();

			if(docs.length >= 1) {
				doc = docs.shift();
			}

			if(docs.length >= 1) {
				return ARRAY(docs).map(function workers_unregister_duplicate(d) {
					return function step(db_) {
						return db_.del(d);
					};
				}).reduce($Q.when, $Q(db));
			}

			return db;
		}).then(function workers_register_get_or_create(db) {

			if(doc) {
				debug.log('Worker found: ', doc);
				return db;
			}

			return db.create("Worker")({
				"hostname": hostname,
				"port": port
			}).then(function workers_register_fetch(db) {
				doc = db.fetch();
				debug.assert(doc).is('object');
				debug.log('Created document: ', doc);
				return db;
			});

		}).commit().then(function workers_register_return() {
			return doc;
		});
	};

	/** Unregister worker */
	workers.unregister = function workers_unregister(hostname, port) {
		var where;

		if(arguments.length >= 2) {
			where = {
				"hostname": hostname,
				"port": port
			};
		} else if(hostname && hostname.$id) {
			where = {'$id': hostname.$id};
		} else if(hostname && hostname.hostname && hostname.port) {
			where = {'hostname': hostname.hostname, 'port': hostname.port};
		} else if(is.uuid(hostname)) {
			where = {'$id': hostname};
		} else {
			throw new TypeError("Unknown arguments for workers_unregister");
		}

		debug.log('Unregistering worker: ', where);

		return NoPg.start(opts.pg).searchSingle("Worker")(where).then(function workers_unregister_del_if_found(db) {
			var doc = db.fetch();
			if(doc) {
				return db.del(doc);
			}
		}).commit();
	};

	/** Get list of all workers registered at the moment */
	workers.list = function workers_list() {
		return NoPg.start(opts.pg).search("Worker")().commit().then(function workers_list_fetch(db) {
			var list = db.fetch();
			debug.assert(list).is('array');
			return list;
		});
	};

	/** Get list of all workers registered at the moment */
	workers.fetch = function workers_fetch(id) {
		debug.assert(id).is('uuid');
		return NoPg.start(opts.pg).searchSingle("Worker")({'$id': id}).commit().then(function workers_fetch(db) {
			var doc = db.fetch();
			debug.assert(doc).is('object');
			debug.assert(doc.$id).equals(id);
			return doc;
		});
	};

	// Export
	return workers;
};

/* EOF */
