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

	/** Register new worker or renew old worker record if it exists. This will also remove Sockets and Timers associated to the UUID.
	 */
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

		//debug.log('input = ', input);

		var doc;
		return $Q(NoPg.start(opts.pg).search("Worker")(input).then(function workers_register_remove_if_necessary(db) {
			var docs = db.fetch();

			if(docs.length >= 1) {
				doc = docs.shift();
			}

			if(docs.length >= 1) {
				//debug.log('here');
				return ARRAY(docs).map(function workers_unregister_duplicate(d) {
					return function step(db_) {
						return db_.del(d);
					};
				}).reduce($Q.when, $Q(db));
			}

			//debug.log('here');
			return db;
		}).then(function workers_register_get_or_create(db) {

			if(doc) {
				//debug.log('Worker found: ', doc);
				return db;
			}

			return db.create("Worker")({
				"hostname": hostname,
				"port": port
			}).then(function workers_register_fetch(db) {
				doc = db.fetch();
				debug.assert(doc).is('object');
				//debug.log('Created document: ', doc);
				return db;
			});

		}).then(function workers_register_return(db) {

			// Remove Sockets and Timers which were registered to doc.$id
			return db.search()(['OR',
			  {
				'$type': 'Socket',
				'worker': doc.$id,
			  },
			  {
				'$type': 'Timer',
				'worker': doc.$id,
			  }]).then(function remove_sockets_timers(db) {
				var docs = db.fetch();
				debug.assert(docs).is('array');
				if(docs.length === 0) {
					//debug.log('Nothing to clean from database (no Sockets nor Timers)');
					return db;
				}

				debug.info('Going to delete ' + docs.length + ' resource(s) for worker#' + doc.$id + '...');

				return ARRAY(docs).map(function step_builder(d_doc) {
					return function step(d) {
						return d.del(d_doc);
					};
				}).reduce($Q.when, $Q(db));
			});

		}).then(function(db) {
			return db.commit();
		}).then(function() {
			//debug.log('here');
			return doc;
		}));
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

		debug.info('Unregistering worker: ', where);

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

	/** Fetch the record for this worker
	 * @params id {uuid} The UUID of the worker
	 * @returns {object|undefined} The database record for this worker and `undefined` if not found
	 */
	workers.fetch = function workers_fetch(id) {
		debug.assert(id).is('uuid');
		return NoPg.start(opts.pg).searchSingle("Worker")({'$id': id}).commit().then(function workers_fetch(db) {
			var doc = db.fetch();
			if(doc === undefined) {
				return;
			}
			debug.assert(doc).is('object');
			debug.assert(doc.$id).equals(id);
			return doc;
		});
	};

	// Export
	return workers;
};

/* EOF */
