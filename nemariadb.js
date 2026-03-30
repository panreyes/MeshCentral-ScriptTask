/** 
* @description MeshCentral database abstraction layer for MariaDB to be more Mongo-like
* @author Ryan Blenis
* @copyright 
* @license Apache-2.0
* This is a simple abstraction layer for many commonly used DB calls.
* It supplements the need to duplicate and modify all calls in the db.js file.
*/

class NEMariaDB {
    constructor(pool) {
        this.pool = pool;
        this._find = null;
        this._proj = null;
        this._limit = null;
        this._sort = null;
        
        // initialize table
        this._initDB();
        
        return this;
    }
    
    _initDB() {
        this.pool.query("CREATE TABLE IF NOT EXISTS plugin_scripttask (id VARCHAR(128) PRIMARY KEY, doc JSON)")
            .then(() => {
                // optionally create indexes on JSON fields in MariaDB if needed for speed
            })
            .catch(err => {
                console.log("PLUGIN: ScriptTask: Error creating database table", err);
            });
    }

    _escape(val) {
        if (typeof val === 'string') {
            return "'" + val.replace(/'/g, "''").replace(/\\/g, "\\\\") + "'";
        }
        if (typeof val === 'number') return val;
        if (val === null) return "NULL";
        if (typeof val === 'boolean') return val ? 'TRUE' : 'FALSE';
        return "'" + JSON.stringify(val).replace(/'/g, "''").replace(/\\/g, "\\\\") + "'";
    }

    _buildWhere(filter) {
        if (!filter || Object.keys(filter).length === 0) return "1=1";
        
        var conditions = [];
        for (var key in filter) {
            if (key === '$or') {
                var orConds = [];
                for (var i in filter.$or) {
                    orConds.push("(" + this._buildWhere(filter.$or[i]) + ")");
                }
                conditions.push("(" + orConds.join(" OR ") + ")");
            } else if (key === '$and') {
                var andConds = [];
                for (var i in filter.$and) {
                    andConds.push("(" + this._buildWhere(filter.$and[i]) + ")");
                }
                conditions.push("(" + andConds.join(" AND ") + ")");
            } else {
                var val = filter[key];
                var dbKey = key === '_id' ? 'id' : `JSON_UNQUOTE(JSON_EXTRACT(doc, '$.${key}'))`;
                
                if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
                    // special operator
                    for (var op in val) {
                        if (op === '$in') {
                            var inList = val.$in.map(v => this._escape(v)).join(",");
                            conditions.push(`${dbKey} IN (${inList})`);
                        } else if (op === '$gte') {
                            conditions.push(`${dbKey} >= ${val.$gte}`);
                        } else if (op === '$lte') {
                            conditions.push(`${dbKey} <= ${val.$lte}`);
                        } else if (op === '$gt') {
                            conditions.push(`${dbKey} > ${val.$gt}`);
                        } else if (op === '$lt') {
                            conditions.push(`${dbKey} < ${val.$lt}`);
                        }
                    }
                } else if (val === null) {
                    conditions.push(`(${dbKey} IS NULL OR ${dbKey} = 'null')`);
                } else {
                    conditions.push(`${dbKey} = ${this._escape(val)}`);
                }
            }
        }
        return conditions.join(" AND ");
    }

    find(args, proj) {
        this._find = args;
        this._proj = proj;
        this._sort = null;
        this._limit = null;
        return this;
    }
    
    project(args) {
        this._proj = args;
        return this;
    }
    
    sort(args) {
        this._sort = args;
        return this;
    }
    
    limit(limit) {
        this._limit = limit;
        return this;
    }
    
    toArray(callback) {
        var self = this; 
        return new Promise(function(resolve, reject) {
            var where = self._buildWhere(self._find);
            var query = `SELECT doc FROM plugin_scripttask WHERE ${where}`;
            
            if (self._sort) {
                var order = [];
                for (var key in self._sort) {
                    var dir = self._sort[key] === -1 ? "DESC" : "ASC";
                    if (key === '_id') order.push(`id ${dir}`);
                    else order.push(`JSON_UNQUOTE(JSON_EXTRACT(doc, '$.${key}')) ${dir}`);
                }
                if (order.length > 0) query += " ORDER BY " + order.join(", ");
            }
            if (self._limit) {
                query += ` LIMIT ${self._limit}`;
            }

            self.pool.query(query)
                .then(rows => {
                    var docs = [];
                    for (var i = 0; i < rows.length; i++) {
                        var doc = typeof rows[i].doc === 'string' ? JSON.parse(rows[i].doc) : rows[i].doc;
                        
                        if (self._proj) {
                            var pDoc = {};
                            var keepFields = [];
                            var excludeFields = [];
                            for (var p in self._proj) {
                                if (self._proj[p] === 1) keepFields.push(p);
                                else if (self._proj[p] === 0) excludeFields.push(p);
                            }
                            if (keepFields.length > 0) {
                                for (var k of keepFields) {
                                    if (doc[k] !== undefined) pDoc[k] = doc[k];
                                }
                                if (excludeFields.indexOf('_id') === -1) pDoc._id = doc._id; 
                                docs.push(pDoc);
                            } else {
                                for (var k of excludeFields) delete doc[k];
                                docs.push(doc);
                            }
                        } else {
                            docs.push(doc);
                        }
                    }
                    if (callback != null && typeof callback == 'function') callback(null, docs);
                    resolve(docs);
                })
                .catch(err => {
                    if (callback != null && typeof callback == 'function') callback(err, null);
                    reject(err);
                });
        });
    }
    
    insertOne(args, options) {
        var self = this;
        return new Promise(function(resolve, reject) {
            var id = args._id;
            if (!id) {
                // Generate a random 24 char hex id similar to Mongo
                id = require('crypto').randomBytes(12).toString('hex');
                args._id = id;
            }
            var docStr = JSON.stringify(args);
            self.pool.query("INSERT INTO plugin_scripttask (id, doc) VALUES (?, ?)", [id, docStr])
                .then(res => {
                    resolve({ insertedId: id });
                })
                .catch(err => reject(err));
        });
    }
    
    deleteOne(filter, options) {
        var self = this;
        var where = self._buildWhere(filter);
        return new Promise(function(resolve, reject) {
            self.pool.query(`DELETE FROM plugin_scripttask WHERE ${where} LIMIT 1`)
                .then(res => resolve({ deletedCount: res.affectedRows }))
                .catch(err => reject(err));
        });
    }
    
    deleteMany(filter, options) {
        var self = this;
        var where = self._buildWhere(filter);
        return new Promise(function(resolve, reject) {
            self.pool.query(`DELETE FROM plugin_scripttask WHERE ${where}`)
                .then(res => resolve({ deletedCount: res.affectedRows }))
                .catch(err => reject(err));
        });
    }
    
    updateOne(filter, update, options) {
        var self = this;
        var where = self._buildWhere(filter);
        if (options == null) options = {};
        if (options.upsert == null) options.upsert = false;
        
        return new Promise(function(resolve, reject) {
            self.pool.query(`SELECT id, doc FROM plugin_scripttask WHERE ${where} LIMIT 1`)
                .then(rows => {
                    if (rows.length === 0) {
                        if (options.upsert) {
                            var newDoc = { ...filter };
                            if (update.$set) newDoc = { ...newDoc, ...update.$set };
                            return self.insertOne(newDoc).then(res => resolve({ matchedCount: 0, modifiedCount: 1, upsertedId: res.insertedId }));
                        }
                        return resolve({ matchedCount: 0, modifiedCount: 0 });
                    }
                    
                    var id = rows[0].id;
                    var doc = typeof rows[0].doc === 'string' ? JSON.parse(rows[0].doc) : rows[0].doc;
                    var modifiedFields = 0;

                    if (update.$set) {
                        for (var k in update.$set) {
                            doc[k] = update.$set[k];
                        }
                        modifiedFields = 1;
                    } else {
                        doc = { ...doc, ...update };
                        if (!doc._id) doc._id = id;
                        modifiedFields = 1;
                    }
                    
                    if (modifiedFields) {
                        var docStr = JSON.stringify(doc);
                        return self.pool.query("UPDATE plugin_scripttask SET doc = ? WHERE id = ?", [docStr, id])
                            .then(res => resolve({ matchedCount: 1, modifiedCount: 1, upsertedId: id }));
                    } else {
                        return resolve({ matchedCount: 1, modifiedCount: 0 });
                    }
                })
                .catch(err => reject(err));
        });
    }
    
    updateMany(filter, update, options) {
        var self = this;
        var where = self._buildWhere(filter);
        if (options == null) options = {};
        if (options.upsert == null) options.upsert = false;
        
        return new Promise(function(resolve, reject) {
            self.pool.query(`SELECT id, doc FROM plugin_scripttask WHERE ${where}`)
                .then(rows => {
                    if (rows.length === 0) {
                        if (options.upsert) {
                           // Upsert logic for updateMany doesn't normally generate multiple
                           var newDoc = { ...filter };
                           if (update.$set) newDoc = { ...newDoc, ...update.$set };
                           return self.insertOne(newDoc).then(res => resolve({ matchedCount: 0, modifiedCount: 1, upsertedId: res.insertedId }));
                        }
                        return resolve({ matchedCount: 0, modifiedCount: 0 });
                    }
                    
                    var updates = [];
                    for (var i = 0; i < rows.length; i++) {
                        var id = rows[i].id;
                        var doc = typeof rows[i].doc === 'string' ? JSON.parse(rows[i].doc) : rows[i].doc;
                        
                        if (update.$set) {
                            for (var k in update.$set) {
                                doc[k] = update.$set[k];
                            }
                        } else {
                            doc = { ...doc, ...update };
                            if (!doc._id) doc._id = id;
                        }
                        var docStr = JSON.stringify(doc);
                        updates.push(self.pool.query("UPDATE plugin_scripttask SET doc = ? WHERE id = ?", [docStr, id]));
                    }
                    
                    if (updates.length > 0) {
                        return Promise.all(updates).then(() => resolve({ matchedCount: rows.length, modifiedCount: rows.length }));
                    } else {
                        return resolve({ matchedCount: rows.length, modifiedCount: 0 });
                    }
                })
                .catch(err => reject(err));
        });
    }

    indexes(callback) {
        if (callback != null && typeof callback == 'function') callback(null, []);
    }
    
    dropIndexes(callback) {
        if (callback != null && typeof callback == 'function') callback(null);
    }
    
    createIndex(args, options) {
        // Ignored for JSON DB adapter for now
    }
}

module.exports = NEMariaDB;
