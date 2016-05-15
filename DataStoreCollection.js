var _ = require('lodash');
let updater = require('./lib/updater');

export default class DataStoreCollection {
    dataSet;
    namespace;


    constructor(collectionName:String, dataSet:Object, namespace:String) {
        this.collectionName = collectionName;
        this.dataSet = dataSet;
        this.namespace = namespace;

    }

    /**
     * Insert Object To dataStore
     * @param object
     * @returns {Promise}
     */
    insertOne(object) {
        let key = this.dataSet.key({namespace: this.namespace, path: [this.collectionName, object._id]});
        var entity = {
            key: key,
            data: object
        };
        let that = this;
        return new Promise(function (fulfill, reject) {
            that.dataSet.save(entity, function (err) {
                if (err) {
                    return reject(err);
                }
                fulfill({
                    ops: [object]
                });
            });
        });
    }

    findOneAndUpdate(query, update) {

        let dQuery = this.dataSet.createQuery(this.namespace, this.collectionName);
        _.each(query, function (v, k) {
            dQuery.filter(k, '=', v)

        });


        let that = this;
        return new Promise(function (fulfill, reject) {
            that.dataSet.runQuery(dQuery, function (err, entities) {
                var entity = entities[0];
                if (err)
                    return reject(err);
                if (entity === undefined) {
                    reject();

                } else {
                    updater(update, entity.data);
                    that.dataSet.save(entity, function (err) {
                        if (err) {
                            return reject(err);
                        }
                        fulfill(that.fromDataStore(entity));
                    });

                }

            });
        });

    }


    /**
     * Update One
     * @param query
     * @param update
     * @returns {Promise}
     */
    upsertOne(query, update) {
        let that = this;
        return new Promise(function (fulfill, reject) {
            let key = that.dataSet.key({namespace: that.namespace, path: [that.collectionName, query._id]});

            that.dataSet.get(key, function (err, entity) {
                if (err)
                    return reject(err);
                if (entity === undefined) {

                } else {
                    updater(update, entity.data);
                    that.dataSet.save(entity, function (err) {
                        if (err) {
                            return reject(err);
                        }
                        fulfill(that.fromDataStore(entity));
                    });

                }

            });

        });
    }

    /**
     *Used to convert dataStore Format to Normal type
     * @param obj
     * @returns {*}
     */
    fromDataStore(obj) {
        obj.data.id = obj.key.path[obj.key.path.length - 1];
        return obj.data;
    }

    find(query, {skip, limit, sort} = {}) {
        return this._rawFind(query, {skip, limit, sort})
            .catch(error => {
                // Check for "no geoindex" error
                if (error.code != 17007 || !error.message.match(/unable to find index for .geoNear/)) {
                    throw error;
                }
                // Figure out what key needs an index
                let key = error.message.match(/field=([A-Za-z_0-9]+) /)[1];
                if (!key) {
                    throw error;
                }

                var index = {};
                index[key] = '2d';
                //TODO: condiser moving index creation logic into Schema.js
                return this._mongoCollection.createIndex(index)
                    // Retry, but just once.
                    .then(() => this._rawFind(query, {skip, limit, sort}));
            });
    }

    _rawFind(query, {skip, limit, sort} = {}) {

        console.log(this.collectionName);
        console.log(query);
        let dQuery = this.dataSet.createQuery(this.namespace, this.collectionName);
        _.each(query, function (v, k) {
            if (v === undefined)
                v = "";
            dQuery.filter(k, '=', v)
        });
        if (limit)
            dQuery.limit(limit);
        if (skip)
            dQuery.offset(skip);
        var that = this;
        return new Promise(function (fulfill, reject) {
            that.dataSet.runQuery(dQuery, function (err, entities) {
                if (err)
                    return reject(err);
                fulfill(entities.map(that.fromDataStore));

            });
        });
    }
}
