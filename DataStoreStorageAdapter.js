import DataStoreCollection from './DataStoreCollection';
import MongoSchemaCollection from './DataStoreSchemaCollection';
import {parse as parseUrl, format as formatUrl} from '../../../vendor/mongodbUrl';


const MongoSchemaCollectionName = '_SCHEMA';


export class MongoStorageAdapter {
    // Private
    _uri:string;
    _options:Object;
    // Public
    connectionPromise;
    database;
    dataSet;

    constructor(uri:string, options:Object) {
        this._uri = uri;
        this._options = options;
        this.dataSet = options.dataSet;
    }

    connect() {
       
        return Promise.resolve();
    }

    collection(name:string) {
        return this.connect().then(() => {
            return this.database.collection(name);
        });
    }

    adaptiveCollection(name:string) {
        return this.connect()
            .then(() => new DataStoreCollection(name, this.dataSet, this._options.namespace));
        //   .then(rawCollection => );
    }

    schemaCollection(collectionPrefix:string) {
        return this.connect()
            .then(() => this.adaptiveCollection(MongoSchemaCollectionName))
            .then(collection => new MongoSchemaCollection(collection));
    }

    collectionExists(name:string) {
        return this.connect().then(() => {
            return this.database.listCollections({name: name}).toArray();
        }).then(collections => {
            return collections.length > 0;
        });
    }

    dropCollection(name:string) {
        return this.collection(name).then(collection => collection.drop());
    }

    // Used for testing only right now.
    collectionsContaining(match:string) {
        return this.connect().then(() => {
            return this.database.collections();
        }).then(collections => {
            return collections.filter(collection => {
                if (collection.namespace.match(/\.system\./)) {
                    return false;
                }
                return (collection.collectionName.indexOf(match) == 0);
            });
        });
    }
}

export default MongoStorageAdapter;
module.exports = MongoStorageAdapter; // Required for tests
