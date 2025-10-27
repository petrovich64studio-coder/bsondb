const Database = require('./lib/Database');
const Model = require('./lib/Model');

class BsonDB {
    constructor() {
        this.connections = new Map();
        this.models = new Map();
    }

    async connect(databaseName, options = {}) {
        try {
            const db = new Database(databaseName, options);
            await db.init();
            this.connections.set(databaseName, db);
            return db;
        } catch (error) {
            throw new Error(`Failed to connect to database: ${error.message}`);
        }
    }

    async disconnect(databaseName) {
        const db = this.connections.get(databaseName);
        if (db) {
            await db.close();
            this.connections.delete(databaseName);
        }
    }

    model(name, schema, options = {}) {
        if (this.models.has(name)) {
            return this.models.get(name);
        }

        const model = new Model(name, schema, options);
        this.models.set(name, model);
        return model;
    }

    getConnection(databaseName) {
        return this.connections.get(databaseName);
    }

    async createCollection(databaseName, collectionName, options = {}) {
        const db = this.connections.get(databaseName);
        if (!db) {
            throw new Error(`Database ${databaseName} not connected`);
        }
        return await db.createCollection(collectionName, options);
    }

    async dropCollection(databaseName, collectionName) {
        const db = this.connections.get(databaseName);
        if (!db) {
            throw new Error(`Database ${databaseName} not connected`);
        }
        return await db.dropCollection(collectionName);
    }

    // Статический метод для быстрого доступа
    static get Database() {
        return Database;
    }

    static get Model() {
        return Model;
    }
}

// Создаем синглтон экземпляр
const bsondb = new BsonDB();

// Экспортируем синглтон и классы
module.exports = bsondb;
module.exports.BsonDB = BsonDB;
module.exports.Database = Database;
module.exports.Model = Model;