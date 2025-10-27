const fs = require('fs').promises;
const path = require('path');
const { ObjectId } = require('bson');
const Collection = require('./Collection');
const Encryption = require('./Encryption');
const FileStorage = require('./FileStorage');
const IndexManager = require('./IndexManager');

class Database {
    constructor(name, options = {}) {
        this.name = name;
        this.options = {
            dataPath: './data',
            encryptionKey: null, // По умолчанию без шифрования
            redisUrl: 'redis://localhost:6379',
            ...options
        };
        this.collections = new Map();
        
        // Инициализируем шифрование (может быть отключено)
        this.encryption = new Encryption(this.options.encryptionKey);
        
        this.fileStorage = new FileStorage(this);
        this.indexManager = new IndexManager(this);
        this.isInitialized = false;
    }

    async init() {
        if (this.isInitialized) return;

        try {
            this.dbPath = path.join(this.options.dataPath, this.name);
            await fs.mkdir(this.dbPath, { recursive: true });
            
            await this.indexManager.connect();
            await this.loadCollections();
            
            this.isInitialized = true;
            
            // Тестируем шифрование при инициализации
            this.encryption.test();
            
        } catch (error) {
            throw new Error(`Database initialization failed: ${error.message}`);
        }
    }

    async loadCollections() {
        try {
            const files = await fs.readdir(this.dbPath);
            for (const file of files) {
                const stat = await fs.stat(path.join(this.dbPath, file));
                if (stat.isDirectory()) {
                    const collection = new Collection(this, file);
                    await collection.init();
                    this.collections.set(file, collection);
                }
            }
        } catch (error) {
            console.warn('⚠️ Error loading collections:', error.message);
        }
    }

    async createCollection(name, options = {}) {
        if (this.collections.has(name)) {
            return this.collections.get(name);
        }

        const collection = new Collection(this, name, options);
        await collection.init();
        this.collections.set(name, collection);
        return collection;
    }

    async dropCollection(name) {
        const collection = this.collections.get(name);
        if (collection) {
            await collection.drop();
            this.collections.delete(name);
            return true;
        }
        return false;
    }

    collection(name) {
        return this.collections.get(name);
    }

    async close() {
        await this.indexManager.disconnect();
        for (const collection of this.collections.values()) {
            await collection.close();
        }
        this.isInitialized = false;
    }

    async stats() {
        const stats = {
            database: this.name,
            collections: {},
            totalSize: 0,
            encryption: this.encryption.isEnabled() ? 'enabled' : 'disabled'
        };

        for (const [name, collection] of this.collections) {
            const collectionStats = await collection.stats();
            stats.collections[name] = collectionStats;
            stats.totalSize += collectionStats.size;
        }

        return stats;
    }

    // Метод для проверки состояния шифрования
    getEncryptionStatus() {
        return {
            enabled: this.encryption.isEnabled(),
            algorithm: this.encryption.isEnabled() ? 'aes-256-cbc' : 'none'
        };
    }
}

module.exports = Database;