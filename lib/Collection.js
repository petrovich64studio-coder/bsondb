const fs = require('fs').promises;
const path = require('path');
const { ObjectId, serialize, deserialize } = require('bson');

class Collection {
    constructor(database, name, options = {}) {
        this.database = database;
        this.name = name;
        this.options = options;
        this.collectionPath = path.join(database.dbPath, name);
        this.dataFile = path.join(this.collectionPath, 'data.bson');
        this.indexFile = path.join(this.collectionPath, '_index.bson');
        this.isInitialized = false;
    }

    async init() {
        if (this.isInitialized) return;

        try {
            await fs.mkdir(this.collectionPath, { recursive: true });
            
            // Создаем файл данных если не существует
            try {
                await fs.access(this.dataFile);
            } catch {
                await this.saveData([]);
            }

            // Создаем индекс по умолчанию для _id
            await this.database.indexManager.createIndex(this.name, '_id');
            this.isInitialized = true;
        } catch (error) {
            throw new Error(`Collection initialization failed: ${error.message}`);
        }
    }

    async saveData(data) {
        try {
            const bsonData = serialize({ documents: data });
            
            // Шифруем данные если включено шифрование
            const encryptedData = this.database.encryption.encrypt(bsonData);
            await fs.writeFile(this.dataFile, encryptedData);
        } catch (error) {
            throw new Error(`Failed to save data: ${error.message}`);
        }
    }

    async loadData() {
        try {
            const encryptedData = await fs.readFile(this.dataFile);
            
            // Дешифруем данные если включено шифрование
            const decryptedData = this.database.encryption.decrypt(encryptedData);
            
            // Пытаемся распарсить BSON
            try {
                const parsed = deserialize(decryptedData);
                return parsed.documents || [];
            } catch (parseError) {
                // Если не удалось распарсить, возможно файл не зашифрован
                console.warn(`⚠️ Failed to parse collection data for ${this.name}, returning empty array`);
                return [];
            }
        } catch (error) {
            // Если файл не существует или ошибка чтения, возвращаем пустой массив
            return [];
        }
    }

    async insertOne(document) {
        if (!document._id) {
            document._id = new ObjectId();
        } else if (typeof document._id === 'string') {
            document._id = new ObjectId(document._id);
        }

        const documents = await this.loadData();
        documents.push(document);
        await this.saveData(documents);

        await this.database.indexManager.updateIndex(this.name, document);
        return document;
    }

    async insertMany(documents) {
        for (const doc of documents) {
            if (!doc._id) {
                doc._id = new ObjectId();
            } else if (typeof doc._id === 'string') {
                doc._id = new ObjectId(doc._id);
            }
        }

        const existingDocs = await this.loadData();
        existingDocs.push(...documents);
        await this.saveData(existingDocs);

        for (const doc of documents) {
            await this.database.indexManager.updateIndex(this.name, doc);
        }

        return documents;
    }

    async find(query = {}, options = {}) {
        const documents = await this.loadData();
        let results = documents.filter(doc => this.matchDocument(doc, query));

        if (options.sort) {
            results = this.sortDocuments(results, options.sort);
        }

        if (options.limit) {
            results = results.slice(0, options.limit);
        }

        if (options.skip) {
            results = results.slice(options.skip);
        }

        return results;
    }

    async findOne(query = {}) {
        const documents = await this.loadData();
        return documents.find(doc => this.matchDocument(doc, query)) || null;
    }

    async findById(id) {
        return await this.findOne({ _id: typeof id === 'string' ? new ObjectId(id) : id });
    }

    async updateOne(query, update, options = {}) {
        const documents = await this.loadData();
        const index = documents.findIndex(doc => this.matchDocument(doc, query));
        
        if (index === -1) {
            if (options.upsert) {
                return await this.insertOne(update.$set || update);
            }
            return null;
        }

        const oldDoc = documents[index];
        const updatedDoc = this.applyUpdate(oldDoc, update);
        documents[index] = updatedDoc;
        await this.saveData(documents);

        await this.database.indexManager.updateIndex(this.name, updatedDoc, oldDoc);
        return updatedDoc;
    }

    async updateMany(query, update) {
        const documents = await this.loadData();
        const results = [];
        
        for (let i = 0; i < documents.length; i++) {
            if (this.matchDocument(documents[i], query)) {
                const oldDoc = documents[i];
                const updatedDoc = this.applyUpdate(oldDoc, update);
                documents[i] = updatedDoc;
                results.push(updatedDoc);
                await this.database.indexManager.updateIndex(this.name, updatedDoc, oldDoc);
            }
        }

        if (results.length > 0) {
            await this.saveData(documents);
        }

        return results;
    }

    async deleteOne(query) {
        const documents = await this.loadData();
        const index = documents.findIndex(doc => this.matchDocument(doc, query));
        
        if (index === -1) return null;

        const deleted = documents.splice(index, 1)[0];
        await this.saveData(documents);
        await this.database.indexManager.removeFromIndex(this.name, deleted);
        return deleted;
    }

    async deleteMany(query) {
        const documents = await this.loadData();
        const toDelete = [];
        const remaining = [];

        for (const doc of documents) {
            if (this.matchDocument(doc, query)) {
                toDelete.push(doc);
            } else {
                remaining.push(doc);
            }
        }

        if (toDelete.length > 0) {
            await this.saveData(remaining);
            for (const doc of toDelete) {
                await this.database.indexManager.removeFromIndex(this.name, doc);
            }
        }

        return toDelete;
    }

    async count(query = {}) {
        const documents = await this.loadData();
        return documents.filter(doc => this.matchDocument(doc, query)).length;
    }

    async createIndex(fields, options = {}) {
        return await this.database.indexManager.createIndex(this.name, fields, options);
    }

    async dropIndex(fields) {
        return await this.database.indexManager.dropIndex(this.name, fields);
    }

    matchDocument(doc, query) {
        for (const [key, value] of Object.entries(query)) {
            if (key === '$or') {
                if (!value.some(condition => this.matchDocument(doc, condition))) {
                    return false;
                }
            } else if (key === '$and') {
                if (!value.every(condition => this.matchDocument(doc, condition))) {
                    return false;
                }
            } else if (typeof value === 'object' && value !== null) {
                for (const [op, opValue] of Object.entries(value)) {
                    if (!this.matchOperator(doc[key], op, opValue)) {
                        return false;
                    }
                }
            } else if (doc[key] !== value) {
                return false;
            }
        }
        return true;
    }

    matchOperator(fieldValue, operator, value) {
        switch (operator) {
            case '$eq': return fieldValue === value;
            case '$ne': return fieldValue !== value;
            case '$gt': return fieldValue > value;
            case '$gte': return fieldValue >= value;
            case '$lt': return fieldValue < value;
            case '$lte': return fieldValue <= value;
            case '$in': return value.includes(fieldValue);
            case '$nin': return !value.includes(fieldValue);
            case '$regex': return new RegExp(value).test(fieldValue);
            default: return false;
        }
    }

    sortDocuments(documents, sort) {
        return documents.sort((a, b) => {
            for (const [field, direction] of Object.entries(sort)) {
                const aVal = a[field];
                const bVal = b[field];
                if (aVal < bVal) return direction === 1 ? -1 : 1;
                if (aVal > bVal) return direction === 1 ? 1 : -1;
            }
            return 0;
        });
    }

    applyUpdate(doc, update) {
        const updated = { ...doc };
        
        for (const [operator, value] of Object.entries(update)) {
            switch (operator) {
                case '$set':
                    Object.assign(updated, value);
                    break;
                case '$unset':
                    for (const field of Object.keys(value)) {
                        delete updated[field];
                    }
                    break;
                case '$inc':
                    for (const [field, amount] of Object.entries(value)) {
                        updated[field] = (updated[field] || 0) + amount;
                    }
                    break;
                case '$push':
                    for (const [field, item] of Object.entries(value)) {
                        if (!Array.isArray(updated[field])) {
                            updated[field] = [];
                        }
                        updated[field].push(item);
                    }
                    break;
            }
        }
        
        return updated;
    }

    async drop() {
        await fs.rm(this.collectionPath, { recursive: true, force: true });
        await this.database.indexManager.dropCollectionIndexes(this.name);
    }

    async close() {
        // Cleanup resources if needed
    }

    async stats() {
        const documents = await this.loadData();
        let size = 0;
        
        try {
            const stat = await fs.stat(this.dataFile);
            size = stat.size;
        } catch (error) {
            // File might not exist
        }
        
        return {
            name: this.name,
            count: documents.length,
            size: size,
            indexes: await this.database.indexManager.getCollectionIndexes(this.name)
        };
    }
}

module.exports = Collection;