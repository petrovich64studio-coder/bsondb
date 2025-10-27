const redis = require('redis');
const fs = require('fs').promises;
const path = require('path');
const { serialize, deserialize } = require('bson');

class IndexManager {
    constructor(database) {
        this.database = database;
        this.redisClient = null;
        this.isConnected = false;
    }

    async connect() {
        try {
            this.redisClient = redis.createClient({ url: this.database.options.redisUrl });
            await this.redisClient.connect();
            this.isConnected = true;
        } catch (error) {
            console.warn('⚠️ Redis connection failed, using file-based indexes only:', error.message);
            this.isConnected = false;
        }
    }

    async disconnect() {
        if (this.redisClient && this.isConnected) {
            await this.redisClient.quit();
        }
    }

    async createIndex(collectionName, fields, options = {}) {
        const fieldArray = Array.isArray(fields) ? fields : [fields];
        const indexName = fieldArray.join('_');
        const indexKey = `index:${this.database.name}:${collectionName}:${indexName}`;

        // Save index metadata
        const indexMeta = {
            name: indexName,
            collection: collectionName,
            fields: fieldArray,
            options: options,
            createdAt: new Date()
        };

        // Store in Redis if available
        if (this.isConnected) {
            await this.redisClient.hSet(
                `db:${this.database.name}:indexes`, 
                indexName, 
                JSON.stringify(indexMeta)
            );
        }

        // Store in file
        const indexFilePath = path.join(
            this.database.dbPath, 
            collectionName, 
            `_index_${indexName}.bson`
        );

        const indexData = { metadata: indexMeta, data: {} };
        
        // Шифруем данные индекса если включено шифрование
        const encryptedIndex = this.database.encryption.encrypt(
            serialize(indexData)
        );
        await fs.writeFile(indexFilePath, encryptedIndex);

        // Build initial index
        const collection = this.database.collection(collectionName);
        if (collection) {
            const documents = await collection.loadData();
            for (const doc of documents) {
                await this.updateIndex(collectionName, doc);
            }
        }

        return indexMeta;
    }

    async updateIndex(collectionName, newDoc, oldDoc = null) {
        const indexes = await this.getCollectionIndexes(collectionName);
        
        for (const index of indexes) {
            const indexKey = this.getIndexKey(collectionName, index.name);
            
            // Remove old document from index
            if (oldDoc) {
                const oldKey = this.getDocumentIndexKey(oldDoc, index.fields);
                if (this.isConnected) {
                    await this.redisClient.sRem(`${indexKey}:${oldKey}`, oldDoc._id.toString());
                }
            }

            // Add new document to index
            const newKey = this.getDocumentIndexKey(newDoc, index.fields);
            if (this.isConnected) {
                await this.redisClient.sAdd(`${indexKey}:${newKey}`, newDoc._id.toString());
            }

            // Update file index
            await this.updateFileIndex(collectionName, index.name, newDoc, oldDoc);
        }
    }

    async removeFromIndex(collectionName, doc) {
        const indexes = await this.getCollectionIndexes(collectionName);
        
        for (const index of indexes) {
            const indexKey = this.getIndexKey(collectionName, index.name);
            const docKey = this.getDocumentIndexKey(doc, index.fields);
            
            if (this.isConnected) {
                await this.redisClient.sRem(`${indexKey}:${docKey}`, doc._id.toString());
            }

            await this.removeFromFileIndex(collectionName, index.name, doc);
        }
    }

    async getCollectionIndexes(collectionName) {
        const indexKey = `db:${this.database.name}:indexes`;
        const indexes = [];

        // Try Redis first
        if (this.isConnected) {
            try {
                const redisIndexes = await this.redisClient.hGetAll(indexKey);
                for (const [name, data] of Object.entries(redisIndexes)) {
                    const indexData = JSON.parse(data);
                    if (indexData.collection === collectionName) {
                        indexes.push(indexData);
                    }
                }
            } catch (error) {
                console.warn('⚠️ Error reading indexes from Redis:', error.message);
            }
        }

        // Fallback to file indexes
        if (indexes.length === 0) {
            const collectionPath = path.join(this.database.dbPath, collectionName);
            try {
                const files = await fs.readdir(collectionPath);
                const indexFiles = files.filter(f => f.startsWith('_index_') && f.endsWith('.bson'));
                
                for (const file of indexFiles) {
                    const filePath = path.join(collectionPath, file);
                    try {
                        const encryptedData = await fs.readFile(filePath);
                        
                        // Дешифруем данные индекса
                        const decryptedData = this.database.encryption.decrypt(encryptedData);
                        
                        try {
                            const parsed = deserialize(decryptedData);
                            
                            if (parsed.metadata && parsed.metadata.collection === collectionName) {
                                indexes.push(parsed.metadata);
                            }
                        } catch (parseError) {
                            console.warn(`⚠️ Failed to parse index file ${file}:`, parseError.message);
                            // Пытаемся восстановить индекс
                            await this.repairIndexFile(collectionName, file);
                        }
                    } catch (fileError) {
                        console.warn(`⚠️ Error reading index file ${file}:`, fileError.message);
                    }
                }
            } catch (error) {
                console.warn('⚠️ Error reading file indexes:', error.message);
            }
        }

        return indexes;
    }

    async repairIndexFile(collectionName, fileName) {
        const filePath = path.join(this.database.dbPath, collectionName, fileName);
        try {
            console.log(`🛠️ Repairing index file: ${fileName}`);
            await fs.unlink(filePath);
            
            // Извлекаем имя индекса из имени файла
            const indexName = fileName.replace('_index_', '').replace('.bson', '');
            const fields = indexName.split('_');
            
            // Пересоздаем индекс
            await this.createIndex(collectionName, fields);
            
        } catch (error) {
            console.error(`❌ Failed to repair index file ${fileName}:`, error.message);
        }
    }

    async dropIndex(collectionName, fields) {
        const fieldArray = Array.isArray(fields) ? fields : [fields];
        const indexName = fieldArray.join('_');
        const indexKey = `index:${this.database.name}:${collectionName}:${indexName}`;

        // Remove from Redis
        if (this.isConnected) {
            await this.redisClient.hDel(`db:${this.database.name}:indexes`, indexName);
            
            // Find and remove all index keys
            const keys = await this.redisClient.keys(`${indexKey}:*`);
            if (keys.length > 0) {
                await this.redisClient.del(keys);
            }
        }

        // Remove file index
        const indexFilePath = path.join(
            this.database.dbPath, 
            collectionName, 
            `_index_${indexName}.bson`
        );
        
        try {
            await fs.unlink(indexFilePath);
        } catch (error) {
            // File might not exist
        }

        return true;
    }

    async dropCollectionIndexes(collectionName) {
        const indexes = await this.getCollectionIndexes(collectionName);
        
        for (const index of indexes) {
            await this.dropIndex(collectionName, index.fields);
        }
    }

    getIndexKey(collectionName, indexName) {
        return `index:${this.database.name}:${collectionName}:${indexName}`;
    }

    getDocumentIndexKey(doc, fields) {
        return fields.map(field => {
            const value = this.getNestedValue(doc, field);
            return value !== undefined ? String(value) : 'null';
        }).join('::');
    }

    getNestedValue(obj, path) {
        return path.split('.').reduce((acc, part) => acc && acc[part], obj);
    }

    async updateFileIndex(collectionName, indexName, newDoc, oldDoc = null) {
        const indexFilePath = path.join(
            this.database.dbPath, 
            collectionName, 
            `_index_${indexName}.bson`
        );

        try {
            const encryptedData = await fs.readFile(indexFilePath);
            const decryptedData = this.database.encryption.decrypt(encryptedData);
            
            try {
                const indexData = deserialize(decryptedData);

                // Remove old document
                if (oldDoc) {
                    const oldKey = this.getDocumentIndexKey(oldDoc, indexData.metadata.fields);
                    if (indexData.data[oldKey]) {
                        indexData.data[oldKey] = indexData.data[oldKey].filter(
                            id => id !== oldDoc._id.toString()
                        );
                        if (indexData.data[oldKey].length === 0) {
                            delete indexData.data[oldKey];
                        }
                    }
                }

                // Add new document
                const newKey = this.getDocumentIndexKey(newDoc, indexData.metadata.fields);
                if (!indexData.data[newKey]) {
                    indexData.data[newKey] = [];
                }
                if (!indexData.data[newKey].includes(newDoc._id.toString())) {
                    indexData.data[newKey].push(newDoc._id.toString());
                }

                // Save updated index
                const updatedEncrypted = this.database.encryption.encrypt(
                    serialize(indexData)
                );
                await fs.writeFile(indexFilePath, updatedEncrypted);
            } catch (parseError) {
                console.warn(`⚠️ Error parsing index file ${indexName}:`, parseError.message);
                // Восстанавливаем файл индекса
                await this.repairIndexFile(collectionName, `_index_${indexName}.bson`);
            }
        } catch (error) {
            console.warn(`⚠️ Error updating file index ${indexName}:`, error.message);
        }
    }

    async removeFromFileIndex(collectionName, indexName, doc) {
        const indexFilePath = path.join(
            this.database.dbPath, 
            collectionName, 
            `_index_${indexName}.bson`
        );

        try {
            const encryptedData = await fs.readFile(indexFilePath);
            const decryptedData = this.database.encryption.decrypt(encryptedData);
            
            try {
                const indexData = deserialize(decryptedData);

                const docKey = this.getDocumentIndexKey(doc, indexData.metadata.fields);
                if (indexData.data[docKey]) {
                    indexData.data[docKey] = indexData.data[docKey].filter(
                        id => id !== doc._id.toString()
                    );
                    if (indexData.data[docKey].length === 0) {
                        delete indexData.data[docKey];
                    }
                }

                const updatedEncrypted = this.database.encryption.encrypt(
                    serialize(indexData)
                );
                await fs.writeFile(indexFilePath, updatedEncrypted);
            } catch (parseError) {
                console.warn(`⚠️ Error parsing index file during removal:`, parseError.message);
            }
        } catch (error) {
            console.warn(`⚠️ Error removing from file index:`, error.message);
        }
    }
}

module.exports = IndexManager;