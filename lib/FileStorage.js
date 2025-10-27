const fs = require('fs').promises;
const path = require('path');
const { ObjectId } = require('bson');

class FileStorage {
    constructor(database) {
        this.database = database;
        this.filesCollection = null;
        this.chunksCollection = null;
    }

    async init() {
        this.filesCollection = await this.database.createCollection('_files');
        this.chunksCollection = await this.database.createCollection('_chunks');
        
        await this.filesCollection.createIndex(['filename']);
        await this.chunksCollection.createIndex(['files_id', 'n']);
    }

    async storeFile(filePath, options = {}) {
        const stats = await fs.stat(filePath);
        const filename = options.filename || path.basename(filePath);
        
        const fileDoc = {
            _id: new ObjectId(),
            filename: filename,
            contentType: options.contentType || 'application/octet-stream',
            length: stats.size,
            uploadDate: new Date(),
            metadata: options.metadata || {}
        };

        await this.filesCollection.insertOne(fileDoc);

        const chunkSize = options.chunkSize || 255 * 1024; // 255KB default
        const fileHandle = await fs.open(filePath, 'r');
        const buffer = Buffer.alloc(chunkSize);
        
        try {
            let chunkIndex = 0;
            
            while (true) {
                const { bytesRead } = await fileHandle.read(buffer, 0, chunkSize, chunkIndex * chunkSize);
                
                if (bytesRead === 0) break;

                const chunkDoc = {
                    _id: new ObjectId(),
                    files_id: fileDoc._id,
                    n: chunkIndex,
                    data: buffer.slice(0, bytesRead)
                };

                await this.chunksCollection.insertOne(chunkDoc);
                chunkIndex++;
            }
        } finally {
            await fileHandle.close();
        }

        return fileDoc._id;
    }

    async getFileStream(fileId) {
        const fileDoc = await this.filesCollection.findById(fileId);
        if (!fileDoc) {
            throw new Error('File not found');
        }

        const chunks = await this.chunksCollection.find({ files_id: fileId }, { sort: { n: 1 } });
        
        return {
            metadata: fileDoc,
            read: async () => {
                // Исправляем ошибку: убеждаемся, что все chunk.data являются Buffer
                const chunkBuffers = chunks.map(chunk => {
                    if (Buffer.isBuffer(chunk.data)) {
                        return chunk.data;
                    } else if (typeof chunk.data === 'string') {
                        return Buffer.from(chunk.data);
                    } else if (chunk.data && typeof chunk.data === 'object') {
                        // Если data это BSON Binary объект
                        return Buffer.from(chunk.data.buffer || chunk.data);
                    } else {
                        return Buffer.alloc(0);
                    }
                });
                
                return Buffer.concat(chunkBuffers);
            }
        };
    }

    async deleteFile(fileId) {
        const fileDoc = await this.filesCollection.findById(fileId);
        if (!fileDoc) {
            return false;
        }

        await this.filesCollection.deleteOne({ _id: fileId });
        await this.chunksCollection.deleteMany({ files_id: fileId });
        
        return true;
    }

    async findFiles(query = {}) {
        return await this.filesCollection.find(query);
    }

    async getFileInfo(fileId) {
        return await this.filesCollection.findById(fileId);
    }
}

module.exports = FileStorage;