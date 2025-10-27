const express = require('express');
const path = require('path');
const Database = require('./Database');
const WorkerManager = require('./WorkerManager');

class Server {
  constructor(options) {
    this.options = {
      port: 6458,
      host: 'localhost',
      threadCount: 1,
      dataPath: './data',
      redisUrl: 'redis://localhost:6379',
      encryptionKey: 'default-encryption-key',
      ...options
    };
    this.app = express();
    this.server = null;
    this.workerManager = new WorkerManager(this.options);
    this.databases = new Map();
    this.setupExpress();
  }

  setupExpress() {
    this.app.use(express.json({ limit: '50mb' }));
    this.app.use(express.urlencoded({ extended: true }));

    // Database management endpoints
    this.app.post('/api/db/:dbName/connect', this.connectDatabase.bind(this));
    this.app.post('/api/db/:dbName/disconnect', this.disconnectDatabase.bind(this));
    this.app.get('/api/db/:dbName/stats', this.getDatabaseStats.bind(this));

    // Collection endpoints
    this.app.post('/api/db/:dbName/collection/:collectionName', this.createCollection.bind(this));
    this.app.delete('/api/db/:dbName/collection/:collectionName', this.dropCollection.bind(this));

    // Document endpoints (using worker threads)
    this.app.post('/api/db/:dbName/collection/:collectionName/insert', this.insertDocuments.bind(this));
    this.app.get('/api/db/:dbName/collection/:collectionName/find', this.findDocuments.bind(this));
    this.app.get('/api/db/:dbName/collection/:collectionName/findOne', this.findOneDocument.bind(this));
    this.app.put('/api/db/:dbName/collection/:collectionName/update', this.updateDocuments.bind(this));
    this.app.delete('/api/db/:dbName/collection/:collectionName/delete', this.deleteDocuments.bind(this));

    // Index endpoints
    this.app.post('/api/db/:dbName/collection/:collectionName/index', this.createIndex.bind(this));
    this.app.delete('/api/db/:dbName/collection/:collectionName/index', this.dropIndex.bind(this));

    // File storage endpoints
    this.app.post('/api/db/:dbName/files/upload', this.uploadFile.bind(this));
    this.app.get('/api/db/:dbName/files/:fileId', this.downloadFile.bind(this));
    this.app.delete('/api/db/:dbName/files/:fileId', this.deleteFile.bind(this));

    // Health check
    this.app.get('/health', (req, res) => {
      res.json({ 
        status: 'ok', 
        workers: this.options.threadCount,
        databases: Array.from(this.databases.keys())
      });
    });
  }

  async insertDocuments(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      
      const result = await this.workerManager.executeTask('insert', {
        dbName,
        collectionName,
        documents: req.body,
        options: this.options
      });

      res.json({ status: 'inserted', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async findDocuments(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      
      const results = await this.workerManager.executeTask('find', {
        dbName,
        collectionName,
        query: req.body.query,
        options: req.body.options,
        options: this.options
      });

      res.json({ results, count: results.length });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async updateDocuments(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      
      const result = await this.workerManager.executeTask('update', {
        dbName,
        collectionName,
        query: req.body.query,
        update: req.body.update,
        options: req.body.options,
        options: this.options
      });

      res.json({ status: 'updated', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async deleteDocuments(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      
      const result = await this.workerManager.executeTask('delete', {
        dbName,
        collectionName,
        query: req.body.query,
        multi: req.body.multi,
        options: this.options
      });

      res.json({ status: 'deleted', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  // Остальные методы остаются без изменений...
  async connectDatabase(req, res) {
    try {
      const { dbName } = req.params;
      const options = { ...this.options, ...req.body };
      
      if (this.databases.has(dbName)) {
        return res.json({ status: 'already_connected' });
      }

      const db = new Database(dbName, options);
      await db.init();
      this.databases.set(dbName, db);

      res.json({ status: 'connected', database: dbName });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async disconnectDatabase(req, res) {
    try {
      const { dbName } = req.params;
      const db = this.databases.get(dbName);
      
      if (db) {
        await db.close();
        this.databases.delete(dbName);
      }

      res.json({ status: 'disconnected', database: dbName });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async getDatabaseStats(req, res) {
    try {
      const { dbName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      const stats = await db.stats();
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async createCollection(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      const collection = await db.createCollection(collectionName, req.body);
      res.json({ status: 'created', collection: collectionName });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async dropCollection(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      await db.dropCollection(collectionName);
      res.json({ status: 'dropped', collection: collectionName });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async findOneDocument(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      const collection = db.collection(collectionName);
      if (!collection) {
        return res.status(404).json({ error: 'Collection not found' });
      }

      const { query } = req.body;
      const result = await collection.findOne(query || {});
      
      res.json({ result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async createIndex(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      const collection = db.collection(collectionName);
      if (!collection) {
        return res.status(404).json({ error: 'Collection not found' });
      }

      const { fields, options } = req.body;
      const result = await collection.createIndex(fields, options);
      
      res.json({ status: 'index_created', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async dropIndex(req, res) {
    try {
      const { dbName, collectionName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      const collection = db.collection(collectionName);
      if (!collection) {
        return res.status(404).json({ error: 'Collection not found' });
      }

      const { fields } = req.body;
      const result = await collection.dropIndex(fields);
      
      res.json({ status: 'index_dropped', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async uploadFile(req, res) {
    try {
      const { dbName } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      await db.fileStorage.init();
      res.json({ status: 'upload_not_implemented' });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async downloadFile(req, res) {
    try {
      const { dbName, fileId } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      res.json({ status: 'download_not_implemented', fileId });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async deleteFile(req, res) {
    try {
      const { dbName, fileId } = req.params;
      const db = this.databases.get(dbName);
      
      if (!db) {
        return res.status(404).json({ error: 'Database not found' });
      }

      await db.fileStorage.init();
      const result = await db.fileStorage.deleteFile(fileId);
      
      res.json({ status: 'deleted', result });
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }

  async start() {
    await this.workerManager.start();
    
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(this.options.port, this.options.host, (err) => {
        if (err) {
          reject(err);
        } else {
          console.log(`BsonDB server started on ${this.options.host}:${this.options.port}`);
          console.log(`Worker threads: ${this.options.threadCount}`);
          console.log(`Data directory: ${path.resolve(this.options.dataPath)}`);
          resolve();
        }
      });
    });
  }

  async stop() {
    if (this.server) {
      await new Promise((resolve) => {
        this.server.close(resolve);
      });
    }

    // Close all database connections
    for (const [name, db] of this.databases) {
      await db.close();
    }

    // Stop worker manager
    await this.workerManager.stop();
  }
}

module.exports = Server;