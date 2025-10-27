const { parentPort, workerData } = require('worker_threads');
const Database = require('./Database');

class WorkerThread {
  constructor(options) {
    this.options = options;
    this.databases = new Map();
  }

  async handleMessage(message) {
    try {
      const { type, data, id } = message;
      let result;

      switch (type) {
        case 'connect':
          result = await this.connectDatabase(data.dbName, data.options);
          break;
        case 'insert':
          result = await this.insertDocuments(data.dbName, data.collectionName, data.documents, data.options);
          break;
        case 'find':
          result = await this.findDocuments(data.dbName, data.collectionName, data.query, data.options);
          break;
        case 'update':
          result = await this.updateDocuments(data.dbName, data.collectionName, data.query, data.update, data.options);
          break;
        case 'delete':
          result = await this.deleteDocuments(data.dbName, data.collectionName, data.query, data.multi, data.options);
          break;
        default:
          throw new Error(`Unknown message type: ${type}`);
      }

      parentPort.postMessage({ type: 'response', id, result });
    } catch (error) {
      parentPort.postMessage({ type: 'error', id, error: error.message });
    }
  }

  async connectDatabase(dbName, options) {
    if (this.databases.has(dbName)) {
      return { status: 'already_connected' };
    }

    const dbOptions = { ...this.options, ...options };
    const db = new Database(dbName, dbOptions);
    await db.init();
    this.databases.set(dbName, db);

    return { status: 'connected' };
  }

  async insertDocuments(dbName, collectionName, documents, options) {
    let db = this.databases.get(dbName);
    let shouldClose = false;

    if (!db) {
      // Create temporary connection
      db = new Database(dbName, { ...this.options, ...options });
      await db.init();
      shouldClose = true;
    }

    try {
      const collection = await db.createCollection(collectionName);
      const result = Array.isArray(documents)
        ? await collection.insertMany(documents)
        : await collection.insertOne(documents);

      return result;
    } finally {
      if (shouldClose && db) {
        await db.close();
      }
    }
  }

  async findDocuments(dbName, collectionName, query, options) {
    let db = this.databases.get(dbName);
    let shouldClose = false;

    if (!db) {
      // Create temporary connection
      db = new Database(dbName, { ...this.options, ...options });
      await db.init();
      shouldClose = true;
    }

    try {
      const collection = db.collection(collectionName);
      if (!collection) {
        throw new Error('Collection not found');
      }

      const results = await collection.find(query || {}, options || {});
      return results;
    } finally {
      if (shouldClose && db) {
        await db.close();
      }
    }
  }

  async updateDocuments(dbName, collectionName, query, update, options) {
    let db = this.databases.get(dbName);
    let shouldClose = false;

    if (!db) {
      // Create temporary connection
      db = new Database(dbName, { ...this.options, ...options });
      await db.init();
      shouldClose = true;
    }

    try {
      const collection = db.collection(collectionName);
      if (!collection) {
        throw new Error('Collection not found');
      }

      const result = options && options.multi
        ? await collection.updateMany(query, update)
        : await collection.updateOne(query, update, options);

      return result;
    } finally {
      if (shouldClose && db) {
        await db.close();
      }
    }
  }

  async deleteDocuments(dbName, collectionName, query, multi, options) {
    let db = this.databases.get(dbName);
    let shouldClose = false;

    if (!db) {
      // Create temporary connection
      db = new Database(dbName, { ...this.options, ...options });
      await db.init();
      shouldClose = true;
    }

    try {
      const collection = db.collection(collectionName);
      if (!collection) {
        throw new Error('Collection not found');
      }

      const result = multi
        ? await collection.deleteMany(query)
        : await collection.deleteOne(query);

      return result;
    } finally {
      if (shouldClose && db) {
        await db.close();
      }
    }
  }
}

// Initialize worker
const worker = new WorkerThread(workerData);

parentPort.on('message', (message) => {
  worker.handleMessage(message);
});

// Signal that worker is ready
parentPort.postMessage({ type: 'ready' });