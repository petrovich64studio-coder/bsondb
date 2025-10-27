const { Worker } = require('worker_threads');
const path = require('path');

class WorkerManager {
  constructor(options) {
    this.options = options;
    this.workers = new Map();
    this.taskId = 0;
    this.pendingTasks = new Map();
  }

  async start() {
    if (this.options.threadCount <= 1) {
      return; // No workers needed for single-threaded mode
    }

    for (let i = 0; i < this.options.threadCount; i++) {
      await this.createWorker(i);
    }
  }

  async createWorker(workerId) {
    return new Promise((resolve, reject) => {
      const worker = new Worker(path.join(__dirname, 'Worker.js'), {
        workerData: { ...this.options, workerId }
      });

      worker.on('message', (message) => {
        if (message.type === 'ready') {
          this.workers.set(workerId, worker);
          resolve(worker);
        } else if (message.type === 'response' || message.type === 'error') {
          const task = this.pendingTasks.get(message.id);
          if (task) {
            this.pendingTasks.delete(message.id);
            if (message.type === 'response') {
              task.resolve(message.result);
            } else {
              task.reject(new Error(message.error));
            }
          }
        }
      });

      worker.on('error', (error) => {
        console.error(`Worker ${workerId} error:`, error);
        reject(error);
      });

      worker.on('exit', (code) => {
        if (code !== 0) {
          console.error(`Worker ${workerId} stopped with exit code ${code}`);
        }
        this.workers.delete(workerId);
      });
    });
  }

  async executeTask(type, data) {
    // If single-threaded or no workers, execute directly
    if (this.options.threadCount <= 1 || this.workers.size === 0) {
      return await this.executeDirect(type, data);
    }

    const taskId = this.taskId++;
    const workerId = taskId % this.workers.size;
    const worker = this.workers.get(workerId);

    if (!worker) {
      return await this.executeDirect(type, data);
    }

    return new Promise((resolve, reject) => {
      this.pendingTasks.set(taskId, { resolve, reject });
      worker.postMessage({ type, data, id: taskId });
    });
  }

  async executeDirect(type, data) {
    // Direct execution for single-threaded mode
    const db = require('./Database');
    
    switch (type) {
      case 'connect':
        // In single-threaded mode, databases are managed by Server
        return { status: 'connected' };
      case 'insert':
        const insertDb = new db(data.dbName, data.options);
        await insertDb.init();
        const insertCollection = await insertDb.createCollection(data.collectionName);
        const insertResult = Array.isArray(data.documents)
          ? await insertCollection.insertMany(data.documents)
          : await insertCollection.insertOne(data.documents);
        await insertDb.close();
        return insertResult;
      case 'find':
        const findDb = new db(data.dbName, data.options);
        await findDb.init();
        const findCollection = findDb.collection(data.collectionName);
        const findResults = await findCollection.find(data.query || {}, data.options || {});
        await findDb.close();
        return findResults;
      case 'update':
        const updateDb = new db(data.dbName, data.options);
        await updateDb.init();
        const updateCollection = updateDb.collection(data.collectionName);
        const updateResult = data.options && data.options.multi
          ? await updateCollection.updateMany(data.query, data.update)
          : await updateCollection.updateOne(data.query, data.update, data.options);
        await updateDb.close();
        return updateResult;
      case 'delete':
        const deleteDb = new db(data.dbName, data.options);
        await deleteDb.init();
        const deleteCollection = deleteDb.collection(data.collectionName);
        const deleteResult = data.multi
          ? await deleteCollection.deleteMany(data.query)
          : await deleteCollection.deleteOne(data.query);
        await deleteDb.close();
        return deleteResult;
      default:
        throw new Error(`Unknown task type: ${type}`);
    }
  }

  async stop() {
    for (const [workerId, worker] of this.workers) {
      await worker.terminate();
    }
    this.workers.clear();
    this.pendingTasks.clear();
  }
}

module.exports = WorkerManager;