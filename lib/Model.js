const { EventEmitter } = require('events');

class Model extends EventEmitter {
  constructor(name, schema, options = {}) {
    super();
    this.name = name;
    this.schema = schema;
    this.options = options;
    this.collection = null;
    this.hooks = new Map();
  }

  setCollection(collection) {
    this.collection = collection;
  }

  async validate(document) {
    const errors = [];
    
    for (const [field, rules] of Object.entries(this.schema)) {
      const value = document[field];
      
      // Если поле обязательно и не определено
      if (rules.required && (value === undefined || value === null)) {
        errors.push(`${field} is required`);
        continue;
      }
      
      // Если значение есть, проверяем правила
      if (value !== undefined && value !== null) {
        // Проверка типа с более гибким подходом
        if (rules.type) {
          let isValidType = false;
          
          switch (rules.type) {
            case 'string':
              isValidType = typeof value === 'string';
              break;
            case 'number':
              isValidType = typeof value === 'number' && !isNaN(value);
              break;
            case 'boolean':
              isValidType = typeof value === 'boolean';
              break;
            case 'array':
              isValidType = Array.isArray(value);
              // Автоматически преобразуем строку в массив, если нужно
              if (!isValidType && typeof value === 'string') {
                document[field] = value.split(',').map(item => item.trim()).filter(item => item);
                isValidType = true;
              }
              break;
            case 'object':
              isValidType = typeof value === 'object' && !Array.isArray(value) && value !== null;
              break;
            default:
              isValidType = true;
          }
          
          if (!isValidType) {
            errors.push(`${field} must be of type ${rules.type}`);
          }
        }
        
        // Проверка минимального значения
        if (rules.min !== undefined && value < rules.min) {
          errors.push(`${field} must be at least ${rules.min}`);
        }
        
        // Проверка максимального значения
        if (rules.max !== undefined && value > rules.max) {
          errors.push(`${field} must be at most ${rules.max}`);
        }
        
        // Проверка enum
        if (rules.enum && !rules.enum.includes(value)) {
          errors.push(`${field} must be one of: ${rules.enum.join(', ')}`);
        }
        
        // Проверка регулярным выражением
        if (rules.match && typeof value === 'string' && !rules.match.test(value)) {
          errors.push(`${field} must match pattern ${rules.match}`);
        }
        
        // Пользовательская валидация
        if (rules.validate && !rules.validate(value)) {
          errors.push(`${field} failed custom validation`);
        }
      }
    }
    
    if (errors.length > 0) {
      throw new Error(`Validation failed: ${errors.join(', ')}`);
    }
    
    return true;
  }

  async applyHooks(type, data) {
    const hooks = this.hooks.get(type) || [];
    let result = data;
    
    for (const hook of hooks) {
      result = await hook(result);
    }
    
    return result;
  }

  pre(method, fn) {
    if (!this.hooks.has(method)) {
      this.hooks.set(method, []);
    }
    this.hooks.get(method).push(fn);
  }

  post(method, fn) {
    this.on(method, fn);
  }

  async create(data) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const documents = Array.isArray(data) ? data : [data];
    const validatedDocs = [];

    for (let doc of documents) {
      // Создаем копию документа для обработки
      let processedDoc = { ...doc };
      
      // Применяем pre-хуки перед валидацией
      processedDoc = await this.applyHooks('save', processedDoc);
      
      // Валидируем документ
      await this.validate(processedDoc);
      
      validatedDocs.push(processedDoc);
    }

    const result = Array.isArray(data) 
      ? await this.collection.insertMany(validatedDocs)
      : await this.collection.insertOne(validatedDocs[0]);

    this.emit('save', result);
    return result;
  }

  async find(query = {}, options = {}) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const results = await this.collection.find(query, options);
    const processedResults = await this.applyHooks('find', results);
    
    this.emit('find', processedResults);
    return processedResults;
  }

  async findOne(query = {}, options = {}) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const result = await this.collection.findOne(query, options);
    if (result) {
      const processedResult = await this.applyHooks('findOne', result);
      this.emit('findOne', processedResult);
      return processedResult;
    }
    return null;
  }

  async findById(id, options = {}) {
    return await this.findOne({ _id: id }, options);
  }

  async updateOne(query, update, options = {}) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const processedUpdate = await this.applyHooks('update', update);
    const result = await this.collection.updateOne(query, processedUpdate, options);
    
    if (result) {
      this.emit('update', result);
    }
    return result;
  }

  async updateMany(query, update) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const processedUpdate = await this.applyHooks('update', update);
    const results = await this.collection.updateMany(query, processedUpdate);
    
    if (results.length > 0) {
      this.emit('update', results);
    }
    return results;
  }

  async deleteOne(query) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const result = await this.collection.deleteOne(query);
    if (result) {
      this.emit('delete', result);
    }
    return result;
  }

  async deleteMany(query) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }

    const results = await this.collection.deleteMany(query);
    if (results.length > 0) {
      this.emit('delete', results);
    }
    return results;
  }

  async count(query = {}) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }
    return await this.collection.count(query);
  }

  async createIndex(fields, options = {}) {
    if (!this.collection) {
      throw new Error('Model not connected to collection');
    }
    return await this.collection.createIndex(fields, options);
  }
}

module.exports = Model;