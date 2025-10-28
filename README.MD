# @axmat/bsondb 🗄️

A simple but powerful NoSQL database with BSON files, encryption, and file storage capabilities. ⚡

✨ **Features**

    📁 BSON File Storage: Efficient binary JSON storage

    🔐 Encryption: AES-256-GCM encryption for data security

    📎 File Storage: Store files directly in the database

    📊 Indexing: Single and multi-field indexing with Redis support
    
    🔄 Mongoose-like API: Familiar interface for MongoDB users
        
    ⚡ Multi-threading: Worker thread support for better performance
    
    🏠 Localhost Only: Secure by default, only accessible from localhost
        
    ✅ Schema Validation: Data validation with custom schemas
        
    🎣 Hooks: Pre and post hooks for data manipulation

🚀 **Installation**
*As NPM Package*

```bash
npm install @axmat/bsondb
```


*Global Installation (for CLI)*

```bash
npm install -g @axmat/bsondb
```


🏁 **Quick Start**
*Using as Module*

```javascript
const bsondb = require('@axmat/bsondb');

async function example() {
  // Connect to database (creates if doesn't exist)
  const db = await bsondb.connect('mydatabase', {
    dataPath: './data',
    encryptionKey: 'my-secret-key',
    redisUrl: 'redis://localhost:6379'
  });

  // Create collection
  const users = await db.createCollection('users');

  // Insert documents
  await users.insertOne({
    name: 'John Doe',
    email: 'john@example.com',
    age: 30
  });

  // Find documents
  const results = await users.find({ age: { $gte: 18 } });
  console.log(results);
}

example();

Using Models
javascript

const bsondb = require('@axmat/bsondb');

// Define schema
const userSchema = {
  name: { type: 'string', required: true },
  email: { type: 'string', required: true, match: /.+@.+\..+/ },
  age: { type: 'number', min: 0, max: 150 }
};

// Create model
const User = bsondb.model('User', userSchema);

async function modelExample() {
  const db = await bsondb.connect('mydatabase');
  const usersCollection = await db.createCollection('users');
  
  User.setCollection(usersCollection);

  // Add hooks
  User.pre('save', async (doc) => {
    doc.createdAt = new Date();
    return doc;
  });

  // Create document
  const user = await User.create({
    name: 'Jane Smith',
    email: 'jane@example.com',
    age: 25
  });

  // Find documents
  const users = await User.find({ age: { $gt: 20 } });
}

```

*Using CLI Server*
*Start server with custom options*

```bash
bsondb -port 6458 -thread 4 -data ./mydata -encryption-key my-key
```


**Default usage**

```bash
bsondb
```


📚 *API Reference*
🗃️ *Database Methods*

```methods bsondb
    connect(dbName, options) - Connect to database 🔌
    disconnect(dbName) - Disconnect from database 🔋
    createCollection(name, options) - Create new collection 🆕
    dropCollection(name) - Drop collection 🗑️
    collection(name) - Get collection instance 📂
    stats() - Get database statistics 📈
```
📄 *Collection Methods*

```methods bsondb
    insertOne(document) - Insert single document ➕
    insertMany(documents) - Insert multiple documents 🚀
    find(query, options) - Find documents 🔍
    findOne(query) - Find single document 👁️
    updateOne(query, update, options) - Update single document ✏️
    updateMany(query, update) - Update multiple documents 📝
    deleteOne(query) - Delete single document ❌
    deleteMany(query) - Delete multiple documents 🗑️
    count(query) - Count documents 🔢
    createIndex(fields, options) - Create index 📊
    dropIndex(fields) - Drop index 📉
```

🔍 *Query Operators*

```bsondb operators
    $eq - Equal ✅
    $ne - Not equal ❌
    $gt - Greater than ⬆️
    $gte - Greater than or equal ⬆️✅
    $lt - Less than ⬇️
    $lte - Less than or equal ⬇️✅
    $in - In array 📥
    $nin - Not in array 📤
    $regex - Regular expression 🔤
    $and - Logical AND 🔗
    $or - Logical OR 🔀
```
✏️ *Update Operators*

```bsondb operators
    $set - Set field values 🎯
    $unset - Remove fields 🗑️
    $inc - Increment field values ➕
    $push - Push to array 📥
```
🎭 *Model Methods*

```bsondb model methods
    create(data) - Create document(s) 🆕
    find(query, options) - Find documents 🔍
    findOne(query) - Find single document 👁️
    findById(id) - Find by ID 🆔
    updateOne(query, update, options) - Update document ✏️
    updateMany(query, update) - Update multiple documents 📝
    deleteOne(query) - Delete document ❌
    deleteMany(query) - Delete multiple documents 🗑️
    count(query) - Count documents 🔢
    pre(hook, fn) - Add pre-hook ⏪
    post(hook, fn) - Add post-hook ⏩
```
✅ *Schema Validation*

*Schemas support the following validators:*

```Model validate Schema
    type - Data type (string, number, boolean, etc.) 🔤
    required - Field is required ⭐
    min - Minimum value (numbers) 📏
    max - Maximum value (numbers) 📐
    enum - Allowed values 🎯
    match - Regular expression pattern 🔍
    validate - Custom validation function ⚙️
```
📎 *File Storage*

```javascript

const db = await bsondb.connect('filesdb');
await db.fileStorage.init();

// Store file
const fileId = await db.fileStorage.storeFile('./path/to/file.txt', {
  filename: 'document.txt',
  contentType: 'text/plain'
});

// Retrieve file
const fileStream = await db.fileStorage.getFileStream(fileId);
const fileData = await fileStream.read();

// Delete file
await db.fileStorage.deleteFile(fileId);

📊 Indexing
javascript

const collection = await db.createCollection('users');

// Single field index
await collection.createIndex('email');

// Compound index
await collection.createIndex(['name', 'age']);

// Unique index
await collection.createIndex('email', { unique: true });

// Drop index
await collection.dropIndex('email');

```

⌨️ *CLI Options*

```Info
    -p, --port <number> - Port number (default: 6458) 🚪
    -t, --thread <number> - Number of worker threads (default: CPU cores) ⚡
    -h, --host <string> - Host address (default: localhost) 🌐
    -d, --data <path> - Data directory path (default: ./data) 📁
    -r, --redis <string> - Redis connection string (default: redis://localhost:6379) 🛑
    -e, --encryption-key <string> - Encryption key (default: default-encryption-key) 🔑
```
🌐 *REST API*
```Info
When running as server, bsondb provides REST API:
    POST /api/db/:dbName/connect - Connect to database 🔌
    POST /api/db/:dbName/collection/:collectionName - Create collection 🆕
    POST /api/db/:dbName/collection/:collectionName/insert - Insert documents ➕
    GET /api/db/:dbName/collection/:collectionName/find - Find documents 🔍
    PUT /api/db/:dbName/collection/:collectionName/update - Update documents ✏️
    DELETE /api/db/:dbName/collection/:collectionName/delete - Delete documents ❌
```
🔒 *Security*

    🔐 Data is encrypted using AES-256-GCM

    🏠 Server only binds to localhost by default

    📊 File-based and Redis-based indexing options

    ✅ Schema validation prevents invalid data

🚀 *Performance Tips*

    📊 Use indexes for frequently queried fields
    
    ⚡ Use multi-threading for heavy workloads
        
    💾 Store large files using file storage API
    
    🛑 Use Redis for better index performance

📄 **License - MIT**
❓ Support


This README provides comprehensive documentation for using bsondb in various scenarios. The database is designed to be simple yet powerful, offering features similar to MongoDB with the convenience of file-based storage and built-in encryption. 🎉
