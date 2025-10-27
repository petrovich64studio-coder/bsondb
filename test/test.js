const bsondb = require('../index');
const fs = require('fs').promises;
const path = require('path');

async function runTests() {
  console.log('Starting BsonDB tests...\n');

  try {
    // Test 1: Connect to database
    console.log('1. Testing database connection...');
    const db = await bsondb.connect('testdb', {
      dataPath: './test-data',
      encryptionKey: 'test-key-123'
    });
    console.log('✓ Database connected successfully');

    // Test 2: Create collection
    console.log('2. Testing collection creation...');
    const usersCollection = await db.createCollection('users');
    console.log('✓ Collection created successfully');

    // Test 3: Insert documents
    console.log('3. Testing document insertion...');
    const user1 = await usersCollection.insertOne({
      name: 'John Doe',
      email: 'john@example.com',
      age: 30,
      tags: ['user', 'premium']
    });

    const user2 = await usersCollection.insertOne({
      name: 'Jane Smith',
      email: 'jane@example.com',
      age: 25,
      tags: ['user', 'admin']
    });

    console.log('✓ Documents inserted successfully');
    console.log(`  User1 ID: ${user1._id}`);
    console.log(`  User2 ID: ${user2._id}`);

    // Test 4: Find documents
    console.log('4. Testing document finding...');
    const allUsers = await usersCollection.find();
    console.log(`✓ Found ${allUsers.length} users`);

    const john = await usersCollection.findOne({ name: 'John Doe' });
    console.log('✓ Found specific user:', john.name);

    // Test 5: Update documents
    console.log('5. Testing document updates...');
    await usersCollection.updateOne(
      { name: 'John Doe' },
      { $set: { age: 31 } }
    );
    const updatedJohn = await usersCollection.findOne({ name: 'John Doe' });
    console.log(`✓ User age updated to: ${updatedJohn.age}`);

    // Test 6: Create index
    console.log('6. Testing index creation...');
    await usersCollection.createIndex(['email']);
    await usersCollection.createIndex(['name', 'age']);
    console.log('✓ Indexes created successfully');

    // Test 7: Complex queries
    console.log('7. Testing complex queries...');
    const adultUsers = await usersCollection.find({
      age: { $gte: 18 }
    });
    console.log(`✓ Found ${adultUsers.length} adult users`);

    const adminUsers = await usersCollection.find({
      tags: { $in: ['admin'] }
    });
    console.log(`✓ Found ${adminUsers.length} admin users`);

    // Test 8: Count documents
    console.log('8. Testing document counting...');
    const userCount = await usersCollection.count();
    console.log(`✓ Total users: ${userCount}`);

    // Test 9: Delete documents
    console.log('9. Testing document deletion...');
    await usersCollection.deleteOne({ name: 'John Doe' });
    const remainingCount = await usersCollection.count();
    console.log(`✓ Users after deletion: ${remainingCount}`);

    // Test 10: Model usage
    console.log('10. Testing model usage...');
    const userSchema = {
      name: { type: 'string', required: true },
      email: { type: 'string', required: true },
      age: { type: 'number', min: 0, max: 150 }
    };

    const User = bsondb.model('User', userSchema);
    User.setCollection(usersCollection);

    // Add pre-save hook
    User.pre('save', async (doc) => {
      doc.createdAt = new Date();
      return doc;
    });

    const newUser = await User.create({
      name: 'Model User',
      email: 'model@example.com',
      age: 35
    });
    console.log('✓ Model created user:', newUser.name);

    const modelUsers = await User.find({ age: { $gt: 20 } });
    console.log(`✓ Model found ${modelUsers.length} users`);

    // Test 11: Database stats
    console.log('11. Testing database statistics...');
    const stats = await db.stats();
    console.log('✓ Database stats:', stats);

    // Test 12: Cleanup
    console.log('12. Testing cleanup...');
    await db.dropCollection('users');
    await bsondb.disconnect('testdb');
    
    // Clean test data
    await fs.rm('./test-data', { recursive: true, force: true });
    console.log('✓ Cleanup completed');

    console.log('\n🎉 All tests passed successfully!');

  } catch (error) {
    console.error('❌ Test failed:', error);
    process.exit(1);
  }
}

// Run tests if this file is executed directly
if (require.main === module) {
  runTests();
}

module.exports = runTests;