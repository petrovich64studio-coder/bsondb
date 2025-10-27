const fs = require('fs').promises;
const path = require('path');
const { serialize } = require('bson');

class IndexFixer {
    constructor(dataPath = './data', encryptionKey = 'default-encryption-key') {
        this.dataPath = dataPath;
        this.encryptionKey = encryptionKey;
    }

    async fixAllIndexes() {
        try {
            console.log('Starting index repair...');
            
            // Проверяем существование директории данных
            try {
                await fs.access(this.dataPath);
            } catch (error) {
                console.log('Data directory does not exist, nothing to fix.');
                return;
            }
            
            // Получаем список всех баз данных
            const databases = await fs.readdir(this.dataPath);
            
            for (const dbName of databases) {
                const dbPath = path.join(this.dataPath, dbName);
                const stat = await fs.stat(dbPath);
                
                if (stat.isDirectory()) {
                    console.log(`\nProcessing database: ${dbName}`);
                    await this.fixDatabaseIndexes(dbName);
                }
            }
            
            console.log('\n✅ Index repair completed successfully!');
        } catch (error) {
            console.error('❌ Error during index repair:', error.message);
        }
    }

    async fixDatabaseIndexes(dbName) {
        const dbPath = path.join(this.dataPath, dbName);
        
        try {
            const collections = await fs.readdir(dbPath);
            
            for (const collectionName of collections) {
                const collectionPath = path.join(dbPath, collectionName);
                const stat = await fs.stat(collectionPath);
                
                if (stat.isDirectory()) {
                    console.log(`  Fixing collection: ${collectionName}`);
                    await this.fixCollectionIndexes(dbName, collectionName);
                }
            }
        } catch (error) {
            console.error(`  Error processing database ${dbName}:`, error.message);
        }
    }

    async fixCollectionIndexes(dbName, collectionName) {
        const collectionPath = path.join(this.dataPath, dbName, collectionName);
        
        try {
            // Удаляем все поврежденные файлы индексов
            const files = await fs.readdir(collectionPath);
            const indexFiles = files.filter(f => f.startsWith('_index_') && f.endsWith('.bson'));
            
            let deletedCount = 0;
            for (const indexFile of indexFiles) {
                const filePath = path.join(collectionPath, indexFile);
                try {
                    await fs.unlink(filePath);
                    console.log(`    ✅ Deleted corrupted index: ${indexFile}`);
                    deletedCount++;
                } catch (error) {
                    console.log(`    ❌ Could not delete ${indexFile}:`, error.message);
                }
            }

            // Пересоздаем основной индексный файл если он существует
            const mainIndexFile = path.join(collectionPath, '_index.bson');
            try {
                await fs.unlink(mainIndexFile);
                console.log(`    ✅ Deleted main index file`);
                deletedCount++;
            } catch (error) {
                // Файл может не существовать - это нормально
            }

            if (deletedCount > 0) {
                console.log(`    ✅ Fixed ${deletedCount} indexes for ${collectionName}`);
            } else {
                console.log(`    ℹ️  No indexes to fix for ${collectionName}`);
            }
            
        } catch (error) {
            console.error(`    ❌ Error fixing collection ${collectionName}:`, error.message);
        }
    }
}

// Если файл запущен напрямую
if (require.main === module) {
    const fixer = new IndexFixer();
    fixer.fixAllIndexes();
}

module.exports = IndexFixer;