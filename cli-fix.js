#!/usr/bin/env node

const { Command } = require('commander');
const Server = require('./lib/Server');
const path = require('path');
const os = require('os');

// Загружаем IndexFixer правильно
const IndexFixer = require('./fix-indexes');

const program = new Command();

program
    .name('bsondb')
    .description('BsonDB NoSQL Database Server')
    .version('1.0.0');

program
    .option('-p, --port <number>', 'port number', '6458')
    .option('-t, --thread <number>', 'number of worker threads', os.cpus().length.toString())
    .option('-h, --host <string>', 'host address', 'localhost')
    .option('-d, --data <path>', 'data directory path', './data')
    .option('-r, --redis <string>', 'redis connection string', 'redis://localhost:6379')
    .option('-e, --encryption-key <string>', 'encryption key', 'default-encryption-key')
    .option('-f, --fix-indexes', 'fix corrupted indexes before starting', false)
    .action(async (options) => {
        try {
            // Если нужно исправить индексы
            if (options.fixIndexes) {
                console.log('🔧 Fixing corrupted indexes...');
                const fixer = new IndexFixer(options.data, options.encryptionKey);
                await fixer.fixAllIndexes();
                console.log('✅ Index fix completed');
            }

            const server = new Server({
                port: parseInt(options.port),
                threadCount: parseInt(options.thread),
                host: options.host,
                dataPath: path.resolve(options.data),
                redisUrl: options.redis,
                encryptionKey: options.encryptionKey
            });

            await server.start();
            console.log(`🚀 BsonDB server started on ${options.host}:${options.port}`);
            console.log(`👷 Worker threads: ${options.thread}`);
            console.log(`💾 Data directory: ${path.resolve(options.data)}`);
            console.log(`🔐 Encryption: ${options.encryptionKey ? 'Enabled' : 'Disabled'}`);

            process.on('SIGINT', async () => {
                console.log('\n🛑 Shutting down server...');
                await server.stop();
                process.exit(0);
            });

        } catch (error) {
            console.error('❌ Failed to start server:', error.message);
            process.exit(1);
        }
    });

program.parse();