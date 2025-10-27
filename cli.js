#!/usr/bin/env node

const { Command } = require('commander');
const Server = require('./lib/Server');
const path = require('path');
const os = require('os');

// Загружаем .env файл если существует
try {
    require('dotenv').config();
} catch (error) {
    // dotenv может быть не установлен - это нормально
}

const program = new Command();

program
    .name('bsondb')
    .description('BsonDB NoSQL Database Server - A simple embedded NoSQL database with BSON files and encryption')
    .version('1.0.0')
    .showHelpAfterError('(add --help for additional information)');

program
    .option('-p, --port <number>', 'port number (default: 6458)', '6458')
    .option('-t, --thread <number>', 'number of worker threads (default: CPU cores)', os.cpus().length.toString())
    .option('-h, --host <string>', 'host address (default: localhost)', 'localhost')
    .option('-d, --data <path>', 'data directory path (default: ./data)', './data')
    .option('-r, --redis <string>', 'redis connection string for indexes (default: redis://localhost:6379)', 'redis://localhost:6379')
    .option('-e, --encryption-key <string>', 'encryption key for data security', process.env.BSONDB_ENCRYPTION_KEY || null)
    .action(async (options) => {
        try {
            const server = new Server({
                port: parseInt(options.port),
                threadCount: parseInt(options.thread),
                host: options.host,
                dataPath: path.resolve(options.data),
                redisUrl: options.redis,
                encryptionKey: options.encryptionKey
            });

            await server.start();
            
            console.log('┌─────────────────────────────────────────────');
            console.log('│            BsonDB Server Started           ');
            console.log('├─────────────────────────────────────────────');
            console.log(`│ Port: ${options.port.toString().padEnd(35)} `);
            console.log(`│ Host: ${options.host.padEnd(35)} `);
            console.log(`│ Workers: ${options.thread.toString().padEnd(32)} `);
            console.log(`│ Data: ${path.resolve(options.data).padEnd(34)} `);
            
            if (options.encryptionKey && options.encryptionKey !== 'default-encryption-key') {
                console.log(`│ Encryption: ${'Enabled'.padEnd(30)} `);
            } else {
                console.log(`│ Encryption: ${'Disabled'.padEnd(29)} `);
            }
            
            console.log('└─────────────────────────────────────────────');
            console.log('⏹️  Press Ctrl+C to stop the server\n');

            process.on('SIGINT', async () => {
                console.log('\n🛑 Shutting down server gracefully...');
                await server.stop();
                console.log('✅ Server stopped successfully');
                process.exit(0);
            });

            process.on('SIGTERM', async () => {
                console.log('\n🛑 Received SIGTERM, shutting down...');
                await server.stop();
                process.exit(0);
            });

        } catch (error) {
            console.error('❌ Failed to start server:', error.message);
            console.log('\n💡 Troubleshooting tips:');
            console.log('   • Check if port 6458 is available');
            console.log('   • Ensure data directory is writable');
            console.log('   • Verify Redis connection if using indexes');
            process.exit(1);
        }
    });

// Добавляем команду для проверки версии
program
    .command('version')
    .description('Show version information')
    .action(() => {
        console.log('BsonDB v1.0.0');
        console.log('Node.js', process.version);
    });

// Добавляем команду для проверки здоровья
program
    .command('health')
    .description('Check database health')
    .action(async () => {
        try {
            const http = require('http');
            
            const options = {
                hostname: 'localhost',
                port: 6458,
                path: '/health',
                method: 'GET',
                timeout: 5000
            };
            
            const req = http.request(options, (res) => {
                let data = '';
                
                res.on('data', (chunk) => {
                    data += chunk;
                });
                
                res.on('end', () => {
                    try {
                        const health = JSON.parse(data);
                        console.log('✅ BsonDB server is healthy');
                        console.log('Health status:', health);
                    } catch (e) {
                        console.log('⚠️  Server is running but health check format is invalid');
                    }
                });
            });
            
            req.on('error', (error) => {
                console.log('❌ BsonDB server is not running or not accessible');
                console.log('Error:', error.message);
            });
            
            req.on('timeout', () => {
                console.log('❌ Health check timeout - server may be busy or not running');
                req.destroy();
            });
            
            req.end();
            
        } catch (error) {
            console.log('❌ Health check failed:', error.message);
        }
    });

program.parse();