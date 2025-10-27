const crypto = require('crypto');

class Encryption {
    constructor(key = null) {
        this.enabled = false;
        this.key = null;
        this.algorithm = 'aes-256-cbc';
        this.ivLength = 16;

        if (key && key !== 'default-encryption-key') {
            this.enabled = true;
            this.key = crypto.createHash('sha256').update(String(key)).digest();
            console.log('🔐 Encryption: Enabled');
        } else {
            console.log('🔓 Encryption: Disabled (no valid key provided)');
        }
    }

    encrypt(data) {
        // Если шифрование отключено, возвращаем данные как есть
        if (!this.enabled) {
            return data;
        }

        try {
            // Генерируем случайный IV
            const iv = crypto.randomBytes(this.ivLength);
            
            // Создаем cipher
            const cipher = crypto.createCipheriv(this.algorithm, this.key, iv);
            
            // Шифруем данные
            let encrypted = cipher.update(data);
            encrypted = Buffer.concat([encrypted, cipher.final()]);
            
            // Возвращаем IV + encrypted data
            return Buffer.concat([iv, encrypted]);
        } catch (error) {
            console.error('❌ Encryption failed:', error.message);
            // В случае ошибки возвращаем незашифрованные данные
            return data;
        }
    }

    decrypt(encryptedData) {
        // Если шифрование отключено, возвращаем данные как есть
        if (!this.enabled) {
            return encryptedData;
        }

        try {
            // Проверяем, что данные достаточно длинные для IV
            if (!encryptedData || encryptedData.length < this.ivLength) {
                console.warn('⚠️ Invalid encrypted data length, returning as-is');
                return encryptedData;
            }

            // Извлекаем IV (первые 16 байт)
            const iv = encryptedData.slice(0, this.ivLength);
            
            // Извлекаем зашифрованные данные (остальные байты)
            const encrypted = encryptedData.slice(this.ivLength);
            
            // Создаем decipher
            const decipher = crypto.createDecipheriv(this.algorithm, this.key, iv);
            
            // Дешифруем данные
            let decrypted = decipher.update(encrypted);
            decrypted = Buffer.concat([decrypted, decipher.final()]);
            
            return decrypted;
        } catch (error) {
            console.error('❌ Decryption failed:', error.message);
            // В случае ошибки возвращаем данные как есть
            return encryptedData;
        }
    }

    encryptText(text) {
        if (!this.enabled) {
            return text;
        }

        try {
            const buffer = Buffer.from(text, 'utf8');
            const encrypted = this.encrypt(buffer);
            return encrypted.toString('base64');
        } catch (error) {
            console.error('❌ Text encryption failed:', error.message);
            return text;
        }
    }

    decryptText(encryptedText) {
        if (!this.enabled) {
            return encryptedText;
        }

        try {
            const buffer = Buffer.from(encryptedText, 'base64');
            const decrypted = this.decrypt(buffer);
            return decrypted.toString('utf8');
        } catch (error) {
            console.error('❌ Text decryption failed:', error.message);
            return encryptedText;
        }
    }

    // Метод для проверки работы шифрования
    test() {
        if (!this.enabled) {
            console.log('🔓 Encryption test: Disabled');
            return true;
        }

        try {
            const testData = 'Hello, BsonDB Encryption Test!';
            const encrypted = this.encryptText(testData);
            const decrypted = this.decryptText(encrypted);
            
            if (decrypted !== testData) {
                throw new Error('Encryption/decryption mismatch');
            }
            
            console.log('✅ Encryption test passed');
            return true;
        } catch (error) {
            console.error('❌ Encryption test failed:', error.message);
            return false;
        }
    }

    isEnabled() {
        return this.enabled;
    }
}

module.exports = Encryption;