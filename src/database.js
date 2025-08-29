const sqlite3 = require('sqlite3').verbose();
const path = require('path');

class CampaignFinanceDB {
    constructor(dbPath = null) {
        this.dbPath = dbPath || path.join(__dirname, '..', 'data', 'campaign_finance.db');
        this.db = null;
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.db = new sqlite3.Database(this.dbPath, (err) => {
                if (err) {
                    reject(err);
                } else {
                    console.log('Connected to SQLite database');
                    this.db.run('PRAGMA foreign_keys = ON');
                    resolve();
                }
            });
        });
    }

    async close() {
        return new Promise((resolve, reject) => {
            if (this.db) {
                this.db.close((err) => {
                    if (err) {
                        reject(err);
                    } else {
                        console.log('Database connection closed');
                        resolve();
                    }
                });
            } else {
                resolve();
            }
        });
    }

    async createTables() {
        const tables = [
            // Main transactions table - unified structure for all schedules
            `CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_id TEXT,
                committee_code TEXT,
                committee_name TEXT,
                candidate_name TEXT,
                candidate_name_normalized TEXT,
                report_year INTEGER,
                report_date TEXT,
                party TEXT,
                office_sought TEXT,
                district TEXT,
                schedule_type TEXT,
                transaction_date TEXT,
                amount REAL,
                total_to_date REAL,
                entity_name TEXT,
                entity_name_normalized TEXT,
                entity_first_name TEXT,
                entity_last_name TEXT,
                entity_address TEXT,
                entity_city TEXT,
                entity_state TEXT,
                entity_zip TEXT,
                entity_employer TEXT,
                entity_occupation TEXT,
                transaction_type TEXT,
                purpose TEXT,
                data_source TEXT,
                folder_name TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Candidates table for normalized names and info
            `CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                normalized_name TEXT UNIQUE,
                aliases TEXT,
                party TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Entities table for contributors/payees
            `CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                normalized_name TEXT,
                first_name TEXT,
                last_name TEXT,
                address TEXT,
                city TEXT,
                state TEXT,
                zip TEXT,
                employer TEXT,
                occupation TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`,

            // Reports metadata
            `CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY,
                committee_code TEXT,
                committee_name TEXT,
                candidate_name TEXT,
                report_year INTEGER,
                filing_date TEXT,
                start_date TEXT,
                end_date TEXT,
                party TEXT,
                office_sought TEXT,
                district TEXT,
                data_source TEXT,
                folder_name TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )`
        ];

        for (const table of tables) {
            await this.run(table);
        }

        // Create indexes for better query performance
        const indexes = [
            'CREATE INDEX IF NOT EXISTS idx_transactions_candidate ON transactions(candidate_name_normalized)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_entity ON transactions(entity_name_normalized)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_year ON transactions(report_year)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_amount ON transactions(amount)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_office ON transactions(office_sought)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_district ON transactions(district)',
            'CREATE INDEX IF NOT EXISTS idx_transactions_schedule ON transactions(schedule_type)',
            'CREATE INDEX IF NOT EXISTS idx_candidates_normalized ON candidates(normalized_name)',
            'CREATE INDEX IF NOT EXISTS idx_entities_normalized ON entities(normalized_name)'
        ];

        for (const index of indexes) {
            await this.run(index);
        }

        console.log('Database tables and indexes created successfully');
    }

    async run(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.run(sql, params, function(err) {
                if (err) {
                    reject(err);
                } else {
                    resolve({ id: this.lastID, changes: this.changes });
                }
            });
        });
    }

    async get(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.get(sql, params, (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row);
                }
            });
        });
    }

    async all(sql, params = []) {
        return new Promise((resolve, reject) => {
            this.db.all(sql, params, (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });
    }

    async insertTransaction(transaction) {
        const sql = `
            INSERT INTO transactions (
                report_id, committee_code, committee_name, candidate_name, 
                candidate_name_normalized, report_year, report_date, party, 
                office_sought, district, schedule_type, transaction_date, 
                amount, total_to_date, entity_name, entity_name_normalized,
                entity_first_name, entity_last_name, entity_address, entity_city,
                entity_state, entity_zip, entity_employer, entity_occupation,
                transaction_type, purpose, data_source, folder_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;
        
        return await this.run(sql, [
            transaction.report_id,
            transaction.committee_code,
            transaction.committee_name,
            transaction.candidate_name,
            transaction.candidate_name_normalized,
            transaction.report_year,
            transaction.report_date,
            transaction.party,
            transaction.office_sought,
            transaction.district,
            transaction.schedule_type,
            transaction.transaction_date,
            transaction.amount,
            transaction.total_to_date,
            transaction.entity_name,
            transaction.entity_name_normalized,
            transaction.entity_first_name,
            transaction.entity_last_name,
            transaction.entity_address,
            transaction.entity_city,
            transaction.entity_state,
            transaction.entity_zip,
            transaction.entity_employer,
            transaction.entity_occupation,
            transaction.transaction_type,
            transaction.purpose,
            transaction.data_source,
            transaction.folder_name
        ]);
    }

    async insertReport(report) {
        const sql = `
            INSERT OR REPLACE INTO reports (
                report_id, committee_code, committee_name, candidate_name,
                report_year, filing_date, start_date, end_date, party,
                office_sought, district, data_source, folder_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;
        
        return await this.run(sql, [
            report.report_id,
            report.committee_code,
            report.committee_name,
            report.candidate_name,
            report.report_year,
            report.filing_date,
            report.start_date,
            report.end_date,
            report.party,
            report.office_sought,
            report.district,
            report.data_source,
            report.folder_name
        ]);
    }

    async getTransactionCount() {
        const result = await this.get('SELECT COUNT(*) as count FROM transactions');
        return result.count;
    }

    async getYearRange() {
        const result = await this.get('SELECT MIN(report_year) as min_year, MAX(report_year) as max_year FROM transactions');
        return { min: result.min_year, max: result.max_year };
    }

    async clearData() {
        await this.run('DELETE FROM transactions');
        await this.run('DELETE FROM reports');
        await this.run('DELETE FROM candidates');
        await this.run('DELETE FROM entities');
        console.log('All data cleared from database');
    }
}

module.exports = CampaignFinanceDB;