const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const CampaignFinanceDB = require('./database');
const NameNormalizer = require('./name-normalizer');

class DataProcessor {
    constructor() {
        this.db = new CampaignFinanceDB();
        this.nameNormalizer = new NameNormalizer();
        
        // Define which schedules are transactional (avoid summaries)
        this.transactionalSchedules = new Set([
            'ScheduleA', 'ScheduleB', 'ScheduleC', 'ScheduleD', 'ScheduleF', 'ScheduleI'
        ]);
        
        // Skip summary schedules and loans to avoid double-counting
        this.skipSchedules = new Set([
            'ScheduleG', 'ScheduleH', 'ScheduleE'  // E is loans
        ]);
    }

    async processAllData() {
        await this.db.connect();
        await this.db.createTables();
        
        const dataDir = path.join(__dirname, '..', 'data');
        const folders = fs.readdirSync(dataDir).filter(folder => 
            fs.statSync(path.join(dataDir, folder)).isDirectory()
        );
        
        console.log(`Processing ${folders.length} folders...`);
        
        for (const folder of folders.sort()) {
            const isOld = this.isOldFolder(folder);
            console.log(`\nProcessing ${isOld ? 'old' : 'new'} folder: ${folder}`);
            
            if (isOld) {
                await this.processOldFolder(folder);
            } else {
                await this.processNewFolder(folder);
            }
        }
        
        const count = await this.db.getTransactionCount();
        const yearRange = await this.db.getYearRange();
        console.log(`\nProcessing complete! ${count} transactions loaded covering ${yearRange.min}-${yearRange.max}`);
    }

    isOldFolder(folder) {
        return /^\d{4}$/.test(folder) && parseInt(folder) <= 2011;
    }

    async processOldFolder(folder) {
        const folderPath = path.join(__dirname, '..', 'data', folder);
        const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.csv'));
        
        console.log(`  Found ${files.length} CSV files in ${folder}`);
        
        for (const file of files) {
            const scheduleType = this.extractScheduleType(file);
            
            if (this.skipSchedules.has(scheduleType)) {
                console.log(`    Skipping ${file} (summary/loan schedule)`);
                continue;
            }
            
            if (!this.transactionalSchedules.has(scheduleType)) {
                console.log(`    Skipping ${file} (unknown schedule type)`);
                continue;
            }
            
            console.log(`    Processing ${file}...`);
            await this.processOldCSV(folderPath, file, folder, scheduleType);
        }
    }

    async processNewFolder(folder) {
        const folderPath = path.join(__dirname, '..', 'data', folder);
        const files = fs.readdirSync(folderPath).filter(f => f.endsWith('.csv'));
        
        console.log(`  Found ${files.length} CSV files in ${folder}`);
        
        // First process Report.csv to get base report data
        const reportFile = files.find(f => f.toLowerCase() === 'report.csv');
        const reports = new Map();
        
        if (reportFile) {
            console.log(`    Processing ${reportFile}...`);
            const reportData = await this.loadCSV(path.join(folderPath, reportFile));
            
            for (const row of reportData) {
                reports.set(row.ReportId, {
                    report_id: row.ReportId,
                    committee_code: row.CommitteeCode,
                    committee_name: row.CommitteeName,
                    candidate_name: row.CandidateName,
                    report_year: parseInt(row.ReportYear) || null,
                    filing_date: row.FilingDate,
                    start_date: row.StartDate,
                    end_date: row.EndDate,
                    party: row.Party,
                    office_sought: row.OfficeSought,
                    district: row.District,
                    data_source: 'new',
                    folder_name: folder
                });
                
                await this.db.insertReport(reports.get(row.ReportId));
            }
        }
        
        // Process transactional schedules
        for (const file of files) {
            const scheduleType = this.extractScheduleType(file);
            
            if (file.toLowerCase() === 'report.csv') continue; // Already processed
            
            if (this.skipSchedules.has(scheduleType)) {
                console.log(`    Skipping ${file} (summary/loan schedule)`);
                continue;
            }
            
            if (!this.transactionalSchedules.has(scheduleType)) {
                console.log(`    Skipping ${file} (unknown schedule type)`);
                continue;
            }
            
            console.log(`    Processing ${file}...`);
            await this.processNewCSV(folderPath, file, folder, scheduleType, reports);
        }
    }

    extractScheduleType(filename) {
        const match = filename.match(/^(Schedule[A-Z])(_PAC)?\.csv$/i);
        return match ? match[1] : filename.replace('.csv', '');
    }

    async processOldCSV(folderPath, filename, folder, scheduleType) {
        const filePath = path.join(folderPath, filename);
        const rows = await this.loadCSV(filePath);
        
        let processedCount = 0;
        
        for (const row of rows) {
            const transaction = this.mapOldRowToTransaction(row, folder, scheduleType);
            
            if (transaction && transaction.amount && !isNaN(parseFloat(transaction.amount))) {
                await this.db.insertTransaction(transaction);
                processedCount++;
            }
        }
        
        console.log(`      Processed ${processedCount} transactions from ${filename}`);
    }

    async processNewCSV(folderPath, filename, folder, scheduleType, reports) {
        const filePath = path.join(folderPath, filename);
        const rows = await this.loadCSV(filePath);
        
        let processedCount = 0;
        
        for (const row of rows) {
            const transaction = this.mapNewRowToTransaction(row, folder, scheduleType, reports);
            
            if (transaction && transaction.amount && !isNaN(parseFloat(transaction.amount))) {
                await this.db.insertTransaction(transaction);
                processedCount++;
            }
        }
        
        console.log(`      Processed ${processedCount} transactions from ${filename}`);
    }

    mapOldRowToTransaction(row, folder, scheduleType) {
        // Extract amount - try different column names
        let amount = null;
        const amountFields = ['Trans Amount', 'TRANS_AMNT', 'Trans_Amount'];
        for (const field of amountFields) {
            if (row[field] && !isNaN(parseFloat(row[field]))) {
                amount = parseFloat(row[field]);
                break;
            }
        }
        
        if (!amount) return null;
        
        // Extract names
        const candidateName = this.extractCandidateName(row);
        const entityName = this.extractEntityName(row);
        
        return {
            report_id: row['Committee Code'] || row['COMMITTEE_CODE'],
            committee_code: row['Committee Code'] || row['COMMITTEE_CODE'],
            committee_name: row['Committee Name'] || row['COMMITTEE_NAME'],
            candidate_name: candidateName,
            candidate_name_normalized: this.nameNormalizer.normalizeName(candidateName),
            report_year: parseInt(row['Report Year'] || row['REPORT_YEAR']) || parseInt(folder),
            report_date: row['Date Received'] || row['DATE_RECEIVED'],
            party: row['Party'] || row['Party_Desc'],
            office_sought: row['Office Code'] || row['OFFICE_CODE'],
            district: row['Office Sub Code'] || row['OFFICE_SUB_CODE'],
            schedule_type: scheduleType,
            transaction_date: row['Trans Date'] || row['TRANS_DATE'],
            amount: amount,
            total_to_date: parseFloat(row['Trans Agg To Date'] || row['TRANS_AGG_TO_DATE']) || null,
            entity_name: entityName,
            entity_name_normalized: this.nameNormalizer.normalizeName(entityName),
            entity_first_name: row['First Name'] || row['FIRSTNAME'],
            entity_last_name: row['Last Name'] || row['LASTNAME'],
            entity_address: row['Entity Address'] || row['ENTITY_ADDRESS'],
            entity_city: row['Entity City'] || row['ENTITY_CITY'],
            entity_state: row['Entity State'] || row['ENTITY_STATE'],
            entity_zip: row['Entity Zip'] || row['ENTITY_ZIP'],
            entity_employer: row['Entity Employer'] || row['ENTITY_EMPLOYER'],
            entity_occupation: row['Entity Occupation'] || row['ENTITY_OCCUPATION'],
            transaction_type: row['Trans Type'] || row['TRANS_TYPE'],
            purpose: row['Trans Service Or Goods'] || row['TRANS_ITEM_OR_SERVICE'],
            data_source: 'old',
            folder_name: folder
        };
    }

    mapNewRowToTransaction(row, folder, scheduleType, reports) {
        const amount = parseFloat(row.Amount);
        if (!amount || isNaN(amount)) return null;
        
        const reportInfo = reports.get(row.ReportId) || {};
        const entityName = this.buildEntityName(row);
        
        return {
            report_id: row.ReportId,
            committee_code: reportInfo.committee_code,
            committee_name: reportInfo.committee_name,
            candidate_name: reportInfo.candidate_name,
            candidate_name_normalized: this.nameNormalizer.normalizeName(reportInfo.candidate_name),
            report_year: reportInfo.report_year,
            report_date: reportInfo.filing_date,
            party: reportInfo.party,
            office_sought: reportInfo.office_sought,
            district: reportInfo.district,
            schedule_type: scheduleType,
            transaction_date: row.TransactionDate,
            amount: amount,
            total_to_date: parseFloat(row.TotalToDate) || null,
            entity_name: entityName,
            entity_name_normalized: this.nameNormalizer.normalizeName(entityName),
            entity_first_name: row.FirstName,
            entity_last_name: row.LastOrCompanyName,
            entity_address: row.AddressLine1,
            entity_city: row.City,
            entity_state: row.StateCode,
            entity_zip: row.ZipCode,
            entity_employer: row.NameOfEmployer,
            entity_occupation: row.OccupationOrTypeOfBusiness,
            transaction_type: scheduleType,
            purpose: row.ItemOrService || row.ProductOrService || row.PurposeOfObligation,
            data_source: 'new',
            folder_name: folder
        };
    }

    extractCandidateName(row) {
        // Try to build candidate name from available fields
        const firstName = row['First Name'] || row['FIRSTNAME'] || '';
        const lastName = row['Last Name'] || row['LASTNAME'] || '';
        const middleName = row['Middle Name'] || row['MIDDLENAME'] || '';
        
        if (firstName && lastName) {
            return `${firstName} ${middleName} ${lastName}`.replace(/\s+/g, ' ').trim();
        }
        
        // Fall back to committee name
        return row['Committee Name'] || row['COMMITTEE_NAME'] || '';
    }

    extractEntityName(row) {
        const entityName = row['Entity Name'] || row['ENTITY_NAME'];
        if (entityName) return entityName;
        
        // Build from name parts
        const firstName = row['First Name'] || row['FIRSTNAME'] || '';
        const lastName = row['Last Name'] || row['LASTNAME'] || '';
        const middleName = row['Middle Name'] || row['MIDDLENAME'] || '';
        
        if (firstName || lastName) {
            return `${firstName} ${middleName} ${lastName}`.replace(/\s+/g, ' ').trim();
        }
        
        return '';
    }

    buildEntityName(row) {
        if (row.LastOrCompanyName) {
            if (row.FirstName) {
                const middle = row.MiddleName ? ` ${row.MiddleName}` : '';
                return `${row.FirstName}${middle} ${row.LastOrCompanyName}`;
            }
            return row.LastOrCompanyName;
        }
        return '';
    }

    async loadCSV(filePath) {
        return new Promise((resolve, reject) => {
            const rows = [];
            
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', (row) => rows.push(row))
                .on('end', () => resolve(rows))
                .on('error', reject);
        });
    }

    async close() {
        await this.db.close();
    }
}

module.exports = DataProcessor;