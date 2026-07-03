const CampaignFinanceDB = require('./database');
const NameNormalizer = require('./name-normalizer');

class QueryEngine {
    constructor() {
        this.db = new CampaignFinanceDB();
        this.nameNormalizer = new NameNormalizer();
    }

    async connect() {
        await this.db.connect();
    }

    async close() {
        await this.db.close();
    }

    // Main query function with flexible filtering and aggregation
    async sumBy(options = {}) {
        const {
            groupBy = ['candidate_name'],
            year = null,
            years = null,
            topN = null,
            filters = {},
            scheduleTypes = null,
            orderBy = 'total_amount',
            orderDirection = 'DESC'
        } = options;

        let whereConditions = [];
        let params = [];
        let paramIndex = 1;

        // Year filtering
        if (year) {
            whereConditions.push(`report_year = $${paramIndex}`);
            params.push(year);
            paramIndex++;
        } else if (years && Array.isArray(years)) {
            const yearPlaceholders = years.map(() => `$${paramIndex++}`).join(',');
            whereConditions.push(`report_year IN (${yearPlaceholders})`);
            params.push(...years);
        }

        // Schedule type filtering
        if (scheduleTypes && Array.isArray(scheduleTypes)) {
            const typePlaceholders = scheduleTypes.map(() => `$${paramIndex++}`).join(',');
            whereConditions.push(`schedule_type IN (${typePlaceholders})`);
            params.push(...scheduleTypes);
        }

        // Dynamic filters
        for (const [field, value] of Object.entries(filters)) {
            if (value === null || value === undefined) continue;
            
            if (typeof value === 'string') {
                // Case-insensitive substring search for strings
                whereConditions.push(`UPPER(${field}) LIKE UPPER($${paramIndex})`);
                params.push(`%${value}%`);
                paramIndex++;
            } else if (Array.isArray(value)) {
                // IN clause for arrays
                const placeholders = value.map(() => `$${paramIndex++}`).join(',');
                whereConditions.push(`${field} IN (${placeholders})`);
                params.push(...value);
            } else {
                // Exact match for other types
                whereConditions.push(`${field} = $${paramIndex}`);
                params.push(value);
                paramIndex++;
            }
        }

        // Build the query
        const groupByClause = groupBy.join(', ');
        const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';
        const limitClause = topN ? `LIMIT ${topN}` : '';

        const sql = `
            SELECT 
                ${groupBy.join(', ')},
                SUM(amount) as total_amount,
                COUNT(*) as transaction_count,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount,
                MIN(transaction_date) as earliest_date,
                MAX(transaction_date) as latest_date
            FROM transactions 
            ${whereClause}
            GROUP BY ${groupByClause}
            ORDER BY ${orderBy} ${orderDirection}
            ${limitClause}
        `;

        return await this.db.all(sql, params);
    }

    // Specialized queries for common use cases

    async getCandidateSpending(options = {}) {
        const {
            year,
            office,
            district,
            topN = 10,
            includeCommittees = false
        } = options;

        const groupBy = includeCommittees ? 
            ['candidate_name_normalized', 'committee_name'] : 
            ['candidate_name_normalized'];

        const filters = {};
        if (office) filters.office_sought = office;
        if (district) filters.district = district;

        // Focus on expenditure schedules
        const scheduleTypes = ['ScheduleB', 'ScheduleD'];

        const results = await this.sumBy({
            groupBy,
            year,
            topN,
            filters,
            scheduleTypes
        });

        return results.map(row => ({
            candidate: row.candidate_name_normalized,
            committee: row.committee_name,
            totalSpent: row.total_amount,
            transactionCount: row.transaction_count,
            avgAmount: row.avg_amount,
            dateRange: `${row.earliest_date} to ${row.latest_date}`
        }));
    }

    async getTopContributors(options = {}) {
        const {
            year,
            candidateName,
            topN = 10,
            minAmount = 0
        } = options;

        const filters = {};
        if (candidateName) {
            filters.candidate_name_normalized = this.nameNormalizer.normalizeName(candidateName);
        }
        if (minAmount > 0) {
            filters.amount = `>= ${minAmount}`;
        }

        // Focus on contribution schedules
        const scheduleTypes = ['ScheduleA', 'ScheduleC'];

        const results = await this.sumBy({
            groupBy: ['entity_name_normalized'],
            year,
            topN,
            filters,
            scheduleTypes
        });

        return results.map(row => ({
            contributor: row.entity_name_normalized,
            totalContributed: row.total_amount,
            transactionCount: row.transaction_count,
            avgAmount: row.avg_amount
        }));
    }

    async searchByEntity(entityName, options = {}) {
        const { year, topN = 20 } = options;
        
        const normalizedEntity = this.nameNormalizer.normalizeName(entityName);
        
        const results = await this.sumBy({
            groupBy: ['candidate_name_normalized'],
            year,
            topN,
            filters: {
                entity_name_normalized: normalizedEntity
            }
        });

        return {
            entity: normalizedEntity,
            recipients: results.map(row => ({
                candidate: row.candidate_name_normalized,
                totalReceived: row.total_amount,
                transactionCount: row.transaction_count,
                avgAmount: row.avg_amount
            }))
        };
    }

    async getElectionSpending(options = {}) {
        const {
            year,
            office,
            district,
            locality // for local elections
        } = options;

        const filters = {};
        if (office) filters.office_sought = office;
        if (district) filters.district = district;
        
        // For local elections, we might need to filter by geographic indicators
        if (locality) {
            // This might need adjustment based on how localities are stored
            filters.committee_name = locality;
        }

        const results = await this.sumBy({
            groupBy: ['candidate_name_normalized'],
            year,
            filters,
            scheduleTypes: ['ScheduleB', 'ScheduleD'] // Expenditures
        });

        return results.map(row => ({
            candidate: row.candidate_name_normalized,
            totalSpent: row.total_amount,
            transactionCount: row.transaction_count,
            avgAmount: row.avg_amount
        }));
    }

    async getDetailedTransactions(options = {}) {
        const {
            candidateName,
            entityName,
            year,
            scheduleType,
            minAmount,
            limit = 100
        } = options;

        let whereConditions = [];
        let params = [];
        let paramIndex = 1;

        if (candidateName) {
            whereConditions.push(`UPPER(candidate_name_normalized) LIKE UPPER($${paramIndex})`);
            params.push(`%${this.nameNormalizer.normalizeName(candidateName)}%`);
            paramIndex++;
        }

        if (entityName) {
            whereConditions.push(`UPPER(entity_name_normalized) LIKE UPPER($${paramIndex})`);
            params.push(`%${this.nameNormalizer.normalizeName(entityName)}%`);
            paramIndex++;
        }

        if (year) {
            whereConditions.push(`report_year = $${paramIndex}`);
            params.push(year);
            paramIndex++;
        }

        if (scheduleType) {
            whereConditions.push(`schedule_type = $${paramIndex}`);
            params.push(scheduleType);
            paramIndex++;
        }

        if (minAmount) {
            whereConditions.push(`amount >= $${paramIndex}`);
            params.push(minAmount);
            paramIndex++;
        }

        const whereClause = whereConditions.length > 0 ? `WHERE ${whereConditions.join(' AND ')}` : '';

        const sql = `
            SELECT 
                candidate_name,
                committee_name,
                entity_name,
                transaction_date,
                amount,
                schedule_type,
                purpose,
                entity_city,
                entity_state,
                entity_employer,
                report_year
            FROM transactions 
            ${whereClause}
            ORDER BY amount DESC, transaction_date DESC
            LIMIT ${limit}
        `;

        return await this.db.all(sql, params);
    }

    // Helper method to search candidates
    async searchCandidates(searchTerm, limit = 10) {
        const sql = `
            SELECT DISTINCT 
                candidate_name_normalized,
                candidate_name,
                party,
                office_sought,
                district,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount
            FROM transactions 
            WHERE UPPER(candidate_name_normalized) LIKE UPPER(?)
            GROUP BY candidate_name_normalized
            ORDER BY total_amount DESC
            LIMIT ?
        `;

        return await this.db.all(sql, [`%${this.nameNormalizer.normalizeName(searchTerm)}%`, limit]);
    }

    // Helper method to get database statistics
    async getStats() {
        const totalTransactions = await this.db.get('SELECT COUNT(*) as count FROM transactions');
        const yearRange = await this.db.getYearRange();
        
        const candidateCount = await this.db.get(
            'SELECT COUNT(DISTINCT candidate_name_normalized) as count FROM transactions WHERE candidate_name_normalized IS NOT NULL'
        );
        
        const entityCount = await this.db.get(
            'SELECT COUNT(DISTINCT entity_name_normalized) as count FROM transactions WHERE entity_name_normalized IS NOT NULL'
        );
        
        const totalAmount = await this.db.get('SELECT SUM(amount) as total FROM transactions');

        return {
            totalTransactions: totalTransactions.count,
            yearRange: `${yearRange.min}-${yearRange.max}`,
            uniqueCandidates: candidateCount.count,
            uniqueEntities: entityCount.count,
            totalAmount: totalAmount.total
        };
    }
}

module.exports = QueryEngine;