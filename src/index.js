#!/usr/bin/env node

const QueryEngine = require('./query-engine');
const NameNormalizer = require('./name-normalizer');
const NaturalLanguageParser = require('./natural-language-parser');

class CampaignFinanceQuery {
    constructor() {
        this.queryEngine = new QueryEngine();
        this.nameNormalizer = new NameNormalizer();
        this.nlParser = new NaturalLanguageParser();
    }

    async connect() {
        await this.queryEngine.connect();
    }

    async close() {
        await this.queryEngine.close();
    }

    // Main query methods

    async arlingtonCountyBoard2024() {
        console.log('Arlington County Board 2024 Election Spending\n');
        
        const results = await this.queryEngine.getCandidateSpending({
            year: 2024,
            // Arlington County Board elections might be coded differently
            // We'll search for Arlington-related committees
            topN: 20
        });

        // Filter for Arlington-related candidates
        const arlingtonResults = results.filter(result => 
            result.candidate.includes('ARLINGTON') || 
            result.committee?.includes('ARLINGTON')
        );

        if (arlingtonResults.length === 0) {
            console.log('No Arlington County Board candidates found in 2024 data.');
            console.log('This might indicate:');
            console.log('1. Data for 2024 is not yet available');
            console.log('2. Arlington elections are coded differently');
            console.log('3. The election was held in a different year');
            
            // Let's search more broadly
            console.log('\nSearching for any Arlington-related activity in 2024:');
            const broadSearch = await this.queryEngine.sumBy({
                year: 2024,
                filters: { committee_name: 'Arlington' },
                topN: 10
            });
            
            if (broadSearch.length > 0) {
                broadSearch.forEach((result, index) => {
                    console.log(`${index + 1}. ${result.candidate_name_normalized}: $${result.total_amount?.toLocaleString() || 0}`);
                });
            }
            return;
        }

        arlingtonResults.forEach((result, index) => {
            console.log(`${index + 1}. ${result.candidate}: $${result.totalSpent?.toLocaleString() || 0}`);
            if (result.committee) {
                console.log(`   Committee: ${result.committee}`);
            }
            console.log(`   Transactions: ${result.transactionCount}`);
            console.log(`   Average: $${result.avgAmount?.toLocaleString() || 0}\n`);
        });
    }

    async dominionEnergyContributions(year = 2024) {
        console.log(`Dominion Energy Contributions in ${year}\n`);
        
        const results = await this.queryEngine.searchByEntity('Dominion Energy', { year, topN: 15 });
        
        if (results.recipients.length === 0) {
            console.log(`No Dominion Energy contributions found in ${year}.`);
            
            // Try searching with variations
            const variations = ['Dominion', 'Virginia Electric', 'VEPCO'];
            for (const variant of variations) {
                console.log(`\nSearching for "${variant}":`);
                const variantResults = await this.queryEngine.searchByEntity(variant, { year, topN: 5 });
                if (variantResults.recipients.length > 0) {
                    variantResults.recipients.forEach((result, index) => {
                        console.log(`${index + 1}. ${result.candidate}: $${result.totalReceived?.toLocaleString() || 0}`);
                    });
                }
            }
            return;
        }

        console.log(`Entity: ${results.entity}`);
        console.log(`Recipients:\n`);
        
        results.recipients.forEach((result, index) => {
            console.log(`${index + 1}. ${result.candidate}: $${result.totalReceived?.toLocaleString() || 0}`);
            console.log(`   Transactions: ${result.transactionCount}`);
            console.log(`   Average: $${result.avgAmount?.toLocaleString() || 0}\n`);
        });
    }

    async glenYoungkinDominionMoney(year = 2024) {
        console.log(`Glen Youngkin - Dominion Energy contributions in ${year}\n`);
        
        const transactions = await this.queryEngine.getDetailedTransactions({
            candidateName: 'Glen Youngkin',
            entityName: 'Dominion',
            year,
            limit: 20
        });

        if (transactions.length === 0) {
            console.log('No transactions found between Glen Youngkin and Dominion Energy in 2024.');
            
            // Try broader search
            console.log('\nSearching for Glen Youngkin transactions in 2024:');
            const youngkinTransactions = await this.queryEngine.getDetailedTransactions({
                candidateName: 'Youngkin',
                year,
                limit: 10
            });
            
            if (youngkinTransactions.length > 0) {
                youngkinTransactions.forEach((transaction, index) => {
                    console.log(`${index + 1}. ${transaction.entity_name}: $${transaction.amount?.toLocaleString() || 0}`);
                    console.log(`   Date: ${transaction.transaction_date}, Committee: ${transaction.committee_name}\n`);
                });
            }
            return;
        }

        let total = 0;
        transactions.forEach((transaction, index) => {
            console.log(`${index + 1}. $${transaction.amount?.toLocaleString() || 0} on ${transaction.transaction_date}`);
            console.log(`   From: ${transaction.entity_name}`);
            console.log(`   To: ${transaction.committee_name}`);
            console.log(`   Purpose: ${transaction.purpose || 'Not specified'}\n`);
            total += transaction.amount || 0;
        });
        
        console.log(`Total from Dominion-related entities: $${total.toLocaleString()}`);
    }

    async highestSpending2024() {
        console.log('Highest Spending Candidates in 2024\n');
        
        const results = await this.queryEngine.getCandidateSpending({
            year: 2024,
            topN: 20
        });

        if (results.length === 0) {
            console.log('No spending data found for 2024.');
            console.log('This might indicate that 2024 data is not yet available.');
            
            // Try 2023
            console.log('\nTrying 2023 data instead:');
            const results2023 = await this.queryEngine.getCandidateSpending({
                year: 2023,
                topN: 10
            });
            
            results2023.forEach((result, index) => {
                console.log(`${index + 1}. ${result.candidate}: $${result.totalSpent?.toLocaleString() || 0}`);
            });
            return;
        }

        results.forEach((result, index) => {
            console.log(`${index + 1}. ${result.candidate}: $${result.totalSpent?.toLocaleString() || 0}`);
            console.log(`   Transactions: ${result.transactionCount}`);
            console.log(`   Average: $${result.avgAmount?.toLocaleString() || 0}`);
            console.log(`   Period: ${result.dateRange}\n`);
        });
    }

    async interactiveMode() {
        const readline = require('readline');
        const rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });

        console.log('Virginia Campaign Finance Query Tool - Interactive Mode');
        console.log('=====================================================\n');
        
        const stats = await this.queryEngine.getStats();
        console.log('Database Statistics:');
        console.log(`  Total Transactions: ${stats.totalTransactions?.toLocaleString() || 0}`);
        console.log(`  Year Range: ${stats.yearRange}`);
        console.log(`  Unique Candidates: ${stats.uniqueCandidates?.toLocaleString() || 0}`);
        console.log(`  Unique Contributors/Payees: ${stats.uniqueEntities?.toLocaleString() || 0}`);
        console.log(`  Total Amount: $${stats.totalAmount?.toLocaleString() || 0}\n`);

        const commands = [
            '1. Arlington County Board 2024 spending',
            '2. Dominion Energy contributions 2024',
            '3. Glen Youngkin - Dominion money 2024',
            '4. Highest spending candidates 2024',
            '5. Search candidates',
            '6. Search contributors',
            '7. Natural language query (Ask in plain English!)',
            '8. Query examples and help',
            '9. Exit'
        ];

        const showMenu = () => {
            console.log('\nAvailable Queries:');
            commands.forEach(cmd => console.log(`  ${cmd}`));
            console.log('');
        };

        const processChoice = async (choice) => {
            switch(choice) {
                case '1':
                    await this.arlingtonCountyBoard2024();
                    break;
                case '2':
                    await this.dominionEnergyContributions(2024);
                    break;
                case '3':
                    await this.glenYoungkinDominionMoney(2024);
                    break;
                case '4':
                    await this.highestSpending2024();
                    break;
                case '5':
                    rl.question('Enter candidate name to search: ', async (name) => {
                        const results = await this.queryEngine.searchCandidates(name);
                        results.forEach((result, index) => {
                            console.log(`${index + 1}. ${result.candidate_name} (${result.party || 'Unknown party'})`);
                            console.log(`   Total: $${result.total_amount?.toLocaleString() || 0}, Transactions: ${result.transaction_count}`);
                        });
                        showMenu();
                    });
                    return;
                case '6':
                    rl.question('Enter contributor/entity name to search: ', async (entity) => {
                        rl.question('Enter year (or press Enter for all years): ', async (year) => {
                            const searchYear = year ? parseInt(year) : null;
                            const results = await this.queryEngine.searchByEntity(entity, { year: searchYear });
                            console.log(`\nContributions from ${results.entity}:`);
                            results.recipients.forEach((result, index) => {
                                console.log(`${index + 1}. ${result.candidate}: $${result.totalReceived?.toLocaleString() || 0}`);
                            });
                            showMenu();
                        });
                    });
                    return;
                case '7':
                    rl.question('💬 Ask me anything in plain English (e.g., "How much did each candidate spend in the Arlington County Board primary election in 2024?"): ', async (query) => {
                        if (query.trim()) {
                            await this.queryNaturalLanguage(query);
                        } else {
                            console.log('Please enter a query.');
                        }
                        showMenu();
                    });
                    return;
                case '8':
                    this.showQueryHelp();
                    break;
                case '9':
                    console.log('Goodbye!');
                    rl.close();
                    return;
                default:
                    console.log('Invalid choice. Please try again.');
            }
            showMenu();
        };

        showMenu();

        rl.on('line', async (input) => {
            await processChoice(input.trim());
        });

        rl.on('close', () => {
            this.close();
            process.exit(0);
        });
    }

    async queryNaturalLanguage(naturalQuery) {
        console.log(`\n🤖 Processing: "${naturalQuery}"`);
        console.log('='.repeat(60));
        
        try {
            const parsed = this.nlParser.parseQuery(naturalQuery);
            const queryParams = this.nlParser.toQueryParams(parsed);
            
            console.log(`🎯 Query type: ${queryParams.method}`);
            
            let results;
            switch (queryParams.method) {
                case 'getCandidateSpending':
                    results = await this.queryEngine.getCandidateSpending(queryParams.params);
                    this.displayCandidateSpending(results, parsed);
                    break;
                    
                case 'getDetailedTransactions':
                    results = await this.queryEngine.getDetailedTransactions(queryParams.params);
                    this.displayDetailedTransactions(results, parsed);
                    break;
                    
                case 'searchByEntity':
                    results = await this.queryEngine.searchByEntity(queryParams.params.entityName, {
                        year: queryParams.params.year,
                        topN: queryParams.params.topN
                    });
                    this.displayEntitySearch(results, parsed);
                    break;
                    
                default:
                    console.log('❌ Could not determine query type');
                    this.showQueryHelp();
            }
            
        } catch (error) {
            console.error('❌ Query error:', error.message);
            console.log('\n💡 Try rephrasing your query or use one of these examples:');
            const suggestions = this.nlParser.generateSuggestions(naturalQuery);
            suggestions.forEach(suggestion => console.log(suggestion));
        }
    }

    displayCandidateSpending(results, parsed) {
        if (results.length === 0) {
            console.log('❌ No spending data found for your query');
            return;
        }

        console.log(`\n💰 Campaign Spending Results (${results.length} found):`);
        console.log('-'.repeat(60));
        
        results.forEach((result, index) => {
            console.log(`${index + 1}. ${result.candidate}`);
            console.log(`   💸 Total Spent: $${result.totalSpent?.toLocaleString() || 0}`);
            if (result.committee && result.committee !== result.candidate) {
                console.log(`   🏛️  Committee: ${result.committee}`);
            }
            console.log(`   📊 Transactions: ${result.transactionCount}`);
            console.log(`   📈 Average: $${result.avgAmount?.toLocaleString() || 0}`);
            console.log('');
        });
        
        const total = results.reduce((sum, r) => sum + (r.totalSpent || 0), 0);
        console.log(`🎯 Total spending across all candidates: $${total.toLocaleString()}`);
    }

    displayDetailedTransactions(results, parsed) {
        if (results.length === 0) {
            console.log('❌ No transactions found for your query');
            return;
        }

        console.log(`\n🔍 Transaction Details (${results.length} found):`);
        console.log('-'.repeat(60));
        
        let total = 0;
        results.forEach((transaction, index) => {
            console.log(`${index + 1}. $${transaction.amount?.toLocaleString() || 0} on ${transaction.transaction_date}`);
            console.log(`   👤 From: ${transaction.entity_name}`);
            console.log(`   🏛️  To: ${transaction.committee_name}`);
            console.log(`   📋 Purpose: ${transaction.purpose || 'Not specified'}`);
            if (transaction.entity_city && transaction.entity_state) {
                console.log(`   📍 Location: ${transaction.entity_city}, ${transaction.entity_state}`);
            }
            console.log('');
            total += transaction.amount || 0;
        });
        
        console.log(`🎯 Total transaction amount: $${total.toLocaleString()}`);
    }

    displayEntitySearch(results, parsed) {
        if (results.recipients.length === 0) {
            console.log('❌ No contributions found for your query');
            return;
        }

        console.log(`\n🏢 Contributions from: ${results.entity}`);
        console.log('-'.repeat(60));
        console.log(`Recipients (${results.recipients.length} found):`);
        
        results.recipients.forEach((result, index) => {
            console.log(`${index + 1}. ${result.candidate}`);
            console.log(`   💰 Total Received: $${result.totalReceived?.toLocaleString() || 0}`);
            console.log(`   📊 Transactions: ${result.transactionCount}`);
            console.log(`   📈 Average: $${result.avgAmount?.toLocaleString() || 0}`);
            console.log('');
        });
        
        const total = results.recipients.reduce((sum, r) => sum + (r.totalReceived || 0), 0);
        console.log(`🎯 Total contributions: $${total.toLocaleString()}`);
    }

    showQueryHelp() {
        const examples = [
            "🗣️  Natural Language Query Examples:",
            "",
            "💡 Try asking questions like:",
            "• 'How much did Glenn Youngkin spend in 2024?'",
            "• 'List how much each candidate spent in the Arlington County Board primary election in 2024'",
            "• 'How much money did Glenn Youngkin receive from Dominion Energy in 2024?'",
            "• 'Who are the top recipients from Dominion Energy in 2023?'",
            "• 'What was the highest spending candidate in Virginia in 2024?'",
            "• 'Show me Richmond mayor race spending in 2023'",
            "",
            "🎯 Query Tips:",
            "• Always include a year (2024, 2023, etc.)",
            "• Be specific about locations (Arlington, Richmond, Virginia Beach)",
            "• Mention election types (primary, general election)",
            "• Use candidate names or company names you're interested in",
            "• Try words like 'top', 'highest', 'most' for rankings"
        ];
        
        examples.forEach(line => console.log(line));
    }

    async runExample(exampleName) {
        switch(exampleName) {
            case 'arlington':
                await this.arlingtonCountyBoard2024();
                break;
            case 'dominion':
                await this.dominionEnergyContributions(2024);
                break;
            case 'youngkin':
                await this.glenYoungkinDominionMoney(2024);
                break;
            case 'highest':
                await this.highestSpending2024();
                break;
            default:
                console.log('Available examples: arlington, dominion, youngkin, highest');
        }
    }
}

async function main() {
    const args = process.argv.slice(2);
    const query = new CampaignFinanceQuery();
    
    try {
        await query.connect();
        
        if (args.length === 0) {
            await query.interactiveMode();
        } else if (args[0] === 'example' && args[1]) {
            await query.runExample(args[1]);
            await query.close();
        } else if (args[0] === 'query') {
            // Natural language query from command line
            const naturalQuery = args.slice(1).join(' ');
            if (naturalQuery) {
                await query.queryNaturalLanguage(naturalQuery);
            } else {
                console.log('Please provide a query after "query"');
                console.log('Example: node src/index.js query "How much did Glenn Youngkin spend in 2024?"');
            }
            await query.close();
        } else {
            // Try to interpret the entire command line as a natural language query
            const naturalQuery = args.join(' ');
            if (naturalQuery.length > 10) { // Reasonable query length
                console.log('🤖 Interpreting as natural language query...');
                await query.queryNaturalLanguage(naturalQuery);
                await query.close();
            } else {
                console.log('Virginia Campaign Finance Database');
                console.log('=================================\n');
                console.log('Usage:');
                console.log('  npm start                                    # Interactive mode');
                console.log('  npm start example arlington                  # Pre-built examples');
                console.log('  npm start query "your question here"        # Natural language query');
                console.log('');
                console.log('Natural Language Examples:');
                console.log('  npm start "How much did Glenn Youngkin spend in 2024?"');
                console.log('  npm start "List candidates in Arlington County Board 2024 election"');
                console.log('  npm start "Top Dominion Energy recipients in 2023"');
                console.log('');
                console.log('Pre-built Examples:');
                console.log('  npm start example arlington   # Arlington County Board 2024');
                console.log('  npm start example dominion    # Dominion Energy contributions');
                console.log('  npm start example youngkin    # Glenn Youngkin - Dominion money');
                console.log('  npm start example highest     # Highest spending candidates');
                await query.close();
            }
        }
    } catch (error) {
        console.error('Error:', error.message);
        await query.close();
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = CampaignFinanceQuery;