#!/usr/bin/env node

const VirginiaDataFetcher = require('./data-fetcher');
const DataProcessor = require('./data-processor');

async function main() {
    console.log('Virginia Campaign Finance Database Builder');
    console.log('==========================================\n');

    const args = process.argv.slice(2);
    const skipDownload = args.includes('--skip-download');
    const clearData = args.includes('--clear');

    try {
        if (!skipDownload) {
            console.log('Step 1: Downloading data from Virginia sources...');
            const fetcher = new VirginiaDataFetcher();
            
            const results = await fetcher.downloadAllData();
            
            console.log('\nDownload Results:');
            console.log(`  Old folders (1999-2011): ${results.old.length} files`);
            console.log(`  New folders (2012-2025): ${results.new.length} files`);
            console.log(`  Unknown folders: ${results.unknown.length} files`);
            console.log(`  Errors: ${results.errors.length}`);
            
            if (results.errors.length > 0) {
                console.log('\nErrors encountered:');
                results.errors.forEach(error => {
                    console.log(`  ${error.folder}/${error.file || ''}: ${error.error}`);
                });
            }
        } else {
            console.log('Step 1: Skipping download (using existing data)');
        }

        console.log('\nStep 2: Processing data and building database...');
        const processor = new DataProcessor();
        
        if (clearData) {
            await processor.db.connect();
            await processor.db.clearData();
            await processor.db.close();
            console.log('Existing data cleared.');
        }
        
        await processor.processAllData();
        await processor.close();

        console.log('\n✅ Database build complete!');
        console.log('\nUsage:');
        console.log('  node src/index.js');
        console.log('  or');
        console.log('  npm start');

    } catch (error) {
        console.error('❌ Error during build:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = { main };