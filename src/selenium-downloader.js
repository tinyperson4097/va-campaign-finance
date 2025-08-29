const { Builder, By, until, Key } = require('selenium-webdriver');
const chrome = require('selenium-webdriver/chrome');
const { ServiceBuilder } = require('selenium-webdriver/chrome');
const fs = require('fs').promises;
const path = require('path');

class VirginiaSeleniumDownloader {
    constructor() {
        this.baseUrl = 'https://apps.elections.virginia.gov/SBE_CSV/CF/';
        this.dataDir = path.join(__dirname, '..', 'data');
        this.driver = null;
        this.downloadDir = null;
        this.stats = {
            foldersProcessed: 0,
            filesDownloaded: 0,
            errors: [],
            startTime: Date.now()
        };
    }

    async setupDriver() {
        // Create downloads directory
        this.downloadDir = path.join(this.dataDir, 'downloads_temp');
        await fs.mkdir(this.downloadDir, { recursive: true });
        await fs.mkdir(this.dataDir, { recursive: true });

        // Chrome options for downloads
        const options = new chrome.Options();
        options.addArguments('--headless'); // Run in background
        options.addArguments('--no-sandbox');
        options.addArguments('--disable-dev-shm-usage');
        options.addArguments('--disable-gpu');
        options.addArguments('--window-size=1920,1080');
        
        // Set download preferences
        options.setUserPreferences({
            'download.default_directory': this.downloadDir,
            'download.prompt_for_download': false,
            'download.directory_upgrade': true,
            'safebrowsing.enabled': true
        });

        // Let selenium-webdriver automatically find and use the correct ChromeDriver
        this.driver = await new Builder()
            .forBrowser('chrome')
            .setChromeOptions(options)
            .build();

        console.log('✅ Chrome driver initialized');
    }

    async discoverFolders() {
        console.log('🔍 Discovering available folders...');
        
        await this.driver.get(this.baseUrl);
        await this.driver.sleep(2000); // Wait for page load
        
        // Find all folder links
        const links = await this.driver.findElements(By.css('a[href*="/"]'));
        const folders = [];
        
        for (let link of links) {
            try {
                const href = await link.getAttribute('href');
                const text = await link.getText();
                
                // Match year folders (2008) or year_month folders (2024_08)
                if (/^\d{4}$/.test(text) || /^\d{4}_\d{2}$/.test(text)) {
                    folders.push({
                        name: text,
                        url: href,
                        isOld: /^\d{4}$/.test(text),
                        isNew: /^\d{4}_\d{2}$/.test(text)
                    });
                }
            } catch (err) {
                // Skip problematic links
                continue;
            }
        }
        
        // Sort folders chronologically
        folders.sort((a, b) => {
            const aYear = parseInt(a.name.split('_')[0]);
            const bYear = parseInt(b.name.split('_')[0]);
            if (aYear !== bYear) return aYear - bYear;
            
            const aMonth = a.name.includes('_') ? parseInt(a.name.split('_')[1]) : 0;
            const bMonth = b.name.includes('_') ? parseInt(b.name.split('_')[1]) : 0;
            return aMonth - bMonth;
        });
        
        console.log(`📁 Found ${folders.length} folders to process`);
        console.log(`   Old format (1999-2011): ${folders.filter(f => f.isOld).length}`);
        console.log(`   New format (2012-2025): ${folders.filter(f => f.isNew).length}`);
        
        return folders;
    }

    async downloadFolder(folder) {
        console.log(`\n📂 Processing folder: ${folder.name}`);
        
        try {
            // Navigate to folder
            await this.driver.get(folder.url);
            await this.driver.sleep(3000); // Wait for page load
            
            // Find CSV file links
            const csvLinks = await this.driver.findElements(By.css('a[href$=".csv"]'));
            
            if (csvLinks.length === 0) {
                console.log(`   ⚠️  No CSV files found in ${folder.name}`);
                return { folder: folder.name, files: [], errors: [] };
            }
            
            console.log(`   Found ${csvLinks.length} CSV files`);
            
            // Create destination folder
            const folderPath = path.join(this.dataDir, folder.name);
            await fs.mkdir(folderPath, { recursive: true });
            
            const downloadedFiles = [];
            const errors = [];
            
            // Download each CSV file
            for (let i = 0; i < csvLinks.length; i++) {
                try {
                    const link = csvLinks[i];
                    const fileName = await link.getText();
                    const fileUrl = await link.getAttribute('href');
                    
                    console.log(`   📄 Downloading: ${fileName} (${i + 1}/${csvLinks.length})`);
                    
                    // Click to download
                    await this.driver.executeScript('arguments[0].click();', link);
                    await this.driver.sleep(2000); // Wait for download to start
                    
                    // Wait for download to complete and move file
                    await this.waitForDownloadAndMove(fileName, folderPath);
                    
                    downloadedFiles.push(fileName);
                    this.stats.filesDownloaded++;
                    
                    // Small delay between downloads
                    await this.driver.sleep(1000);
                    
                } catch (err) {
                    console.log(`   ❌ Error downloading file: ${err.message}`);
                    errors.push({ file: fileName, error: err.message });
                }
            }
            
            this.stats.foldersProcessed++;
            console.log(`   ✅ Downloaded ${downloadedFiles.length}/${csvLinks.length} files`);
            
            return { folder: folder.name, files: downloadedFiles, errors };
            
        } catch (err) {
            console.log(`   ❌ Error processing folder ${folder.name}: ${err.message}`);
            this.stats.errors.push({ folder: folder.name, error: err.message });
            return { folder: folder.name, files: [], errors: [{ error: err.message }] };
        }
    }

    async waitForDownloadAndMove(fileName, destinationFolder, maxWaitMs = 30000) {
        const startTime = Date.now();
        
        while (Date.now() - startTime < maxWaitMs) {
            try {
                const tempFiles = await fs.readdir(this.downloadDir);
                
                // Look for the downloaded file (might have different name due to browser handling)
                for (const tempFile of tempFiles) {
                    if (tempFile.includes(fileName.replace('.csv', '')) && tempFile.endsWith('.csv')) {
                        // Move file to proper location
                        const sourcePath = path.join(this.downloadDir, tempFile);
                        const destPath = path.join(destinationFolder, fileName);
                        
                        await fs.rename(sourcePath, destPath);
                        return true;
                    }
                }
                
                await new Promise(resolve => setTimeout(resolve, 1000));
                
            } catch (err) {
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }
        
        throw new Error(`Download timeout for ${fileName}`);
    }

    async downloadAll(options = {}) {
        const { 
            startFrom = null,
            endAt = null,
            recentOnly = false,
            maxFolders = null 
        } = options;
        
        try {
            await this.setupDriver();
            const folders = await this.discoverFolders();
            
            let foldersToProcess = folders;
            
            // Apply filters
            if (recentOnly) {
                // Only process folders from 2020 onwards
                foldersToProcess = folders.filter(f => {
                    const year = parseInt(f.name.split('_')[0]);
                    return year >= 2020;
                });
            }
            
            if (startFrom) {
                const startIndex = folders.findIndex(f => f.name === startFrom);
                if (startIndex >= 0) {
                    foldersToProcess = foldersToProcess.slice(startIndex);
                }
            }
            
            if (endAt) {
                const endIndex = foldersToProcess.findIndex(f => f.name === endAt);
                if (endIndex >= 0) {
                    foldersToProcess = foldersToProcess.slice(0, endIndex + 1);
                }
            }
            
            if (maxFolders) {
                foldersToProcess = foldersToProcess.slice(0, maxFolders);
            }
            
            console.log(`\n🚀 Starting download of ${foldersToProcess.length} folders...`);
            
            const results = [];
            
            for (const folder of foldersToProcess) {
                const result = await this.downloadFolder(folder);
                results.push(result);
                
                // Progress update
                const elapsed = ((Date.now() - this.stats.startTime) / 1000 / 60).toFixed(1);
                console.log(`\n📊 Progress: ${this.stats.foldersProcessed}/${foldersToProcess.length} folders, ${this.stats.filesDownloaded} files, ${elapsed}min elapsed`);
            }
            
            return results;
            
        } finally {
            if (this.driver) {
                await this.driver.quit();
            }
            
            // Clean up temp directory
            try {
                await fs.rmdir(this.downloadDir, { recursive: true });
            } catch (err) {
                console.log('Could not clean up temp directory:', err.message);
            }
        }
    }

    async downloadRecent(years = 2) {
        const currentYear = new Date().getFullYear();
        const startYear = currentYear - years;
        
        return await this.downloadAll({
            recentOnly: true,
            startFrom: `${startYear}_01`
        });
    }

    printStats() {
        const elapsed = ((Date.now() - this.stats.startTime) / 1000 / 60).toFixed(1);
        
        console.log('\n' + '='.repeat(50));
        console.log('📈 DOWNLOAD STATISTICS');
        console.log('='.repeat(50));
        console.log(`⏱️  Total time: ${elapsed} minutes`);
        console.log(`📁 Folders processed: ${this.stats.foldersProcessed}`);
        console.log(`📄 Files downloaded: ${this.stats.filesDownloaded}`);
        console.log(`❌ Errors: ${this.stats.errors.length}`);
        
        if (this.stats.errors.length > 0) {
            console.log('\nErrors encountered:');
            this.stats.errors.forEach(error => {
                console.log(`   ${error.folder}: ${error.error}`);
            });
        }
        
        console.log('\n✅ Download complete! Run "npm run build --skip-download" to process the data.');
    }
}

// Command line interface
async function main() {
    const args = process.argv.slice(2);
    const downloader = new VirginiaSeleniumDownloader();
    
    try {
        console.log('🤖 Virginia Campaign Finance Selenium Downloader');
        console.log('='.repeat(50));
        
        if (args.includes('--recent')) {
            console.log('📅 Downloading recent data (last 2 years)...');
            await downloader.downloadRecent(2);
        } else if (args.includes('--test')) {
            console.log('🧪 Test mode - downloading 2 recent folders...');
            await downloader.downloadAll({ maxFolders: 2, recentOnly: true });
        } else if (args.includes('--all')) {
            console.log('📥 Downloading ALL data (this will take a long time)...');
            await downloader.downloadAll();
        } else {
            console.log('📅 Default: Downloading recent data (last 2 years)...');
            await downloader.downloadRecent(2);
        }
        
        downloader.printStats();
        
    } catch (error) {
        console.error('❌ Download failed:', error.message);
        console.error(error.stack);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = VirginiaSeleniumDownloader;