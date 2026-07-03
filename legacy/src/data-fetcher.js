const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs').promises;
const path = require('path');

class VirginiaDataFetcher {
    constructor() {
        this.baseUrl = 'https://apps.elections.virginia.gov/SBE_CSV/CF/';
        this.secondaryUrl = 'https://cfreports.elections.virginia.gov/';
        this.dataDir = path.join(__dirname, '..', 'data');
    }

    async ensureDataDir() {
        try {
            await fs.mkdir(this.dataDir, { recursive: true });
        } catch (err) {
            console.log('Data directory already exists or created');
        }
    }

    async fetchDirectoryListing() {
        try {
            // Add user agent to appear more like a regular browser
            const response = await axios.get(this.baseUrl, {
                headers: {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                },
                timeout: 30000
            });
            const $ = cheerio.load(response.data);
            
            const folders = [];
            $('a').each((i, element) => {
                const href = $(element).attr('href');
                if (href && href.includes('/') && !href.includes('..')) {
                    const folderName = href.replace('/', '');
                    if (folderName) {
                        folders.push(folderName);
                    }
                }
            });
            
            return folders.filter(folder => {
                return /^\d{4}$/.test(folder) || /^\d{4}_\d{2}$/.test(folder);
            }).sort();
        } catch (error) {
            if (error.response?.status === 403) {
                console.error('Access denied to Virginia data source. This could be due to:');
                console.error('1. Server blocking automated requests');
                console.error('2. Geographic restrictions');
                console.error('3. Authentication requirements');
                console.error('4. Rate limiting');
                console.error('\nTry manually downloading files or using a VPN if outside Virginia.');
            } else {
                console.error('Error fetching directory listing:', error.message);
            }
            return [];
        }
    }

    async fetchFolderContents(folder) {
        try {
            const url = `${this.baseUrl}${folder}/`;
            const response = await axios.get(url);
            const $ = cheerio.load(response.data);
            
            const files = [];
            $('a').each((i, element) => {
                const href = $(element).attr('href');
                if (href && href.endsWith('.csv')) {
                    files.push(href);
                }
            });
            
            return files;
        } catch (error) {
            console.error(`Error fetching folder contents for ${folder}:`, error);
            return [];
        }
    }

    async downloadFile(folder, fileName) {
        try {
            const url = `${this.baseUrl}${folder}/${fileName}`;
            const folderPath = path.join(this.dataDir, folder);
            await fs.mkdir(folderPath, { recursive: true });
            
            const filePath = path.join(folderPath, fileName);
            
            // Check if file already exists
            try {
                await fs.access(filePath);
                console.log(`File already exists: ${folder}/${fileName}`);
                return filePath;
            } catch {
                // File doesn't exist, download it
            }
            
            console.log(`Downloading: ${folder}/${fileName}`);
            const response = await axios.get(url, { responseType: 'stream' });
            
            const writer = require('fs').createWriteStream(filePath);
            response.data.pipe(writer);
            
            return new Promise((resolve, reject) => {
                writer.on('finish', () => resolve(filePath));
                writer.on('error', reject);
            });
        } catch (error) {
            console.error(`Error downloading ${folder}/${fileName}:`, error);
            throw error;
        }
    }

    categorizeFolder(folder) {
        // Old folders: 1999-2011 (just year)
        if (/^\d{4}$/.test(folder)) {
            const year = parseInt(folder);
            return year >= 1999 && year <= 2011 ? 'old' : 'unknown';
        }
        
        // New folders: 2012_03 to 2025_08 (year_month)
        if (/^\d{4}_\d{2}$/.test(folder)) {
            const [year, month] = folder.split('_').map(Number);
            if (year >= 2012 && year <= 2025 && month >= 1 && month <= 12) {
                return 'new';
            }
        }
        
        return 'unknown';
    }

    async downloadAllData() {
        await this.ensureDataDir();
        
        const folders = await this.fetchDirectoryListing();
        console.log(`Found ${folders.length} folders to process`);
        
        const results = {
            old: [],
            new: [],
            unknown: [],
            errors: []
        };
        
        for (const folder of folders) {
            try {
                const category = this.categorizeFolder(folder);
                const files = await this.fetchFolderContents(folder);
                
                console.log(`\nProcessing ${category} folder: ${folder} (${files.length} files)`);
                
                for (const file of files) {
                    try {
                        const filePath = await this.downloadFile(folder, file);
                        results[category].push({ folder, file, path: filePath });
                    } catch (error) {
                        results.errors.push({ folder, file, error: error.message });
                    }
                }
            } catch (error) {
                results.errors.push({ folder, error: error.message });
            }
        }
        
        return results;
    }

    async getAvailableFolders() {
        const folders = await this.fetchDirectoryListing();
        return folders.map(folder => ({
            name: folder,
            category: this.categorizeFolder(folder),
            isOld: this.categorizeFolder(folder) === 'old',
            isNew: this.categorizeFolder(folder) === 'new'
        }));
    }
}

module.exports = VirginiaDataFetcher;