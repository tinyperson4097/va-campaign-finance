const fs = require('fs').promises;
const path = require('path');
const csv = require('csv-parser');

class DataValidator {
    constructor() {
        this.dataDir = path.join(__dirname, '..', 'data');
        this.expectedOldFiles = [
            'ScheduleA.csv', 'ScheduleB.csv', 'ScheduleC.csv', 'ScheduleD.csv',
            'ScheduleE.csv', 'ScheduleF.csv', 'ScheduleG.csv', 'ScheduleH.csv',
            'ScheduleI.csv', 'ScheduleA_PAC.csv', 'ScheduleB_PAC.csv',
            'ScheduleC_PAC.csv', 'ScheduleD_PAC.csv', 'ScheduleE_PAC.csv',
            'ScheduleF_PAC.csv', 'ScheduleG_PAC.csv', 'ScheduleH_PAC.csv',
            'ScheduleI_PAC.csv'
        ];
        this.expectedNewFiles = [
            'Report.csv', 'ScheduleA.csv', 'ScheduleB.csv', 'ScheduleC.csv',
            'ScheduleD.csv', 'ScheduleE.csv', 'ScheduleF.csv', 'ScheduleG.csv',
            'ScheduleH.csv', 'ScheduleI.csv'
        ];
    }

    async validateDownloads() {
        console.log('🔍 Validating Downloaded Data');
        console.log('============================\n');

        try {
            const folders = await this.getFolders();
            
            if (folders.length === 0) {
                console.log('❌ No data folders found in data/');
                console.log('💡 Run download process first:');
                console.log('   npm run download-recent  # Download recent data');
                console.log('   npm run download-all     # Download all data (slow)');
                return;
            }

            let totalFolders = 0;
            let completeFolders = 0;
            let totalFiles = 0;
            let totalSize = 0;

            for (const folder of folders) {
                const result = await this.validateFolder(folder);
                totalFolders++;
                totalFiles += result.fileCount;
                totalSize += result.totalSize;
                
                if (result.isComplete) {
                    completeFolders++;
                }
            }

            console.log('\n' + '='.repeat(50));
            console.log('📊 VALIDATION SUMMARY');
            console.log('='.repeat(50));
            console.log(`📁 Total folders: ${totalFolders}`);
            console.log(`✅ Complete folders: ${completeFolders}`);
            console.log(`❌ Incomplete folders: ${totalFolders - completeFolders}`);
            console.log(`📄 Total files: ${totalFiles}`);
            console.log(`💾 Total size: ${this.formatSize(totalSize)}`);
            
            const completeness = ((completeFolders / totalFolders) * 100).toFixed(1);
            console.log(`🎯 Completeness: ${completeness}%`);
            
            if (completeness >= 80) {
                console.log('\n✅ Data looks good! Ready to build database.');
                console.log('Run: npm run build --skip-download');
            } else {
                console.log('\n⚠️  Some data is missing. Consider re-downloading.');
                console.log('Run: npm run download-recent');
            }

        } catch (error) {
            console.error('❌ Validation error:', error.message);
        }
    }

    async getFolders() {
        try {
            const items = await fs.readdir(this.dataDir);
            const folders = [];
            
            for (const item of items) {
                const itemPath = path.join(this.dataDir, item);
                const stat = await fs.stat(itemPath);
                
                if (stat.isDirectory() && (
                    /^\d{4}$/.test(item) || // Old format (2008)
                    /^\d{4}_\d{2}$/.test(item) // New format (2024_08)
                )) {
                    folders.push({
                        name: item,
                        path: itemPath,
                        isOld: /^\d{4}$/.test(item),
                        isNew: /^\d{4}_\d{2}$/.test(item)
                    });
                }
            }
            
            return folders.sort((a, b) => a.name.localeCompare(b.name));
        } catch (error) {
            return [];
        }
    }

    async validateFolder(folder) {
        console.log(`📂 ${folder.name} (${folder.isOld ? 'old' : 'new'} format)`);
        
        try {
            const files = await fs.readdir(folder.path);
            const csvFiles = files.filter(f => f.endsWith('.csv'));
            
            const expectedFiles = folder.isOld ? this.expectedOldFiles : this.expectedNewFiles;
            const foundFiles = csvFiles.filter(f => expectedFiles.includes(f));
            const missingFiles = expectedFiles.filter(f => !csvFiles.includes(f));
            
            let totalSize = 0;
            let validFiles = 0;
            
            for (const file of csvFiles) {
                const filePath = path.join(folder.path, file);
                const stat = await fs.stat(filePath);
                totalSize += stat.size;
                
                if (stat.size > 0) {
                    validFiles++;
                }
            }
            
            const isComplete = missingFiles.length === 0;
            const completeness = foundFiles.length / expectedFiles.length;
            
            console.log(`   📄 Files: ${csvFiles.length} (${validFiles} non-empty)`);
            console.log(`   💾 Size: ${this.formatSize(totalSize)}`);
            console.log(`   🎯 Completeness: ${(completeness * 100).toFixed(0)}%`);
            
            if (missingFiles.length > 0 && missingFiles.length <= 3) {
                console.log(`   ⚠️  Missing: ${missingFiles.join(', ')}`);
            } else if (missingFiles.length > 3) {
                console.log(`   ❌ Missing ${missingFiles.length} files`);
            }
            
            if (isComplete) {
                console.log('   ✅ Complete');
            }
            
            console.log('');
            
            return {
                name: folder.name,
                fileCount: csvFiles.length,
                validFileCount: validFiles,
                totalSize,
                isComplete,
                completeness,
                missingFiles
            };
            
        } catch (error) {
            console.log(`   ❌ Error: ${error.message}\n`);
            return {
                name: folder.name,
                fileCount: 0,
                validFileCount: 0,
                totalSize: 0,
                isComplete: false,
                completeness: 0,
                missingFiles: [],
                error: error.message
            };
        }
    }

    formatSize(bytes) {
        if (bytes === 0) return '0 B';
        const k = 1024;
        const sizes = ['B', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
    }

    async quickValidation() {
        const folders = await this.getFolders();
        
        if (folders.length === 0) {
            return { hasData: false, message: 'No data folders found' };
        }
        
        const recentFolders = folders.filter(f => {
            const year = parseInt(f.name.split('_')[0]);
            return year >= 2020;
        }).slice(-5); // Last 5 recent folders
        
        let validCount = 0;
        for (const folder of recentFolders) {
            try {
                const files = await fs.readdir(folder.path);
                const csvFiles = files.filter(f => f.endsWith('.csv'));
                if (csvFiles.length >= 5) { // At least some data
                    validCount++;
                }
            } catch (error) {
                // Folder might be empty or have issues
            }
        }
        
        const completeness = validCount / Math.max(recentFolders.length, 1);
        
        return {
            hasData: folders.length > 0,
            folderCount: folders.length,
            recentFolderCount: recentFolders.length,
            validRecentCount: validCount,
            completeness,
            message: completeness >= 0.5 ? 
                'Data looks reasonable for database build' : 
                'Limited data available, consider downloading more'
        };
    }
}

async function main() {
    const validator = new DataValidator();
    
    const args = process.argv.slice(2);
    
    if (args.includes('--quick')) {
        const result = await validator.quickValidation();
        console.log('Quick validation result:', result);
    } else {
        await validator.validateDownloads();
    }
}

if (require.main === module) {
    main();
}

module.exports = DataValidator;