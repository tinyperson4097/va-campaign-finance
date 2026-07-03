class NameNormalizer {
    constructor() {
        // Common name variations and abbreviations
        this.nameReplacements = {
            // Common suffixes
            ' JR': ' JR',
            ' SR': ' SR',
            ' III': ' III',
            ' II': ' II',
            ' IV': ' IV',
            
            // Committee suffixes
            ' FOR DELEGATE': '',
            ' FOR SENATE': '',
            ' FOR GOVERNOR': '',
            ' FOR ATTORNEY GENERAL': '',
            ' FOR LT GOVERNOR': '',
            ' FOR LIEUTENANT GOVERNOR': '',
            ' FOR HOUSE': '',
            ' FOR CONGRESS': '',
            ' FOR SUPERVISOR': '',
            ' FOR MAYOR': '',
            ' FOR CITY COUNCIL': '',
            ' FOR SCHOOL BOARD': '',
            ' FOR SHERIFF': '',
            ' FOR CLERK': '',
            ' FOR TREASURER': '',
            ' FOR COMMISSIONER': '',
            
            // Common committee words
            ' COMMITTEE': '',
            ' CAMPAIGN': '',
            ' FRIENDS OF': '',
            
            // Punctuation normalization
            ',': '',
            '.': '',
            "'": '',
            '"': ''
        };

        // Common name patterns to standardize
        this.namePatterns = [
            // "Last, First" -> "First Last"
            { pattern: /^([A-Z][A-Z\s]+),\s*([A-Z][A-Z\s]+)$/i, replacement: '$2 $1' },
            
            // "COMMITTEE TO ELECT First Last" -> "First Last"
            { pattern: /^COMMITTEE TO ELECT\s+(.+)$/i, replacement: '$1' },
            
            // "First Last FOR OFFICE" -> "First Last"
            { pattern: /^(.+?)\s+FOR\s+[A-Z\s]+$/i, replacement: '$1' },
            
            // "FRIENDS OF First Last" -> "First Last"
            { pattern: /^FRIENDS OF\s+(.+)$/i, replacement: '$1' }
        ];
    }

    normalizeName(name) {
        if (!name || typeof name !== 'string') {
            return '';
        }

        let normalized = name.toString().trim().toUpperCase();
        
        // Remove extra whitespace
        normalized = normalized.replace(/\s+/g, ' ');
        
        // Apply pattern replacements
        for (const pattern of this.namePatterns) {
            if (pattern.pattern.test(normalized)) {
                normalized = normalized.replace(pattern.pattern, pattern.replacement);
                break;
            }
        }
        
        // Apply word replacements
        for (const [search, replace] of Object.entries(this.nameReplacements)) {
            const regex = new RegExp(search.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi');
            normalized = normalized.replace(regex, replace);
        }
        
        // Clean up extra spaces and trim
        normalized = normalized.replace(/\s+/g, ' ').trim();
        
        return normalized;
    }

    // Calculate similarity between two normalized names
    calculateSimilarity(name1, name2) {
        if (!name1 || !name2) return 0;
        
        const norm1 = this.normalizeName(name1);
        const norm2 = this.normalizeName(name2);
        
        if (norm1 === norm2) return 1;
        
        // Simple Levenshtein distance-based similarity
        return this.levenshteinSimilarity(norm1, norm2);
    }

    levenshteinSimilarity(str1, str2) {
        const matrix = [];
        const len1 = str1.length;
        const len2 = str2.length;

        if (len1 === 0) return len2 === 0 ? 1 : 0;
        if (len2 === 0) return 0;

        for (let i = 0; i <= len1; i++) {
            matrix[i] = [i];
        }

        for (let j = 0; j <= len2; j++) {
            matrix[0][j] = j;
        }

        for (let i = 1; i <= len1; i++) {
            for (let j = 1; j <= len2; j++) {
                if (str1[i - 1] === str2[j - 1]) {
                    matrix[i][j] = matrix[i - 1][j - 1];
                } else {
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j - 1] + 1,
                        matrix[i][j - 1] + 1,
                        matrix[i - 1][j] + 1
                    );
                }
            }
        }

        const maxLen = Math.max(len1, len2);
        return (maxLen - matrix[len1][len2]) / maxLen;
    }

    // Check if two names likely refer to the same person
    areNamesSimilar(name1, name2, threshold = 0.8) {
        return this.calculateSimilarity(name1, name2) >= threshold;
    }

    // Extract candidate name from various formats
    extractCandidateName(committeeName, candidateName) {
        // If we have an explicit candidate name, use it
        if (candidateName && candidateName.trim() && candidateName.toLowerCase() !== 'null') {
            return this.normalizeName(candidateName);
        }
        
        // Otherwise try to extract from committee name
        if (committeeName) {
            return this.normalizeName(committeeName);
        }
        
        return '';
    }

    // Generate search variations for a name
    generateNameVariations(name) {
        const normalized = this.normalizeName(name);
        const variations = new Set([normalized]);
        
        // Add original name
        if (name !== normalized) {
            variations.add(name.toUpperCase());
        }
        
        // Add variations without middle names/initials
        const parts = normalized.split(' ').filter(part => part.length > 1);
        if (parts.length > 2) {
            // First and last name only
            variations.add(`${parts[0]} ${parts[parts.length - 1]}`);
        }
        
        // Add variations with initials
        if (parts.length >= 2) {
            const firstInitial = parts[0][0];
            const lastName = parts[parts.length - 1];
            variations.add(`${firstInitial} ${lastName}`);
        }
        
        return Array.from(variations);
    }
}

module.exports = NameNormalizer;