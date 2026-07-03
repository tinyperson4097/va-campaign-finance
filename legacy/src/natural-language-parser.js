const NameNormalizer = require('./name-normalizer');

class NaturalLanguageQueryParser {
    constructor() {
        this.nameNormalizer = new NameNormalizer();
        
        // Keywords for different query types
        this.patterns = {
            spending: /spent|spending|expenditure|paid|expense|disburs/i,
            receiving: /receiv|contribut|donat|gave|from/i,
            candidate: /candidate|person|individual/i,
            entity: /company|corporation|business|organization|pac|donor|contributor/i,
            
            // Location patterns
            arlington: /arlington/i,
            virginia: /virginia|va\b/i,
            richmond: /richmond/i,
            norfolk: /norfolk/i,
            
            // Office patterns
            governor: /governor/i,
            senate: /senate|senator/i,
            delegate: /delegate|house/i,
            county_board: /county board|supervisor/i,
            city_council: /city council|council/i,
            mayor: /mayor/i,
            
            // Election types
            primary: /primary/i,
            general: /general/i,
            election: /election/i,
            
            // Time patterns
            year: /(\d{4})/g,
            recent: /recent|latest|current/i,
            
            // Amount patterns
            amount: /\$[\d,]+|\d+\s*dollars?/i,
            top: /top|highest|most|largest|biggest/i,
            
            // Specific entities
            dominion: /dominion\s*(energy)?/i,
            youngkin: /youngkin|glenn/i
        };
        
        // Common query templates
        this.templates = [
            {
                pattern: /how much (?:money )?did (.+?) spend/i,
                type: 'candidate_spending',
                extract: (match, query) => ({
                    candidate: match[1],
                    year: this.extractYear(query)
                })
            },
            {
                pattern: /(.+?) spending (?:in )?(.+?)(?:\s+(\d{4}))?/i,
                type: 'candidate_spending_location',
                extract: (match, query) => ({
                    location: match[2],
                    candidate: match[1] !== 'each candidate' ? match[1] : null,
                    year: match[3] || this.extractYear(query)
                })
            },
            {
                pattern: /(?:how much|what) (?:money )?did (.+?) (?:receive|get) from (.+)/i,
                type: 'specific_contribution',
                extract: (match, query) => ({
                    candidate: match[1],
                    entity: match[2],
                    year: this.extractYear(query)
                })
            },
            {
                pattern: /(?:who received|top recipients) (?:from )?(.+?)(?:\s+in\s+(\d{4}))?/i,
                type: 'entity_recipients',
                extract: (match, query) => ({
                    entity: match[1],
                    year: match[2] || this.extractYear(query)
                })
            },
            {
                pattern: /list (?:how much )?(?:each candidate|candidates?) spent (?:in )?(?:the )?(.+?)(?:\s+(\d{4}))?/i,
                type: 'election_spending',
                extract: (match, query) => ({
                    location: match[1],
                    year: match[2] || this.extractYear(query),
                    election_type: this.extractElectionType(query)
                })
            }
        ];
    }

    parseQuery(naturalQuery) {
        const query = naturalQuery.trim().toLowerCase();
        
        console.log(`\n🔍 Parsing query: "${naturalQuery}"`);
        
        // Try to match against templates
        for (const template of this.templates) {
            const match = query.match(template.pattern);
            if (match) {
                const parsed = template.extract(match, query);
                parsed.type = template.type;
                parsed.originalQuery = naturalQuery;
                
                console.log(`   ✅ Matched template: ${template.type}`);
                console.log(`   📋 Extracted parameters:`, parsed);
                
                return this.enrichParsedQuery(parsed);
            }
        }
        
        // Fallback: analyze keywords
        return this.analyzeKeywords(query, naturalQuery);
    }

    enrichParsedQuery(parsed) {
        // Enhance location parsing
        if (parsed.location) {
            parsed.location = this.parseLocation(parsed.location);
        }
        
        // Enhance candidate name
        if (parsed.candidate) {
            parsed.candidateNormalized = this.nameNormalizer.normalizeName(parsed.candidate);
        }
        
        // Enhance entity name
        if (parsed.entity) {
            parsed.entityNormalized = this.nameNormalizer.normalizeName(parsed.entity);
        }
        
        // Set default year if not specified
        if (!parsed.year) {
            parsed.year = new Date().getFullYear();
        }
        
        return parsed;
    }

    analyzeKeywords(query, originalQuery) {
        const analysis = {
            type: 'keyword_analysis',
            originalQuery,
            isSpending: this.patterns.spending.test(query),
            isReceiving: this.patterns.receiving.test(query),
            year: this.extractYear(query),
            candidates: this.extractCandidates(query),
            entities: this.extractEntities(query),
            locations: this.extractLocations(query),
            offices: this.extractOffices(query),
            electionType: this.extractElectionType(query),
            wantsTop: this.patterns.top.test(query)
        };
        
        console.log(`   🔍 Keyword analysis:`, analysis);
        
        return analysis;
    }

    extractYear(query) {
        const years = query.match(this.patterns.year);
        if (years) {
            return parseInt(years[years.length - 1]); // Return the last year mentioned
        }
        return new Date().getFullYear(); // Default to current year
    }

    extractCandidates(query) {
        const candidates = [];
        
        // Look for specific candidate names
        if (this.patterns.youngkin.test(query)) {
            candidates.push('Glenn Youngkin');
        }
        
        // Could add more specific candidate detection here
        return candidates;
    }

    extractEntities(query) {
        const entities = [];
        
        if (this.patterns.dominion.test(query)) {
            entities.push('Dominion Energy');
        }
        
        return entities;
    }

    extractLocations(query) {
        const locations = [];
        
        if (this.patterns.arlington.test(query)) {
            locations.push('Arlington');
        }
        if (this.patterns.richmond.test(query)) {
            locations.push('Richmond');
        }
        if (this.patterns.norfolk.test(query)) {
            locations.push('Norfolk');
        }
        
        return locations;
    }

    extractOffices(query) {
        const offices = [];
        
        if (this.patterns.governor.test(query)) {
            offices.push('Governor');
        }
        if (this.patterns.senate.test(query)) {
            offices.push('U.S. Senate');
        }
        if (this.patterns.delegate.test(query)) {
            offices.push('House of Delegates');
        }
        if (this.patterns.county_board.test(query)) {
            offices.push('County Board');
        }
        if (this.patterns.city_council.test(query)) {
            offices.push('City Council');
        }
        if (this.patterns.mayor.test(query)) {
            offices.push('Mayor');
        }
        
        return offices;
    }

    extractElectionType(query) {
        if (this.patterns.primary.test(query)) {
            return 'primary';
        }
        if (this.patterns.general.test(query)) {
            return 'general';
        }
        return null;
    }

    parseLocation(locationString) {
        const location = locationString.toLowerCase().trim();
        
        // Map common location variations
        const locationMap = {
            'arlington county board': { location: 'Arlington', office: 'County Board' },
            'arlington county': { location: 'Arlington' },
            'arlington': { location: 'Arlington' },
            'richmond city': { location: 'Richmond' },
            'richmond': { location: 'Richmond' },
            'virginia beach': { location: 'Virginia Beach' },
            'norfolk': { location: 'Norfolk' }
        };
        
        for (const [key, value] of Object.entries(locationMap)) {
            if (location.includes(key)) {
                return value;
            }
        }
        
        return { location: locationString };
    }

    // Convert parsed query to database query parameters
    toQueryParams(parsed) {
        const params = {};
        
        switch (parsed.type) {
            case 'candidate_spending':
                return {
                    method: 'getCandidateSpending',
                    params: {
                        year: parsed.year,
                        candidateName: parsed.candidate,
                        topN: 20
                    }
                };
                
            case 'candidate_spending_location':
                return {
                    method: 'getCandidateSpending',
                    params: {
                        year: parsed.year,
                        office: parsed.location.office,
                        district: parsed.location.location,
                        topN: 20
                    }
                };
                
            case 'specific_contribution':
                return {
                    method: 'getDetailedTransactions',
                    params: {
                        candidateName: parsed.candidate,
                        entityName: parsed.entity,
                        year: parsed.year,
                        limit: 50
                    }
                };
                
            case 'entity_recipients':
                return {
                    method: 'searchByEntity',
                    params: {
                        entityName: parsed.entity,
                        year: parsed.year,
                        topN: 20
                    }
                };
                
            case 'election_spending':
                const locationInfo = parsed.location;
                return {
                    method: 'getCandidateSpending',
                    params: {
                        year: parsed.year,
                        office: locationInfo.office || 'County Board',
                        district: locationInfo.location,
                        topN: 20
                    }
                };
                
            default:
                // Fallback for keyword analysis
                if (parsed.isSpending && parsed.locations.length > 0) {
                    return {
                        method: 'getCandidateSpending',
                        params: {
                            year: parsed.year,
                            district: parsed.locations[0],
                            topN: parsed.wantsTop ? 10 : 20
                        }
                    };
                }
                
                if (parsed.entities.length > 0) {
                    return {
                        method: 'searchByEntity',
                        params: {
                            entityName: parsed.entities[0],
                            year: parsed.year,
                            topN: parsed.wantsTop ? 10 : 20
                        }
                    };
                }
                
                return {
                    method: 'getCandidateSpending',
                    params: {
                        year: parsed.year,
                        topN: 20
                    }
                };
        }
    }

    // Generate helpful suggestions based on failed parse
    generateSuggestions(query) {
        const suggestions = [
            "Try these example queries:",
            "",
            "💡 Candidate Spending:",
            "• 'How much did Glenn Youngkin spend in 2024?'",
            "• 'List how much each candidate spent in the Arlington County Board primary election in 2024'",
            "• 'Top spending candidates in 2023'",
            "",
            "💰 Contributions:",
            "• 'How much money did Glenn Youngkin receive from Dominion Energy in 2024?'",
            "• 'Who received money from Dominion Energy in 2023?'",
            "• 'Top contributors to Tim Kaine'",
            "",
            "🏛️ Elections:",
            "• 'Arlington County Board 2024 election spending'",
            "• 'Richmond mayor race 2023'",
            "• 'Virginia governor candidates 2021'",
            "",
            "🔍 Tips:",
            "• Include the year (e.g., 2024, 2023)",
            "• Mention specific offices (County Board, Governor, Senate)",
            "• Use candidate names or entity names (Dominion Energy, etc.)",
            "• Try 'top' or 'highest' for ranked results"
        ];
        
        return suggestions;
    }
}

module.exports = NaturalLanguageQueryParser;