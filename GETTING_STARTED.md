# Getting Started with Virginia Campaign Finance Database

This guide will get you up and running with the Virginia Campaign Finance Database in minutes.

## 🎯 What This Tool Does

Ask questions like:
- **"How much did each candidate spend in the Arlington County Board primary election in 2024?"**
- **"Who are the top recipients from Dominion Energy in 2023?"**
- **"How much money did Glenn Youngkin receive from Dominion Energy in 2024?"**

The system automatically downloads Virginia's campaign finance data and lets you query it in plain English.

## 🚀 Quick Setup (5 Minutes)

### 1. Install Node.js
If you don't have Node.js installed:
- Visit https://nodejs.org
- Download and install the LTS version

### 2. Get the Code
```bash
# If you have the files already, just navigate to the folder
cd virginia-public-access

# Install required packages
npm install
```

### 3. Download Data and Build Database
```bash
# This will download recent data and build the database (takes 5-15 minutes)
npm run full-build
```

### 4. Start Querying!
```bash
# Ask questions in plain English:
npm start "How much did Glenn Youngkin spend in 2024?"

# Or use interactive mode:
npm start
```

## 💬 How to Ask Questions

### Natural Language Examples

**Campaign Spending:**
- "How much did Glenn Youngkin spend in 2024?"
- "List all candidates who spent money in Virginia in 2023"
- "What was the highest spending candidate in Richmond in 2024?"

**Election Analysis:**
- "How much did each candidate spend in the Arlington County Board primary election in 2024?"
- "Show me Norfolk mayor race spending in 2023"
- "List Virginia Beach city council candidates 2024"

**Contributor Analysis:**
- "Who are the top recipients from Dominion Energy in 2024?"
- "How much money did Tim Kaine receive from unions in 2023?"
- "Show me all contributions from Microsoft to Virginia candidates"

**Comparison Queries:**
- "Compare spending between Republican and Democratic candidates in 2024"
- "Top 10 highest spending House of Delegates candidates in 2023"

### Query Tips
✅ **Always include a year**: 2024, 2023, etc.
✅ **Be specific about locations**: Arlington, Richmond, Virginia Beach
✅ **Mention the office**: Governor, Senate, County Board, City Council
✅ **Use real names**: Glenn Youngkin, Tim Kaine, Dominion Energy

## 📋 Alternative Options

### Option 1: Try Sample Data First
```bash
npm run demo    # See how it works with sample data
npm start       # Then try interactive mode
```

### Option 2: Download Only Recent Data
```bash
npm run download-recent     # Downloads last 2 years only (faster)
npm run build --skip-download
npm start
```

### Option 3: Manual Control
```bash
# Download specific amounts of data:
npm run download-test       # Just 2 folders for testing
npm run download-all        # All data 1999-2025 (very slow)

# Validate what you downloaded:
npm run validate

# Build database from downloaded data:
npm run build --skip-download
```

## 🎮 Interactive Mode

Run `npm start` with no arguments to get a menu:

```
Virginia Campaign Finance Database
==================================

Database Statistics:
  Total Transactions: 45,123
  Year Range: 2020-2024
  Unique Candidates: 2,341
  Total Amount: $89,456,789

Available Queries:
  1. Arlington County Board 2024 spending
  2. Dominion Energy contributions 2024  
  3. Glen Youngkin - Dominion money 2024
  4. Highest spending candidates 2024
  5. Search candidates
  6. Search contributors  
  7. Natural language query (Ask in plain English!)
  8. Query examples and help
  9. Exit
```

Choose option **7** to ask questions in plain English!

## 🔧 Troubleshooting

### "No data found"
- Make sure you ran `npm run full-build` or `npm run download-recent`
- Check if downloads worked: `npm run validate`
- Try the demo first: `npm run demo`

### Downloads fail
- Your IP might be blocked by Virginia's server
- Try using a VPN (especially Virginia-based)
- Or manually download files (see README.md)

### Queries return no results
- Check if you're using the right year (data might not be available yet)
- Try broader queries: "spending in Virginia in 2023" instead of very specific ones
- Use the interactive mode to explore what data is available

### Installation issues
- Make sure you have Node.js 16+ installed
- Try deleting `node_modules` and running `npm install` again

## 📚 More Examples

Once you have data, try these queries:

```bash
# Find big spenders
npm start "Top 10 highest spending candidates in Virginia in 2024"

# Analyze specific races  
npm start "How much did candidates spend in Richmond mayor race 2024"

# Corporate influence
npm start "Which candidates received the most from corporations in 2023"

# Geographic analysis
npm start "Compare spending between Northern Virginia and Richmond candidates"

# Detailed transactions
npm start "Show me all contributions over $10000 in 2024"
```

## 🎯 Real-World Use Cases

**For Journalists:**
- "Find the top corporate donors to gubernatorial candidates in 2021"
- "Compare fundraising between incumbents and challengers in House races"

**For Voters:**
- "How much did my county board candidates spend?"  
- "Who are the major donors to candidates in my district?"

**For Researchers:**
- "Analyze spending patterns in competitive vs safe districts"
- "Track PAC contributions across election cycles"

**For Activists:**
- "Which candidates took money from specific industries?"
- "Find candidates who rely mostly on small donors"

## 💡 Tips for Better Results

1. **Start broad, get specific**: Begin with "spending in Virginia in 2024" then narrow down
2. **Use real names**: The system knows "Glenn Youngkin", "Dominion Energy", etc.  
3. **Include context**: "primary election", "general election", "county board race"
4. **Try variations**: If one query doesn't work, rephrase it
5. **Use the help**: Type `npm start` and choose option 8 for more examples

## 🚀 Ready to Start?

```bash
npm run full-build  # Download data and build database (first time only)
npm start           # Start querying!
```

**Your first query could be:**
```bash
npm start "What was the highest spending candidate in Virginia in 2024?"
```

Have fun exploring Virginia's campaign finance data! 🎉