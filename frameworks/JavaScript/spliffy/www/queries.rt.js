const db = require('../db')
const { parseCount, randomUniqueIds } = require("../fn");

const headers = { server: 'spliffy', 'content-type': 'application/json' };

module.exports = {
    GET: async ({ url }) => {
        const count = parseCount(url.query.queries);
        const ids = randomUniqueIds(count)
        const results = [];
        for (let i = 0; i < count; i++) {
            results.push(db.worldById(ids[i]));
        }
        const worlds = await Promise.all(results);
        return { headers, body: JSON.stringify(worlds) };
    }
}
