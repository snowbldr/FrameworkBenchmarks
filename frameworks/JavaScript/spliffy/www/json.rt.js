const { sjs, attr } = require('slow-json-stringify');
const jsonSerializer = sjs({ message: attr("string")});

// returning the headers with the body lets spliffy answer with one write on the raw uws response
const headers = { server: 'spliffy', 'content-type': 'application/json' };

module.exports = {
    GET: () => ({ headers, body: jsonSerializer({ message: 'Hello, World!' }) })
}
