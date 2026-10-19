import { declarative } from '@srfnstack/spliffy'

// A fixed response compiled once and served inside uWebSockets.js, no javascript runs per request.
export default {
  GET: declarative()
    .writeHeader('server', 'spliffy')
    .writeHeader('content-type', 'text/plain')
    .end('Hello, World!')
}
