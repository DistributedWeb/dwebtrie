const ram = require('random-access-memory')
const dwebtrie = require('../../')

module.exports = function (key, opts) {
  opts = Object.assign({ valueEncoding: 'json' }, opts)
  return dwebtrie(ram, key, opts)
}
