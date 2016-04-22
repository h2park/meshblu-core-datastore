_         = require 'lodash'
crypto    = require 'crypto'
stringify = require 'json-stable-stringify'

class Datastore
  constructor: ({database,collection,@cache,@cacheAttributes}) ->
    throw new Error('Datastore: requires database') if _.isEmpty database
    throw new Error('Datastore: requires collection') if _.isEmpty collection
    @db = database.collection collection

  find: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    return callback new Error("Datastore: requires query") if _.isEmpty query
    projection ?= {}
    projection._id = false

    cursor = @db.find query, projection

    _.defer => cursor.toArray callback
    return cursor

  findOne: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    return callback new Error("Datastore: requires query") if _.isEmpty query

    projection ?= {}
    projection._id = false

    @_findCacheRecord {query, projection}, (error, data) =>
      return callback error if error?
      return callback null, JSON.parse(data) if data?
      @db.findOne query, projection, (error, data) =>
        return callback error if error?
        @_updateCacheRecord {query, projection, data}, (error) =>
          return callback error if error?
          callback null, data

  insert: (record, callback) =>
    @db.insert record, (error, ignored) =>
      callback error

  remove: (query, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.remove query, (error) =>
      return callback error if error?
      @_clearCacheRecord {query}, callback

  update: (query, data, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.update query, data, (error) =>
      return callback error if error?
      @_clearCacheRecord {query}, callback

  _findCacheRecord: ({query, projection}, callback) =>
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @cache.hget cacheKey, cacheField, callback

  _updateCacheRecord: ({query, projection, data}, callback) =>
    cacheKey   = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @cache.hset cacheKey, cacheField, JSON.stringify(data), (error) =>
      return callback error if error?
      @cache.expire cacheKey, 60 * 60 * 1000, callback

  _clearCacheRecord: ({query}, callback) =>
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    @cache.del cacheKey, callback

  _generateCacheField: ({query, projection}) =>
    cacheField = stringify(query) + stringify(projection || '')
    crypto.createHash('sha1').update(cacheField).digest('hex')

  _generateCacheKey: ({query}) =>
    return unless @cacheAttributes?
    attributes = _.pick query, @cacheAttributes
    return unless _.size(_.keys(attributes)) == _.size @cacheAttributes
    cacheKey = stringify(attributes)
    crypto.createHash('sha1').update(cacheKey).digest('hex')

module.exports = Datastore
