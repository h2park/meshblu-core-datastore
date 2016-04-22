_         = require 'lodash'
async     = require 'async'
crypto    = require 'crypto'
stringify = require 'json-stable-stringify'

class Datastore
  constructor: ({database,collection,@cache,@cacheAttributes}) ->
    throw new Error('Datastore: requires database') if _.isEmpty database
    throw new Error('Datastore: requires collection') if _.isEmpty collection
    @db = database.collection collection

  find: (query, projection, options, callback) =>
    if _.isFunction options
      callback ?= options
      options = undefined

    if _.isFunction projection
      callback ?= projection
      projection = undefined

    return callback new Error("Datastore: requires query") if _.isEmpty query
    options ?= {}
    projection ?= {}
    projection._id = false

    @_findCacheRecords {query, projection}, (error, data) =>
      return callback error if error?
      return callback null, data if data?
      @db.find query, projection, options, (error, data) =>
        return callback error if error?
        @_updateCacheRecords {query, projection, data}, (error) =>
          return callback error if error?
          callback null, data

  findOne: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    return callback new Error("Datastore: requires query") if _.isEmpty query

    projection ?= {}
    projection._id = false

    @_findCacheRecord {query, projection}, (error, data) =>
      return callback error if error?
      return callback null, data if data?
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

  upsert: (query, data, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.update query, data, {upsert: true}, (error) =>
      return callback error if error?
      @_clearCacheRecord {query}, callback

  _findCacheRecord: ({query, projection}, callback) =>
    return callback() unless @cache
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @cache.hget cacheKey, cacheField, (error, data) =>
      return callback error if error?
      try
        data = JSON.parse data
      catch error
        # if it's not valid throw it away
        data = null

      callback null, data

  _findCacheRecords: ({query, projection}, callback) =>
    return callback() unless @cache
    cacheKey = @_generateCacheField {query, projection}
    return callback() unless cacheKey?
    @cache.get cacheKey, (error, data) =>
      return callback error if error?
      return callback() unless data?
      try
        data = JSON.parse(data)
      catch error
        return callback()

      return callback() unless data?

      async.map _.keys(data), (key, done) =>
        value = data[key]
        @cache.hget key, value, (error, x) =>
          return done error if error?
          return done() unless x?

          try
            x = JSON.parse x
          catch error
            return done()

          return done null, x
      , (error, records) =>
        return callback error if error?
        return callback() unless _.size(_.flatten(_.compact(records))) == _.size(data)
        callback null, records

  _updateCacheRecord: ({query, projection, data}, callback) =>
    return callback() unless @cache
    cacheKey   = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @cache.hset cacheKey, cacheField, JSON.stringify(data), (error) =>
      return callback error if error?
      @cache.expire cacheKey, 60 * 60 * 1000, (error) =>
        # ignore any redis return values
        callback error

  _updateCacheRecords: ({query, projection, data}, callback) =>
    return callback() unless @cache
    records = {}
    async.eachSeries data, (record, done) =>
      recordCacheKey = @_generateCacheKey {query: record}
      recordCacheField = @_generateCacheField {query: record, projection}
      records[recordCacheKey] = recordCacheField if recordCacheKey?
      @_updateCacheRecord {query: record, projection, data: record}, done
    , (error) =>
      return callback error if error?
      return callback() if _.isEmpty records
      return callback() unless _.size(data) == _.size(records)
      cacheKey = @_generateCacheField {query, projection}
      @cache.setex cacheKey, 60 * 60 * 1000, JSON.stringify(records), callback

  _clearCacheRecord: ({query}, callback) =>
    return callback() unless @cache
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    @cache.del cacheKey, (error) =>
      # ignore any redis return values
      callback error

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
