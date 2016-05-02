_         = require 'lodash'
async     = require 'async'
crypto    = require 'crypto'
stringify = require 'json-stable-stringify'

class Datastore
  constructor: ({database,collection,@cache,@cacheAttributes}) ->
    throw new Error('Datastore: requires database') if _.isEmpty database
    throw new Error('Datastore: requires collection') if _.isEmpty collection
    @db = database.collection collection
    @queryCacheKey = "query:#{collection}"

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
    unless _.isEmpty projection
      _.each @cacheAttributes, (attribute) =>
        projection[attribute] = true

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
    @db.insert record, (error) =>
      return callback error if error?
      @_clearQueryCache callback

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
    @db.findOne query, (error, existingRecord) =>
      return callback error if error?
      # need to clear cache if there is no match, we're about to insert
      @_clearQueryCache(@_logError) unless existingRecord?
      @db.update query, data, {upsert: true}, (error) =>
        return callback error if error?
        @_clearCacheRecord {query}, callback

  _findCacheRecord: ({query, projection}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @_getCacheRecord {cacheKey, cacheField}, callback

  _findCacheRecords: ({query, projection}, callback) =>
    return callback() unless @cache?
    queryCacheField = @_generateCacheField {query, projection}
    return callback() unless queryCacheField?
    @_getCacheRecord {cacheKey: @queryCacheKey, cacheField: queryCacheField}, (error, data) =>
      return callback() unless data?

      async.map _.keys(data), (cacheKey, done) =>
        cacheField = data[cacheKey]
        return done() unless cacheKey? && cacheField?
        @_getCacheRecord {cacheKey, cacheField}, done
      , (error, records) =>
        return callback error if error?
        return callback() unless _.size(_.flatten(_.compact(records))) == _.size(data)
        callback null, records

  _logError: (error) =>
    return unless error?
    console.error 'Error: ', error.message

  _updateCacheRecord: ({query, projection, data}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @_setCacheRecord {cacheKey, cacheField, data}, callback

  _updateCacheRecords: ({query, projection, data}, callback) =>
    return callback() unless @cache?
    records = {}
    async.eachSeries data, (record, done) =>
      attributes = _.pick record, @cacheAttributes
      recordCacheKey = @_generateCacheKey {query: attributes}
      recordCacheField = @_generateCacheField {query: attributes, projection: projection}
      records[recordCacheKey] = recordCacheField if recordCacheKey?
      @_updateCacheRecord {query: attributes, projection: projection, data: record}, done
    , (error) =>
      return callback error if error?
      return callback() unless _.size(data) == _.size(records)
      queryCacheField = @_generateCacheField {query, projection}
      @_setCacheRecord {cacheKey: @queryCacheKey, cacheField: queryCacheField, data: records}, callback

  _clearCacheRecord: ({query}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    @cache.del cacheKey, (error) =>
      # ignore any redis return values
      callback error

  _clearQueryCache: (callback) =>
    return callback() unless @cache?
    @cache.del @queryCacheKey, (error) =>
      # ignore redis callback
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

  _getCacheRecord: ({cacheKey, cacheField}, callback) =>
    return callback() unless @cache?
    @cache.hget cacheKey, cacheField, (error, data) =>
      return callback error if error?
      return callback() unless data?
      try
        data = JSON.parse data
      catch error
        # if it's not valid throw it away
        data = null

      callback null, data

  _setCacheRecord: ({cacheKey, cacheField, data}, callback) =>
    return callback() unless @cache?
    @cache.hset cacheKey, cacheField, JSON.stringify(data), (error) =>
      return callback error if error?
      @cache.expire cacheKey, 60 * 60 * 1000, (error) =>
        # ignore any redis return values
        callback error

module.exports = Datastore
