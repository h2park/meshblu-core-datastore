_         = require 'lodash'
async     = require 'async'
crypto    = require 'crypto'
stringify = require 'json-stable-stringify'

class Datastore
  constructor: ({database,collection,@cache,@cacheAttributes,@useQueryCache,@ttl}) ->
    throw new Error('Datastore: requires database') unless database?
    throw new Error('Datastore: requires collection') unless collection?
    @ttl ?= 60 * 60
    @db = database.collection collection
    @dbRecycle = database.collection "deleted-#{collection}"

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
      falsey = _.some _.values(projection), (value) => value == false
      unless falsey
        _.each @cacheAttributes, (attribute) =>
          projection[attribute] = true

    projection._id = false

    @db.find query, projection, options, (error, data) =>
      return callback error if error?
      @_updateCacheRecords {projection, data}, (error) =>
        return callback error if error?
        callback null, data

  findAndUpdate: ({ query, update, projection }, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    projection ?= {}
    unless _.isEmpty projection
      falsey = _.some _.values(projection), (value) => value == false
      unless falsey
        _.each @cacheAttributes, (attribute) =>
          projection[attribute] = true

    projection._id = false
    sort = {'_id': 1}
    @db.findAndModify { query, sort, update, fields: projection }, (error, result) =>
      return callback error if error?
      @_clearCacheRecord {query}, (error) =>
        return callback error if error?
        callback null, result

  findOne: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    return callback new Error("Datastore: requires query") if _.isEmpty query

    projection ?= {}
    unless _.isEmpty projection
      falsey = _.some _.values(projection), (value) => value == false
      unless falsey
        _.each @cacheAttributes, (attribute) =>
          projection[attribute] = true

    projection._id = false

    @_findCacheRecord {query, projection}, (error, data) =>
      return callback error if error?
      return callback null, data if data?
      @db.findOne query, projection, (error, data) =>
        return callback error if error?
        @_updateCacheRecord {query, projection, data}, (error) =>
          return callback error if error?
          callback null, data

  findOneRecycled: (query, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @dbRecycle.findOne query, callback

  insert: (record, callback) =>
    @db.insert record, (error) =>
      callback error

  recycle: (query, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.find query, (error, records) =>
      async.eachSeries records, (record, next) =>
        @dbRecycle.insert record, next
      , (error) =>
        return callback error if error?
        @db.remove query, callback

  remove: (query, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.remove query, (error) =>
      return callback error if error?
      @_clearCacheRecord {query}, callback

  update: (query, data, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.update query, data, (error, result) =>
      return callback error if error?
      return callback null, updated: false if result.nModified == 0
      @_clearCacheRecord {query}, (error) =>
        return callback error if error?
        return callback null, updated: true

  upsert: (query, data, callback) =>
    return callback new Error("Datastore: requires query") if _.isEmpty query
    @db.update query, data, {upsert: true}, (error) =>
      return callback error if error?
      @_clearCacheRecord {query}, callback

  _findCacheRecord: ({query, projection}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @_getCacheRecord {cacheKey, cacheField}, callback

  _logError: (error) =>
    return unless error?
    console.error 'Error: ', error.message

  _updateCacheRecord: ({query, projection, data}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    cacheField = @_generateCacheField {query, projection}
    @_setCacheRecord {cacheKey, cacheField, data}, callback

  _updateCacheRecords: ({projection, data}, callback) =>
    return callback() unless @cache?
    records = {}
    async.eachLimit data, 100, (record, done) =>
      attributes = _.pick record, @cacheAttributes
      recordCacheKey = @_generateCacheKey {query: attributes}
      recordCacheField = @_generateCacheField {query: attributes, projection: projection}
      records[recordCacheKey] = recordCacheField if recordCacheKey?
      @_updateCacheRecord {query: attributes, projection: projection, data: record}, done
    , callback

  _clearCacheRecord: ({query}, callback) =>
    return callback() unless @cache?
    cacheKey = @_generateCacheKey {query}
    return callback() unless cacheKey?
    @cache.del cacheKey, (error) =>
      # ignore any redis return values
      callback error
    return # redis fix

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
      data = @_tryJSON data
      @_setExpireRecord { cacheKey }, (error) =>
        return callback error if error?
        callback null, data
    return # redis fix

  _setCacheRecord: ({cacheKey, cacheField, data}, callback) =>
    return callback() unless @cache?
    @cache.hset cacheKey, cacheField, JSON.stringify(data), (error) =>
      return callback error if error?
      @_setExpireRecord { cacheKey }, callback
    return # redis fix

  _setExpireRecord: ({cacheKey}, callback) =>
    @cache.expire cacheKey, @ttl, (error) => callback error
    return # redis fix

  _tryJSON: (str) =>
    try return JSON.parse str

module.exports = Datastore
