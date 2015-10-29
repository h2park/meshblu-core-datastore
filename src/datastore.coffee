_       = require 'lodash'
mongojs = require 'mongojs'

class Datastore
  constructor: ({@database,@collection,@cache}) ->
    @db      = mongojs @database, [@collection]

  find: (query, callback) =>
    @db[@collection].find query, (error, records) =>
      records = _.map records, (record) => _.omit record, '_id'
      callback error, records

  findOne: (query, callback) =>
    @findOneFromCache query.uuid, (error, record) =>
      return callback error if error?
      return callback null, record if record?

      @db[@collection].findOne query, (error, record) =>
        return callback error if error?
        return callback null, null unless record?
        record = _.omit(record, '_id') if record?

        @insertIntoCache record.uuid, record, (error) =>
          return callback error if error?
          return callback null, record

  findOneFromCache: (key, callback) =>
    @cache.get key, (error, recordStr) =>
      return callback error if error?
      return callback null, JSON.parse(recordStr) if recordStr?
      return callback null, null

  insert: (record, callback) =>
    @db[@collection].insert record, (error, ignored) =>
      callback error

  insertIntoCache: (key, record, callback) =>
    @cache.set key, JSON.stringify(record), (error) =>
      return callback error if error?
      return callback null, record

  remove: =>
    @db[@collection].remove.apply @db[@collection], arguments

module.exports = Datastore
