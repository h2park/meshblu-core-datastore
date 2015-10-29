_       = require 'lodash'
mongojs = require 'mongojs'

class Datastore
  constructor: ({@database,@collection}) ->
    @db      = mongojs @database, [@collection]

  find: (query, callback) =>
    @db[@collection].find query, (error, records) =>
      records = _.map records, (record) => _.omit record, '_id'
      callback error, records

  findOne: (query, callback) =>
    @db[@collection].findOne query, (error, record) =>
      return callback error if error?
      record = _.omit(record, '_id') if record?
      return callback null, record

  insert: (record, callback) =>
    @db[@collection].insert record, (error, ignored) =>
      callback error
  remove: =>
    @db[@collection].remove.apply @db[@collection], arguments

module.exports = Datastore
