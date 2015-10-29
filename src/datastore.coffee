_ = require 'lodash'
mongojs = require 'mongojs'

class Datastore
  constructor: ({@database,@collection}) ->
    @db = mongojs @database, [@collection]

  find: (query, callback) =>
    @db[@collection].find query, (error, records) =>
      records = _.map records, (record) => _.omit record, '_id'
      callback error, records

  findOne: (query, callback) =>
    @db[@collection].findOne query, (error, record) =>
      record = _.omit(record, '_id') if record?
      callback error, record

  insert: (record, callback) =>
    @db[@collection].insert record, (error, ignored) =>
      callback error

module.exports = Datastore
