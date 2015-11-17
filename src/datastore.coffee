_       = require 'lodash'

class Datastore
  constructor: ({database,collection}) ->
    @db = database.collection collection

  find: (query, callback) =>
    @db.find query, (error, records) =>
      records = _.map records, (record) => _.omit record, '_id'
      callback error, records

  findOne: (query, callback) =>
    @db.findOne query, (error, record) =>
      record = _.omit(record, '_id') if record?
      return callback error, record

  insert: (record, callback) =>
    @db.insert record, (error, ignored) =>
      callback error

  remove: =>
    @db.remove.apply @db, arguments

  update: (query, update, callback) =>
    @db.update query, update, callback

module.exports = Datastore
