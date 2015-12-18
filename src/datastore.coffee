_       = require 'lodash'

class Datastore
  constructor: ({database,collection}) ->
    @db = database.collection collection

  find: (query, callback) =>
    cursor = @db.find query, _id: false
    _.defer => cursor.toArray callback
    return cursor

  findOne: (query, callback) =>
    @db.findOne query, _id: false, callback

  insert: (record, callback) =>
    @db.insert record, (error, ignored) =>
      callback error

  remove: =>
    @db.remove.apply @db, arguments

  update: (args...) =>
    @db.update args...

module.exports = Datastore
