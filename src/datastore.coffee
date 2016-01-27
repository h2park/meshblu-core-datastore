_       = require 'lodash'

class Datastore
  constructor: ({database,collection}) ->
    @db = database.collection collection

  find: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    projection ?= {}
    projection._id = false

    cursor = @db.find query, projection

    _.defer => cursor.toArray callback
    return cursor

  findOne: (query, projection, callback) =>
    if _.isFunction projection
      callback ?= projection
      projection = undefined

    projection ?= {}
    projection._id = false

    @db.findOne query, projection, callback

  insert: (record, callback) =>
    @db.insert record, (error, ignored) =>
      callback error

  remove: =>
    @db.remove.apply @db, arguments

  update: (args...) =>
    @db.update args...

module.exports = Datastore
