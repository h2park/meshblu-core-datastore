mongojs = require 'mongojs'
Datastore = require '../src/datastore'

describe 'Datastore', ->
  beforeEach ->

  describe '->insert', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-insert-test'
        collection: 'devices'

      @db = mongojs 'datastore-insert-test', ['devices']
      @db.devices.remove done

    describe 'when called with an object', ->
      beforeEach (done) ->
        record =
          uuid: 'goose'
          token: 'Duck, duck, DEAD'
        @sut.insert record, (error, @result) => done error

      it 'should yield nothing', ->
        expect(JSON.stringify @result).not.to.exist

      it 'should store the thing', (done) ->
        @db.devices.findOne uuid: 'goose', (error, record) =>
          return done error if error?
          expect(record).to.containSubset
            uuid: 'goose'
            token: 'Duck, duck, DEAD'
          done()

  describe '->findOne', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-findOne-test'
        collection: 'things'

      @db = mongojs 'datastore-findOne-test', ['things']
      @db.things.remove done

    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @db.things.insert record, done

      beforeEach (done) ->
        @sut.findOne uuid: 'sandbag', (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'

    describe 'when there exists no thing', ->
      beforeEach (done) ->
        @sut.findOne uuid: 'blank', (error, @result) => done error

      it 'should yield a non extant record', ->
        expect(@result).not.to.exist
