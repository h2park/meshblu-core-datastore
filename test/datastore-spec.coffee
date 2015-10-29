mongojs = require 'mongojs'
Datastore = require '../src/datastore'

describe 'Datastore', ->
  beforeEach ->

  describe '->insert', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-insert-test'
        collection: 'jalapenos'

      @db = mongojs 'datastore-insert-test', ['jalapenos']
      @db.jalapenos.remove done

    describe 'when called with an object', ->
      beforeEach (done) ->
        record =
          uuid: 'goose'
          token: 'Duck, duck, DEAD'
        @sut.insert record, (error, @result) => done error

      it 'should yield nothing', ->
        expect(JSON.stringify @result).not.to.exist

      it 'should store the thing', (done) ->
        @db.jalapenos.findOne uuid: 'goose', (error, record) =>
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

  describe '->find', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-find-test'
        collection: 'things'

      @db = mongojs 'datastore-find-test', ['things']
      @db.things.remove done

    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'wood'
          type: 'campfire'
          token: 'I bet you can\'t jump over it'
        @db.things.insert record, done

      beforeEach (done) ->
        record =
          uuid: 'marshmellow'
          type: 'campfire'
          token: 'How long can you hold your hand in the fire?'
        @db.things.insert record, done

      beforeEach (done) ->
        @sut.find type: 'campfire', (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal [
          {uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'}
          {uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'}
        ]

    describe 'when there exists no thing', ->
      beforeEach (done) ->
        @sut.find type: 'nobody cares', (error, @result) => done error

      it 'should yield a empty array', ->
        expect(@result).to.be.empty
        expect(@result).to.be.array
