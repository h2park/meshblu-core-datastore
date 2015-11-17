mongojs   = require 'mongojs'
async     = require 'async'
Datastore = require '../src/datastore'

describe 'Datastore', ->
  describe '->find', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   mongojs('datastore-find-test')
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

      describe 'when find is called', ->
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

  describe '->findOne', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   mongojs('datastore-findOne-test')
        collection: 'things'

      @db = mongojs 'datastore-findOne-test', ['things']
      @db.things.remove done

    describe 'on a record that exists', ->
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

    describe 'on a record that does not exist', ->
      beforeEach (done) ->
        @sut.findOne uuid: 'blank', (error, @result) => done error

      it 'should yield a non extant record', ->
        expect(@result).not.to.exist

  describe '->insert', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   mongojs('datastore-insert-test')
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
        expect(@result).not.to.exist

      it 'should store the thing', (done) ->
        @db.jalapenos.findOne uuid: 'goose', (error, record) =>
          return done error if error?
          expect(record).to.containSubset
            uuid: 'goose'
            token: 'Duck, duck, DEAD'
          done()

  describe '->remove', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   mongojs('datastore-remove-test')
        collection: 'things'

      @db = mongojs 'datastore-remove-test', ['things']
      @db.things.remove done

    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @db.things.insert record, done

      describe 'when called with an open query and options', ->
        beforeEach (done) ->
          @sut.remove {}, {}, done

        it 'should empty the collection', (done) ->
          @db.things.count {}, (error, count) =>
            return done error if error?
            expect(count).to.equal 0
            done()

      describe 'when called with an no query or options', ->
        beforeEach (done) ->
          @sut.remove done

        it 'should empty the collection', (done) ->
          @db.things.count {}, (error, count) =>
            return done error if error?
            expect(count).to.equal 0
            done()

  describe '->update', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   mongojs('datastore-update-test')
        collection: 'nails'

      @db = mongojs 'datastore-update-test', ['nails']
      @db.nails.remove done

    describe 'when an object exists', ->
      beforeEach (done) ->
        @db.nails.insert uuid: 'hardware', byline: 'Does it grate?', done

      describe 'when called with an object', ->
        beforeEach (done) ->
          query  = uuid: 'hardware'
          update = $set: {byline: 'Lee Press-Ons?'}
          @sut.update query, update, (error, @result) => done error

        it 'should yield the number of records updated', ->
          expect(@result).to.containSubset n: 1

        it 'should update the thing', (done) ->
          @db.nails.findOne uuid: 'hardware', (error, record) =>
            return done error if error?
            expect(record).to.containSubset
              uuid: 'hardware'
              byline: 'Lee Press-Ons?'
            done()

  describe 'when find is called an ubsurd number of times', ->
    beforeEach (done) ->
      db = mongojs('datastore-find-test')
      async.times 175, (i, callback) =>
        sut = new Datastore
          database:   db
          collection: 'things'
        sut.find type: 'campfire', callback
      , done

    it 'should probably work ok', ->
