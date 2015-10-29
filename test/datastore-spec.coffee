mongojs   = require 'mongojs'
Datastore = require '../src/datastore'
redis     = require 'fakeredis'

describe 'Datastore', ->
  beforeEach ->
    @cache = redis.createClient()

  describe '->find', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-find-test'
        collection: 'things'
        cache: @cache

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

  describe '->findOne', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-findOne-test'
        collection: 'things'
        cache: @cache

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

    describe 'using the cache', ->
      beforeEach (done) ->
        record =
          uuid: 'absolute-zero'
          byline: 'nowhere to go but up'
        @db.things.insert record, done

      describe 'when the cache is warmed up', ->
        beforeEach (done) ->
          @sut.findOne uuid: 'absolute-zero', done

        describe 'when the persistent store is wiped out-of-band', ->
          beforeEach (done) ->
            @db.things.remove done

          beforeEach (done) ->
            @sut.findOne uuid: 'absolute-zero', (error, @result) => done error

          it 'should yield the cached record', ->
            expect(@result).to.deep.equal
              uuid: 'absolute-zero'
              byline: 'nowhere to go but up'

  describe '->insert', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-insert-test'
        collection: 'jalapenos'
        cache: @cache

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

  describe '->insertIntoCache', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-insert-cache-only-test'
        collection: 'jets'
        cache: @cache

      @db = mongojs 'datastore-insert-cache-only-test', ['jets']
      @db.jets.remove done

    describe 'when called with something', ->
      beforeEach (done) ->
        record =
          uuid: 'shot-out-of-cannon'
          byline: 'never trust a clown'

        @sut.insertIntoCache 'shot-out-of-cannon', record, (error, @result) => done error

      it 'should pass through the record', ->
        expect(@result).to.deep.equal
          uuid: 'shot-out-of-cannon'
          byline: 'never trust a clown'

      it 'should be retrievable using findOne', (done) ->
        @sut.findOne uuid: 'shot-out-of-cannon', (error, record) ->
          return done error if error?
          expect(record).to.deep.equal
            uuid: 'shot-out-of-cannon'
            byline: 'never trust a clown'
          done()

      it 'should not be in the database', (done) ->
        @db.jets.findOne uuid: 'shot-out-of-cannon', (error, record) ->
          return done error if error?
          expect(record).not.to.exist
          done()

  describe '->remove', ->
    beforeEach (done) ->
      @sut = new Datastore
        database:   'datastore-remove-test'
        collection: 'things'
        cache: @cache

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
