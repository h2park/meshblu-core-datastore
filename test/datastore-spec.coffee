mongojs   = require 'mongojs'
async     = require 'async'
Datastore = require '../src/datastore'
Cache     = require 'meshblu-core-cache'
redis     = require 'fakeredis'
RedisNS   = require '@octoblu/redis-ns'
UUID      = require 'uuid'

describe 'Datastore', ->
  beforeEach (done) ->
    redisKey = UUID.v4()
    client = new RedisNS 'datastore:test:things', redis.createClient redisKey
    cache = new Cache {client}
    @redis = new RedisNS 'datastore:test:things', redis.createClient redisKey

    @sut = new Datastore
      database:   mongojs('datastore-test')
      collection: 'things'
      cache: cache
      cacheAttributes: ['uuid']
      useQueryCache: true

    @db = mongojs 'datastore-test', ['things']
    @db.things.remove done

  describe '->find', ->
    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'wood'
          type: 'campfire'
          token: 'I bet you can\'t jump over it'
        @sut.insert record, done

      beforeEach (done) ->
        record =
          uuid: 'marshmellow'
          type: 'campfire'
          token: 'How long can you hold your hand in the fire?'
        @sut.insert record, done

      describe 'when find is called', ->
        beforeEach (done) ->
          @sut.find type: 'campfire', (error, @result) => done error

        it 'should yield the record without mongo stuff', ->
          expect(@result).to.deep.equal [
            {uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'}
            {uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'}
          ]

        it 'should add to the search cache', (done) ->
          @redis.hget 'query:things', 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal {
              '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '874dbf9e6e84121a057e6ad2b9c047ebc95150f3'
              '9c77a994790ddf88bc197b11091643662c999a30': '2e9fb62f7fe1d2231b4a09f4d172cd808372327c'
            }
            done()

        it 'should add to the cache', (done) ->
          @redis.hget '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '874dbf9e6e84121a057e6ad2b9c047ebc95150f3', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
            done()

        it 'should add the other to the cache', (done) ->
          @redis.hget '9c77a994790ddf88bc197b11091643662c999a30', '2e9fb62f7fe1d2231b4a09f4d172cd808372327c', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'
            done()

    describe 'when a record is already cached', ->
      beforeEach (done) ->
        data =
          '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '9635cce604dbe5de11fe870a88e250115a3bda4d'
          '9c77a994790ddf88bc197b11091643662c999a30': '77e560dbcffbfd744248a6ff9e6d29de2763e35f'

        @redis.hset 'query:things', 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
        @redis.hset '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '9635cce604dbe5de11fe870a88e250115a3bda4d', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'
        @redis.hset '9c77a994790ddf88bc197b11091643662c999a30', '77e560dbcffbfd744248a6ff9e6d29de2763e35f', JSON.stringify(data), done

      context 'when all the records exist', ->
        beforeEach (done) ->
          @sut.find type: 'campfire', (error, @result) => done error

        it 'should yield the record without mongo stuff', ->
          expect(@result).to.deep.equal [
            {uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'}
            {uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'}
          ]

    describe 'when a record is missing', ->
      beforeEach (done) ->
        data =
          '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '9635cce604dbe5de11fe870a88e250115a3bda4d'
          '9c77a994790ddf88bc197b11091643662c999a30': '77e560dbcffbfd744248a6ff9e6d29de2763e35f'

        @redis.hset 'query:things', 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
        @redis.hset '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '9635cce604dbe5de11fe870a88e250115a3bda4d', JSON.stringify(data), done

      context 'when all the records exist', ->
        beforeEach (done) ->
          @sut.find type: 'campfire', (error, @result) => done error

        it 'should yield the record without mongo stuff', ->
          expect(@result).to.deep.equal []

    describe 'with a projection', ->
      beforeEach (done) ->
        record =
          uuid: 'wood'
          type: 'campfire'
          token: 'I bet you can\'t jump over it'
        @sut.insert record, done

      beforeEach (done) ->
        record =
          uuid: 'marshmellow'
          type: 'campfire'
          token: 'How long can you hold your hand in the fire?'
        @sut.insert record, done

      describe 'when find is called', ->
        beforeEach (done) ->
          @sut.find {type: 'campfire'}, {type: true, uuid: true}, (error, @result) => done error

        it 'should yield the record without mongo stuff', ->
          expect(@result).to.deep.equal [
            {uuid: 'wood', type: 'campfire'}
            {uuid: 'marshmellow', type: 'campfire'}
          ]

        it 'should add to the search cache', (done) ->
          @redis.hget 'query:things', '592e16c142809fcd4f5930e933f79a3980939f49', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal {
              '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '9635cce604dbe5de11fe870a88e250115a3bda4d'
              '9c77a994790ddf88bc197b11091643662c999a30': '77e560dbcffbfd744248a6ff9e6d29de2763e35f'
            }
            done()

        it 'should add to the cache', (done) ->
          @redis.hget '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '9635cce604dbe5de11fe870a88e250115a3bda4d', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'wood', type: 'campfire'
            done()

        it 'should add the other to the cache', (done) ->
          @redis.hget '9c77a994790ddf88bc197b11091643662c999a30', '77e560dbcffbfd744248a6ff9e6d29de2763e35f', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'marshmellow', type: 'campfire'
            done()

    describe 'when there exists no thing', ->
      beforeEach (done) ->
        @sut.find type: 'nobody cares', (error, @result) => done error

      it 'should yield a empty array', ->
        expect(@result).to.be.empty
        expect(@result).to.be.array

      it 'should add to the search cache', (done) ->
        @redis.hget 'query:things', '6f6903006fed8fdff009079cf659b624c8a46403', (error, data) =>
          return done error if error?
          expect(JSON.parse data).to.deep.equal {}
          done()

  describe '->findOne', ->
    describe 'on a record that exists', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @sut.insert record, done

      beforeEach (done) ->
        @sut.findOne uuid: 'sandbag', (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'

      it 'should add to the cache', (done) ->
        @redis.hget '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', 'cfc702c2d593c0667981e3220a271912e456fe61', (error, data) =>
          return done error if error?
          expect(JSON.parse data).to.deep.equal uuid: 'sandbag', token: 'This’ll hold that pesky tsunami!'
          done()

    describe 'record is already cached', ->
      beforeEach (done) ->
        @redis.hset '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', 'cfc702c2d593c0667981e3220a271912e456fe61', JSON.stringify('hi': 'there'), done

      beforeEach (done) ->
        @sut.findOne uuid: 'sandbag', (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal
          hi: 'there'

    describe 'on a record that does not exist', ->
      beforeEach (done) ->
        @sut.findOne uuid: 'blank', (error, @result) => done error

      it 'should yield a non extant record', ->
        expect(@result).not.to.exist

    describe 'on with projection', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @sut.insert record, done

      beforeEach (done) ->
        @sut.findOne {uuid: 'sandbag'}, {token: false}, (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal
          uuid: 'sandbag'

      it 'should cache the projection', (done) ->
        @redis.hget '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', '18ba252583142b0a3a85fc47f56852630f8dfb5c', (error, data) =>
          return done error if error?
          expect(JSON.parse data).to.deep.equal uuid: 'sandbag'
          done()

    describe 'with a different projection', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
          spork: 'bork'
        @sut.insert record, done

      beforeEach (done) ->
        @sut.findOne {uuid: 'sandbag'}, {uuid: true, spork: true}, (error, @result) => done error

      it 'should yield the record without mongo stuff', ->
        expect(@result).to.deep.equal
          uuid: 'sandbag'
          spork: 'bork'

      it 'should cache the projection', (done) ->
        @redis.hget '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', '3b71f93def87ecb1fc0f44eda7e588a3cd4eef95', (error, data) =>
          return done error if error?
          expect(JSON.parse data).to.deep.equal uuid: 'sandbag', spork: 'bork'
          done()

  describe '->insert', ->
    describe 'when called with an object', ->
      beforeEach (done) ->
        @redis.hset 'query:things', 'blah', 'blah', done

      beforeEach (done) ->
        record =
          uuid: 'goose'
          token: 'Duck, duck, DEAD'
        @sut.insert record, (error, @result) => done error

      it 'should yield nothing', ->
        expect(@result).not.to.exist

      it 'should store the thing', (done) ->
        @sut.findOne uuid: 'goose', (error, record) =>
          return done error if error?
          expect(record).to.containSubset
            uuid: 'goose'
            token: 'Duck, duck, DEAD'
          done()

      it 'should remove the query cache', (done) ->
        @redis.exists 'query:things', (error, exists) =>
          return done error if error?
          expect(exists).to.equal 0
          done()

  describe '->upsert', ->
    describe 'when called with an object', ->
      beforeEach (done) ->
        @redis.hset 'query:things', 'blah', 'blah', done

      beforeEach (done) ->
        record =
          uuid: 'goose'
          token: 'Duck, duck, DEAD'
        @sut.upsert {uuid: 'goose'}, record, (error) => done error

      it 'should store the thing', (done) ->
        @sut.findOne uuid: 'goose', (error, record) =>
          return done error if error?
          expect(record).to.containSubset
            uuid: 'goose'
            token: 'Duck, duck, DEAD'
          done()

      it 'should remove the query cache', (done) ->
        @redis.exists 'query:things', (error, exists) =>
          return done error if error?
          expect(exists).to.equal 0
          done()

  describe '->remove', ->
    beforeEach (done) ->
      @redis.hset 'sandbag', 'foo', 'bar', done

    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @sut.insert record, done

      describe 'when called with a query', ->
        beforeEach (done) ->
          @sut.remove uuid: 'sandbag', done

        it 'should remove the record', (done) ->
          @sut.findOne uuid: 'sandbag', (error, device) =>
            return done error if error?
            expect(device).not.to.exist
            done()

        it 'should clear the cache', (done) ->
          @redis.exists '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', (error, exists) =>
            return done error if error?
            expect(exists).to.equal 0
            done()

  describe '->recycle', ->
    beforeEach (done) ->
      @redis.hset 'sandbag', 'foo', 'bar', done

    describe 'when there exists a thing', ->
      beforeEach (done) ->
        record =
          uuid: 'sandbag'
          token: 'This’ll hold that pesky tsunami!'
        @sut.insert record, done

      describe 'when called with a query', ->
        beforeEach (done) ->
          @sut.recycle uuid: 'sandbag', done

        it 'should insert the record into the deleted collection', (done) ->
          @sut.findOneRecycled uuid: 'sandbag', (error, device) =>
            return done error if error?
            expect(device).to.exist
            done()

        it 'should remove the record', (done) ->
          @sut.findOne uuid: 'sandbag', (error, device) =>
            return done error if error?
            expect(device).not.to.exist
            done()

        it 'should clear the cache', (done) ->
          @redis.exists '779f48bb3d0177cb8c61d78e3c0899a5157cdcbd', (error, exists) =>
            return done error if error?
            expect(exists).to.equal 0
            done()

  describe '->update', ->
    describe 'when an object exists', ->
      beforeEach (done) ->
        @redis.hset '9f0f2e3f4d49c05e64727e8993f152f775e1f317', 'foo', 'bar', done

      beforeEach (done) ->
        @sut.insert uuid: 'hardware', byline: 'Does it grate?', done

      describe 'when called with an object that modifies the device', ->
        beforeEach (done) ->
          query  = uuid: 'hardware'
          update = $set: {byline: 'Lee Press-Ons?'}
          @sut.update query, update, (error, @result) => done error

        it 'should update the thing', (done) ->
          @sut.findOne uuid: 'hardware', (error, record) =>
            return done error if error?
            expect(record).to.containSubset
              uuid: 'hardware'
              byline: 'Lee Press-Ons?'
            done()

        it 'should clear the cache', (done) ->
          @redis.exists '9f0f2e3f4d49c05e64727e8993f152f775e1f317', (error, exists) =>
            return done error if error?
            expect(exists).to.equal 0
            done()

        it 'should return a updated: true', ->
          expect(@result.updated).to.be.true

      describe 'when called with an object that does not modify the device', ->
        beforeEach (done) ->
          query  = uuid: 'hardware'
          update = $set: {byline: 'Does it grate?'}
          @sut.update query, update, (error, @result) => done error

        it 'should not clear the cache', (done) ->
          @redis.exists '9f0f2e3f4d49c05e64727e8993f152f775e1f317', (error, exists) =>
            return done error if error?
            expect(exists).to.equal 1
            done()

        it 'should return an updated: false', ->
          expect(@result.updated).to.be.false

  describe '->findAndUpdate', ->
    describe 'when an object exists', ->
      beforeEach (done) ->
        @redis.hset '9f0f2e3f4d49c05e64727e8993f152f775e1f317', 'foo', 'bar', done

      beforeEach (done) ->
        @sut.insert uuid: 'hardware', byline: 'Does it grate?', done

      describe 'when called with an object', ->
        beforeEach (done) ->
          query  = uuid: 'hardware'
          update = $set: {byline: 'Lee Press-Ons?'}
          @sut.findAndUpdate {query, update}, (error, @data) => done error

        it 'should return the previous version of the document', ->
          expect(@data).to.deep.equal uuid: 'hardware', byline: 'Does it grate?'

        it 'should update the thing', (done) ->
          @sut.findOne uuid: 'hardware', (error, record) =>
            return done error if error?
            expect(record).to.containSubset
              uuid: 'hardware'
              byline: 'Lee Press-Ons?'
            done()

        it 'should clear the cache', (done) ->
          @redis.exists '9f0f2e3f4d49c05e64727e8993f152f775e1f317', (error, exists) =>
            return done error if error?
            expect(exists).to.equal 0
            done()
