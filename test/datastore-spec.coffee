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
          @redis.get 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal {
              '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '30f087a9660c39a36cc66cb0e8563f8f5a437bb0'
              '9c77a994790ddf88bc197b11091643662c999a30': 'd3eea2b92d181957905932ada3c7366d98a316c1'
            }
            done()

        it 'should add to the cache', (done) ->
          @redis.hget '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '30f087a9660c39a36cc66cb0e8563f8f5a437bb0', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
            done()

        it 'should add the other to the cache', (done) ->
          @redis.hget '9c77a994790ddf88bc197b11091643662c999a30', 'd3eea2b92d181957905932ada3c7366d98a316c1', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'
            done()

    describe 'when a record is already cached', ->
      beforeEach (done) ->
        data =
          '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '30f087a9660c39a36cc66cb0e8563f8f5a437bb0'
          '9c77a994790ddf88bc197b11091643662c999a30': 'd3eea2b92d181957905932ada3c7366d98a316c1'

        @redis.set 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
        @redis.hset '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '30f087a9660c39a36cc66cb0e8563f8f5a437bb0', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'marshmellow', type: 'campfire', token: 'How long can you hold your hand in the fire?'
        @redis.hset '9c77a994790ddf88bc197b11091643662c999a30', 'd3eea2b92d181957905932ada3c7366d98a316c1', JSON.stringify(data), done

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
          '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': '30f087a9660c39a36cc66cb0e8563f8f5a437bb0'
          '9c77a994790ddf88bc197b11091643662c999a30': 'd3eea2b92d181957905932ada3c7366d98a316c1'

        @redis.set 'b098a0f435aa7818a057fc4aa21aa0775e74a09e', JSON.stringify(data), done

      beforeEach (done) ->
        data = uuid: 'wood', type: 'campfire', token: 'I bet you can\'t jump over it'
        @redis.hset '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', '30f087a9660c39a36cc66cb0e8563f8f5a437bb0', JSON.stringify(data), done

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
          @redis.get '592e16c142809fcd4f5930e933f79a3980939f49', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal {
              '3205fe0fa790bfe1039f95ba0bba03eec1faa05c': 'bf3c374b834e5a47e7d722e3e05c9e4cd8badc10'
              '9c77a994790ddf88bc197b11091643662c999a30': '82129b82991f50ee3ae73e2e82d81819cf63f9a7'
            }
            done()

        it 'should add to the cache', (done) ->
          @redis.hget '3205fe0fa790bfe1039f95ba0bba03eec1faa05c', 'bf3c374b834e5a47e7d722e3e05c9e4cd8badc10', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'wood', type: 'campfire'
            done()

        it 'should add the other to the cache', (done) ->
          @redis.hget '9c77a994790ddf88bc197b11091643662c999a30', '82129b82991f50ee3ae73e2e82d81819cf63f9a7', (error, data) =>
            return done error if error?
            expect(JSON.parse data).to.deep.equal uuid: 'marshmellow', type: 'campfire'
            done()

    describe 'when there exists no thing', ->
      beforeEach (done) ->
        @sut.find type: 'nobody cares', (error, @result) => done error

      it 'should yield a empty array', ->
        expect(@result).to.be.empty
        expect(@result).to.be.array

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

  describe '->upsert', ->
    describe 'when called with an object', ->
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

  describe '->update', ->
    describe 'when an object exists', ->
      beforeEach (done) ->
        @redis.hset '9f0f2e3f4d49c05e64727e8993f152f775e1f317', 'foo', 'bar', done

      beforeEach (done) ->
        @sut.insert uuid: 'hardware', byline: 'Does it grate?', done

      describe 'when called with an object', ->
        beforeEach (done) ->
          query  = uuid: 'hardware'
          update = $set: {byline: 'Lee Press-Ons?'}
          @sut.update query, update, (error) => done error

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
