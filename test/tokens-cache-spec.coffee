{describe,beforeEach,expect,it} = global
mongojs   = require 'mongojs'
Datastore = require '../src/datastore'
Cache     = require 'meshblu-core-cache'
redis     = require 'fakeredis'
RedisNS   = require '@octoblu/redis-ns'
UUID      = require 'uuid'

describe 'Tokens cache', ->
  beforeEach (done) ->
    redisClient = new RedisNS "datastore:test:tokens:#{UUID.v4()}", redis.createClient(UUID.v4(), { fast : true })
    @cache = new Cache { client: redisClient }
    @database = mongojs 'datastore-test', ['tokens']
    @sut = new Datastore {
      @database
      @cache
      collection: 'tokens'
      cacheAttributes: ['uuid', 'hashedToken']
    }
    @database.tokens.remove done
    return # redis fix

  describe 'when the token record already exists', ->
    beforeEach (done) ->
      record =
        uuid: 'some-random-uuid'
        hashedToken: 'some-hashed-token-str'
      @sut.insert record, done

    beforeEach (done) ->
      query = { uuid: 'some-random-uuid', hashedToken: 'some-hashed-token-str' }
      @sut.findOne query, { uuid: true }, (error, @result) => done error

    it 'should yield the record without mongo stuff', ->
      expect(@result).to.deep.equal {
        uuid: 'some-random-uuid'
        hashedToken: 'some-hashed-token-str'
      }

    it 'should add to the cache', (done) ->
      @cache.hget '8dabae054ba9eab67952f209762b3f134fa4c45a', 'a85baad3356e89d84638a715209b56abf5ef6d6e', (error, data) =>
        return done error if error?
        expect(JSON.parse(data)).to.deep.equal { uuid: 'some-random-uuid', hashedToken: 'some-hashed-token-str' }
        done()
      return # redis fix

    it 'should have the ttl of one minute', (done) ->
      @cache.ttl '8dabae054ba9eab67952f209762b3f134fa4c45a', (error, ttl) =>
        return done error if error?
        max = 60 * 60
        min = max - 2
        expect(ttl).to.be.within(min, max)
        done()
      return # redis fix

    describe 'when updated', ->
      beforeEach (done) ->
        record =
          uuid: 'some-random-uuid'
          hashedToken: 'some-hashed-token-str'
        @sut.update record, { "$set": { super: "man" } }, done

      beforeEach (done) ->
        @sut.findOne { uuid: 'some-random-uuid', hashedToken: 'some-hashed-token-str' }, { uuid: true }, (error, @result) => done error

      it 'should have the new result', ->
        expect(@result).to.deep.equal {
          uuid: 'some-random-uuid'
          hashedToken: 'some-hashed-token-str',
        }

      it 'should extend the cache', (done) ->
        @cache.ttl '8dabae054ba9eab67952f209762b3f134fa4c45a', (error, ttl) =>
          return done error if error?
          max = 60 * 60
          min = max - 2
          expect(ttl).to.be.within(min, max)
          done()
        return # redis fix
