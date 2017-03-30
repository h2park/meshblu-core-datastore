{describe,beforeEach,it,expect} = global
mongojs   = require 'mongojs'
Datastore = require '../src/datastore'

describe 'Datastore cache stuff', ->
  beforeEach ->
    @sut = new Datastore
      database:        mongojs('datastore-test')
      collection:      'jalapenos'
      cacheAttributes: ['id', 'jalapId']

  describe '->_generateCacheKey', ->
    describe 'when the fields exist', ->
      beforeEach ->
        query =
          id: 'foo'
          jalapId: 'serano'

        @cacheKey = @sut._generateCacheKey {query}

      it 'should generate the proper key', ->
        expect(@cacheKey).to.equal '71e52b62d7f35e138983163d679121b5e5123f4d'

  describe 'when a field is missing', ->
    beforeEach ->
      query =
        id: 'foo'

      @cacheKey = @sut._generateCacheKey {query}

    it 'should return null', ->
      expect(@cacheKey).to.be.undefined
