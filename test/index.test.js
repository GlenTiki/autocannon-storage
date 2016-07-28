'use strict'

const test = require('tap').test
const abstractBlobStore = require('abstract-blob-store')
const fsBlobStore = require('fs-blob-store')
const fs = require('fs')
const path = require('path')
const storage = require('../')
const arr = [{
  finish: new Date(100000),
  foo: 0
}, {
  finish: new Date(200000),
  foo: 1
}, {
  finish: new Date(300000),
  foo: 2
}].map((obj) => JSON.parse(JSON.stringify(obj)))

test('should save and load as expected', (t) => {
  storage.createStore(abstractBlobStore(), (err, store) => {
    t.error(err)
    store.save({ finish: new Date(), foo: 'bar' }, (err, result) => {
      t.error(err)
      t.ok(result)
      store.load({}, (err, results) => {
        t.error(err)
        t.ok(results)
        t.equal(results.length, 1)
        t.done()
      })
    })
  })
})

test('should save and load multiple results as expected', (t) => {
  storage.createStore(abstractBlobStore(), (err, store) => {
    t.error(err)

    store.save(arr, (err, result) => {
      t.error(err)
      t.ok(result)
      store.load({}, (err, results) => {
        t.error(err)
        t.ok(results)
        t.equal(results.length, 3)
        results.reverse().forEach((res, index) => {
          t.equal(res.finish, arr[index].finish.toString())
        })
        t.done()
      })
    })
  })
})

test('should save and load multiple times without corrupting the metadata file', (t) => {
  storage.createStore(fsBlobStore(__dirname), (err, store) => {
    t.error(err)
    store.save(arr[0], (err, result) => {
      t.error(err)
      t.ok(result)
      store.save(arr[1], (err, result) => {
        t.error(err)
        t.ok(result)
        storage.createStore(fsBlobStore(__dirname), (err, store) => {
          t.error(err)
          store.save(arr[2], (err, result) => {
            t.error(err)
            t.ok(result)
            store.load({}, (err, results) => {
              t.error(err)
              t.ok(results)
              t.equal(results.length, 3)
              t.done()
            })
          })
        })
      })
    })
  })
})

test('should save multiple multiple times without corrupting the metadata file', (t) => {
  const p = path.join(__dirname, 'meta.json')
  if (fs.statSync(p).isFile) fs.unlinkSync(p)

  storage.createStore(fsBlobStore(__dirname), (err, store) => {
    t.error(err)
    store.save(arr[0], (err, result) => {
      t.error(err)
      t.ok(result)
      store.save(arr[1], (err, result) => {
        t.error(err)
        t.ok(result)
        storage.createStore(fsBlobStore(__dirname), (err, store) => {
          t.error(err)
          store.save(arr[2], (err, result) => {
            t.error(err)
            t.ok(result)
            store.load({}, (err, results) => {
              t.error(err)
              t.ok(results)
              t.equal(results.length, 3)
              fs.unlinkSync(p)
              t.done()
            })
          })
        })
      })
    })
  })
})
