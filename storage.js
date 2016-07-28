'use strict'

const concat = require('concat-stream')
const steed = require('steed')
const from = require('from2-array')

function createStore (store, created, meta) {
  let metaData = { results: [], meta: meta }

  store.exists({ key: 'meta.json' }, function (err, exists) {
    if (err) created(new Error('Problem fetching metadata from store'))
    const ret = {
      save: save,
      load: load,
      filter: filter
    }

    if (exists) {
      store.createReadStream({ key: 'meta.json' })
        .pipe(concat((meta) => {
          metaData = JSON.parse(meta)
          created(null, ret)
        }))
    } else {
      created(null, ret)
    }
  })

  function save (results, done) {
    if (!Array.isArray(results)) results = [results]
    done = done || _noop
    steed.map({}, results, (result, cb) => {
      from([ new Buffer(JSON.stringify(result)) ])
        .pipe(store.createWriteStream({ key: result.finish }, (err, meta) => {
          if (err) return cb(new Error('Problem writing results to store'))

          metaData.results.push({ key: result.finish, tag: result.tag })

          from([ new Buffer(JSON.stringify(metaData)) ])
            .pipe(store.createWriteStream({ key: 'meta.json', data: metaData }, (err, metaness) => {
              // if this error is passed up, your file has been written to the store, but this
              // storage module won't reconise its there because its not in the metadata.
              // if your results are physically stored there but missing when filtering or
              // loading, it can be caused by a bad metadata write.
              if (err) return cb(new Error('Problem updating the store\'s metadata.', err))

              cb(null, meta)
            }))
        }))
    }, done)
  }

  function load (opts, done) {
    opts = opts || {}
    opts.amt = opts.amount
    if (opts.amt && opts.amt > metaData.results.length) {
      console.error('amount to load larger than result set, loading all.')
      opts.amt = metaData.results.length
    } else {
      opts.amt = metaData.results.length
    }

    done = done || _noop
    opts.sort = opts.sort || _byDate

    const toFetch = metaData.results.slice(metaData.results.length - opts.amt)

    _fetch(toFetch, opts.sort, done)
  }

  function filter (opts, done) {
    opts = opts || {}
    // just keep all elements if they don't pass a filterFn
    opts.filter = opts.filter || function () { return true }
    opts.sort = opts.sort || _byDate
    const toFetch = metaData.filter(opts.filter)

    _fetch(toFetch, opts.sort, done)
  }

  function _fetch (toFetch, sortFn, done) {
    let results = []

    toFetch.forEach((resMeta) => {
      store
        .createReadStream({ key: resMeta.key })
        .pipe(concat((d, e, cb) => {
          d = JSON.parse(d.toString())
          results.push(d)

          if (results.length === toFetch.length) {
            results = results.sort(sortFn)
            done(null, results)
          }
        }))
        .on('error', done)
    })
  }
}

function _byDate (a, b) {
  if (a.finish < b.finish) {
    return 1
  }
  if (a.finish > b.finish) {
    return -1
  }
  return 0
}

function _noop () {}

module.exports = createStore
