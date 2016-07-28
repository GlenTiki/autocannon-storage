#! /usr/bin/env node

'use strict'

const commist = require('commist')
const minimist = require('minimist')
const ndjson = require('ndjson')
const aws = require('aws-sdk')
const s3blobs = require('s3-blob-store')
const fs = require('fs')
const path = require('path')
const steed = require('steed')
const semver = require('semver')
const createStore = require('./lib/storage')
const help = fs.readFileSync(path.join(__dirname, 'help.txt'), 'utf8')

function runCmd () {
  let store

  commist()
    .register('save', _handleSave)
    .register('load', _handleLoad)
    .parse(process.argv.splice(2))

  function _handleSave (args) {
    const argv = _handleArgs(args)

    store = createStore(createS3Store(argv.credentials, argv.bucket))
    const ndjsonStream = ndjson.parse()

    if (process.stdin.isTTY) {
      if (!argv.input) throw new Error('input file needed when not piping in')

      fs.createReadStream(argv.input)
        .pipe(ndjsonStream)
        .on('data', _handleJsonSave(argv))
    } else {
      process.stdin
        .pipe(ndjsonStream)
        .on('data', _handleJsonSave(argv))
    }
  }

  function _handleJsonSave (argv) {
    return (json) => {
      json.tag = argv.tag
      console.log('starting to save')
      store.save(json, (err) => {
        if (err) throw new Error('save failed', err)
        console.log('saved successfully')
      })
    }
  }

  function _handleLoad (args) {
    const argv = _handleArgs(args)
    store = createStore(createS3Store(argv.credentials, argv.bucket))

    if (!argv.tag) store.load({ amt: argv.amount }, _handleJsonLoad(argv))
    else {
      store.filter({
        amt: argv.amount,
        filter: (elem) => semver.satisfies(elem.tag, argv.tag)
      }, _handleJsonLoad(argv))
    }
  }

  function _handleJsonLoad (argv) {
    return (err, results) => {
      if (err) throw new Error(err)

      if (argv.output) {
        steed.map({}, results, (res, done) => {
          fs.writeFile(`${res.finish}.json`, JSON.stringify(res), done)
        }, (err) => {
          if (err) throw new Error(err)
          else console.error('wrote results to file')
        })
      }

      results.forEach((result) => {
        console.log(JSON.stringify(result))
      })
    }
  }
}

function createS3Store (credentials, bucket) {
  const client = new aws.S3({
    accessKeyId: credentials.accessKey,
    secretAccessKey: credentials.secretKey
  })

  const store = s3blobs({
    client: client,
    bucket: bucket
  })

  return store
}

function _handleArgs (args) {
  const argv = minimist(args, {
    boolean: ['help', 'version'],
    alias: {
      input: 'i',
      output: 'o',
      credentials: 'c',
      accessKey: 'k',
      secretKey: 'K',
      bucket: 'b',
      amount: 'a',
      tag: 't',
      version: 'v',
      help: 'h'
    },
    default: {
      bucket: 'autocannon-results',
      amount: 0
    }
  })

  if (argv.version) {
    console.error('autocannon-storage', 'v' + require('./package').version)
    console.error('node', process.version)
    return process.exit(0)
  }

  if (argv.help) {
    console.error(help)
    return process.exit(0)
  }

  if (!argv.credentials || !(argv.accessKey && argv.secretKey) ||
      !(process.env.S3_ACCESS_KEY && process.env.S3_SECRET_KEY)) {
    throw new Error('Credentials needed for S3 store')
  }

  if (argv.credentials) {
    argv.credentials = path.isAbsolute(argv.credentials) ? argv.credentials : path.join(process.cwd(), argv.credentials)
    argv.credentials = JSON.parse(fs.readFileSync(argv.credentials).toString())
  } else {
    argv.credentials = {
      accessKey: argv.accessKey || process.env.S3_ACCESS_KEY,
      secretKey: argv.secretKey || process.env.S3_SECRET_KEY
    }
  }

  if (argv.output && !path.isAbsolute(argv.output)) argv.output = path.join(process.cwd, argv.output)

  return argv
}

if (require.main === module) {
  runCmd()
}

module.exports.createStore = createStore
module.exports.createS3Store = createS3Store
