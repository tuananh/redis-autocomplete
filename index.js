/*
    Proof of concept to use redis for autocomplete
*/
'use strict'

let debug = require('debug')('redis-autocomplete'),
    Hoek = require('hoek'),
    Promise = require('bluebird'),
    Redis = require('ioredis'),
    redis = new Redis({ parser: 'hiredis', dropBufferSupport: true }),
    LineByLineReader = require('line-by-line')

redis.exists(':compl')
    .then(function(reply) {
        debug('exists ?', reply)
        if (!reply) {
            return buildIndex()
        } else {
            debug('data already exists!')
            return true
        }
    })
    .then(function(reply) {
        debug('done pushing', reply)
        var timerObj = new Hoek.Timer()
        debug("start looking for autocomplete at: " + timerObj.ts)
        autocomplete('mary', 50)
            .then(function(reply) {
                debug('possible autocomplete', reply)
                debug("Autocomplete completes in: " + timerObj.elapsed() + ' milliseconds')
            })
    })
    .catch(console.log)


function buildIndex() {
    debug('pushing')

    return new Promise(function(resolve, reject) {
        var pipeline = redis.pipeline()
        var lr = new LineByLineReader('./data/names.txt')

        lr.on('line', function(line) {
            processLine(line, pipeline)
        })

        lr.on('end', function() {
            debug('on end')
            resolve(pipeline.exec())
        })

        lr.on('error', function(err) {
            reject(new Error(`Unexpected error while reading file ${err}`))
        })
    })
}

function processLine(name, pipeline) {
    // debug('processLine', name)
    let prefix = ''

    // for each word, add all combination; and finally the word itself with an
    //  asterik to mark this is a complete name

    for (let i = 0; i < name.length; ++i) {
        prefix = name.substring(0, i)
        pipeline.zadd(':compl', 0, prefix)
    }
    pipeline.zadd(':compl', 0, name + '*')
}

function autocomplete(prefix, count) {
    debug('autocomplete', prefix, count)
    let rangelen = 50,
        result = []

    return redis.zrank(':compl', prefix)
        .bind({})
        .then(function(start) {
            debug('start', start)
            return redis.zrange(':compl', start, start + rangelen - 1)
        })
        .then(function(result) {
            debug('result', result)
            if (result) {
                // filtering
                let completeWords = []
                for (let i = 0; i < result.length; ++i) {
                    if (result[i].indexOf('*') !== -1 && result[i].substring(0, prefix.length) === prefix) {
                        completeWords.push(result[i])
                    }
                }

                return completeWords
            } else {
                throw new Error('No matched found')
            }
        })
}
