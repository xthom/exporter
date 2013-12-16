
require("cson-config").load()

args = (require "optimist").argv 
mysql = require "mysql"
sql = (require "sql").setDialect "mysql"
async = require "async"
fs = require "fs"
moment = require "moment"

config = process.config

date = moment().format "YYYY-MM-DD"

directory = process.cwd()

callback = (err) -> 
  console.error err.message if err
  console.log "\nExport finished successfully" unless err
  process.exit 0

onConnectionError = (err) ->
  console.log "Connection error!"
  console.error err

exportData = (exp, cb) ->
  console.log "Starting #{exp.title} export"
  connection = mysql.createConnection exp.connection
  connection.on "error", onConnectionError
  connection.query "SET NAMES UTF8", (err, result) ->
    callback err if err
    connection.query "SHOW TABLES", (err, result) ->
      callback err if err
      tables = []
      if exp.table?
        tables = [exp.table]
      else
        tables.push tableData["Tables_in_#{exp.title}"] for tableData in result

      async.eachSeries tables, (tableName, cb) ->
        connection.query "SELECT COUNT(*) AS c FROM #{tableName}", (err, cnt) ->
          cnt = cnt[0].c
          i = 0
          console.log "Exporting table #{tableName} (#{cnt} rows)"
          fn = "#{directory}/#{exp.title}-#{tableName}.json"
          query = connection.query "SELECT * FROM #{tableName}"
          query.on "error", cb
          query.on "fields", (fields) ->
            scheme = []
            for field in fields
              obj = {}
              obj[key] = field[key] for key in ["db", "table", "name", "length", "type", "default", "fieldLength", "zeroFill"]
              scheme.push obj
            schemeData = JSON.stringify scheme
            schemeFn = "#{directory}/#{exp.title}-#{tableName}.scheme.json"
            process.stdout.write "\nWriting scheme of #{tableName} into #{schemeFn}"
            fs.writeFileSync schemeFn, "#{schemeData}"
          query.on "result", (result) ->
            connection.pause()
            data = JSON.stringify result
            i++
            process.stdout.write "... exporting #{Math.round((i/cnt)*100)}%\r"
            if i > 1 and fs.existsSync fn
              fs.appendFileSync fn, ",#{data}"
            else
              fs.writeFileSync fn, "[#{data}"
            connection.resume()
          query.on "end", ->
            fs.appendFileSync fn, "]" if fs.existsSync fn
            console.log "Table #{tableName} data saved to file #{fn}"
            cb()
      , cb
  console.log "--"

if args.list or 'list' in args._
  console.log Object.keys config.exports
  process.exit 0

if args.dir? and args.dir.match /^\/?.+/
  directory += "/#{args.dir}"
  directory = args.dir if args.dir.match /^\//
  if args.dir.match /\/{2}$/
    directory = directory.replace /\/{2}$/, '/'
    directory += "#{date}" 
else
  defaultDirectory = "#{__dirname}/exports"
  unless fs.existsSync defaultDirectory
    try
      fs.mkdirSync defaultDirectory
    catch err
      return callback err
  directory = "#{defaultDirectory}/#{date}"

try
  fs.mkdirSync directory
catch err
  callback err unless err.errno is 47 # file exists

if args.db? and config.exports[args.db]
  exps = [config.exports[args.db]]
else
  exps = Object.keys(config.exports).map((k) -> config.exports[k])

async.eachSeries exps, exportData, callback
