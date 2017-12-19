namespace FSharp.Sql.SqlClient

open System.Data.SqlClient
open FSharp.Sql

[<RequireQualifiedAccess>]
module Sql =
    let bulkInsert (Table (schema, table)) data =
        let bulkInsert ctx =
            use bulk =
                new SqlBulkCopy
                    (connection = (ctx.Connection :?> SqlConnection),
                     copyOptions = SqlBulkCopyOptions.Default,
                     externalTransaction = (ctx.Transaction :?> SqlTransaction),
                     DestinationTableName = sprintf "[%s].[%s]" schema table,
                     EnableStreaming = true,
                     NotifyAfter = 50000,
                     BulkCopyTimeout = 0)
            bulk.SqlRowsCopied.Add(ignore)
            use rdr = Sql.toSqlDataReader data
            bulk.WriteToServer(rdr) // do not use Async version, it's twice as slow
        Sql.tryExecute (async.Return << bulkInsert)
