namespace FSharp.Sql.SqlClient

open System.Data.SqlClient
open FSharp.Sql

type SqlAction<'a> = SqlAction<SqlConnection, SqlTransaction, 'a>

[<RequireQualifiedAccess>]
module Sql =
    open Microsoft.FSharp.Reflection

    let toSqlDataReader (enumerable: seq<'T>) =
        if FSharpType.IsRecord typeof<'T> then
            let fields =
                FSharpType.GetRecordFields(typeof<'T>)
                |> Array.map (fun p -> p.Name)
            FastMember.ObjectReader.Create(enumerable, fields)
        else
            FastMember.ObjectReader.Create(enumerable)

    let bulkInsert (Table (schema, table)) data =
        let bulkInsert ctx =
            use bulk =
                new SqlBulkCopy
                    (connection = ctx.Connection,
                     copyOptions = SqlBulkCopyOptions.Default,
                     externalTransaction = ctx.Transaction,
                     DestinationTableName = sprintf "[%s].[%s]" schema table,
                     EnableStreaming = true,
                     NotifyAfter = 50000,
                     BulkCopyTimeout = 0)
            bulk.SqlRowsCopied.Add(ignore)
            use rdr = toSqlDataReader data
            bulk.WriteToServer(rdr) // do not use Async version, it's twice as slow
        Sql.tryExecute (async.Return << bulkInsert)

    let explain action: SqlAction<string> =
        sql {
            let! _ = Sql.executeNonQuery "SET SHOWPLAN_XML ON"
            let! (result: string) = action
            let! _ = Sql.executeNonQuery "SET SHOWPLAN_XML OFF"
            return result
        }

    let explains actions: SqlAction<Result<string, exn> list> =
        sql {
            let! _ = Sql.executeNonQuery "SET SHOWPLAN_XML ON"
            let! results =
                actions
                |> SqlExtras.sequence
                |> Sql.map Seq.toList
            let! _ = Sql.executeNonQuery "SET SHOWPLAN_XML OFF"
            return results
        }
