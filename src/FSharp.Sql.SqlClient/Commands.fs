namespace FSharp.Sql.SqlClient

open System.Data.SqlClient
open FSharp.Sql

type SqlAction<'a> = SqlAction<SqlConnection, SqlTransaction, 'a>

[<RequireQualifiedAccess>]
module Command =
    open Microsoft.FSharp.Reflection

    let toSqlDataReader (enumerable: seq<'T>) =
        if FSharpType.IsRecord typeof<'T> then
            let fields =
                FSharpType.GetRecordFields(typeof<'T>)
                |> Array.map (fun p -> p.Name)
            FastMember.ObjectReader.Create(enumerable, fields)
        else
            FastMember.ObjectReader.Create(enumerable)

    let bulkInsert (table: Table) data =
        let bulkInsert ctx =
            use bulk =
                new SqlBulkCopy
                    (connection = ctx.Connection,
                     copyOptions = SqlBulkCopyOptions.Default,
                     externalTransaction = ctx.Transaction,
                     DestinationTableName = (table.GetString()),
                     EnableStreaming = true,
                     NotifyAfter = 50000,
                     BulkCopyTimeout = 0)
            bulk.SqlRowsCopied.Add(ignore)
            use rdr = toSqlDataReader data
            bulk.WriteToServer(rdr) // do not use Async version, it's twice as slow
        Sql.tryExecute (async.Return << bulkInsert)

    let explain action: SqlAction<string> =
        sql {
            let! _ = Command.executeNonQuery "SET SHOWPLAN_XML ON"
            let! (result: string) = action
            let! _ = Command.executeNonQuery "SET SHOWPLAN_XML OFF"
            return result
        }

    let explains actions: SqlAction<Result<string, exn> list> =
        sql {
            let! _ = Command.executeNonQuery "SET SHOWPLAN_XML ON"
            let! results =
                actions
                |> SqlExtras.sequence
                |> Sql.map Seq.toList
            let! _ = Command.executeNonQuery "SET SHOWPLAN_XML OFF"
            return results
        }

    let mergeWithStatistics parameters mergeQuery =
        sprintf """DECLARE
        @mergeResultsTable TABLE (MergeAction varchar(20));

    DECLARE
        @insertCount int,
        @updateCount int,
        @deleteCount int;

    %s
    OUTPUT $action INTO @mergeResultsTable;

    SELECT @insertCount = [INSERT],
           @updateCount = [UPDATE],
           @deleteCount = [DELETE]
    FROM (SELECT 'NOOP' MergeAction -- row for null merge into null
          UNION ALL
          SELECT * FROM @mergeResultsTable) mergeResultsPlusEmptyRow
    PIVOT (COUNT(MergeAction)
        FOR MergeAction IN ([INSERT],[UPDATE],[DELETE]))
        AS mergeResultsPivot;

    SELECT @insertCount Insertions, @updateCount Updates, @deleteCount Deletions;""" mergeQuery
        |> Command.readAllWith<Statistics> parameters
        |> Sql.map (Seq.toList >> Seq.head)
