namespace FSharp.Sql.SqlClient

open System.Data
open System.Data.SqlClient

[<AutoOpen>]
module Extensions =

    let cancellationTokenOrDefault cancellationToken =
        defaultArg cancellationToken Async.DefaultCancellationToken

    type SqlBulkCopy with
        member self.AsyncWriteToServer(rows: IDataReader, ?cancellationToken) =
            Async.AwaitTask (self.WriteToServerAsync(rows, cancellationTokenOrDefault cancellationToken))
        member self.AsyncWriteToServer(table: DataTable, ?cancellationToken) =
            Async.AwaitTask (self.WriteToServerAsync(table, cancellationTokenOrDefault cancellationToken))
