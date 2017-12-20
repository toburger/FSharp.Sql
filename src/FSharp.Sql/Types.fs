namespace FSharp.Sql

open System.Data

type SqlException(message, innerException: exn) =
    inherit exn(message, innerException)
    new message = SqlException(message, null)

type SqlContext<'DbConnection, 'DbTransaction when 'DbConnection :> IDbConnection and 'DbTransaction :> IDbTransaction> =
    { Connection: 'DbConnection
      Transaction: 'DbTransaction
      CommandTimeout: int }

type SqlAction<'DbConnection, 'DbTransaction, 'a when 'DbConnection :> IDbConnection and 'DbTransaction :> IDbTransaction> =
    SqlAction of (SqlContext<'DbConnection, 'DbTransaction> -> Async<Result<'a, exn>>)

type Table = Table of schema: string * name:string
with
    override self.ToString() =
        let (Table (schema, name)) = self
        sprintf "[%s].[%s]" schema name

type Type =
    | Bit
    | Int
    | Bigint
    | Smallint
    | Tinyint
    | Char of length: int option
    | Varchar of length: int
    | NChar of length: int option
    | NVarchar of length: int
    | Text
    | NText
    | Decimal of precision: int * scale: int option
    | Money
    | Smallmoney
    | DateTime
    | Date
    | Time
    | Guid
with
    override self.ToString() =
        match self with
        | Bit -> "bit"
        | Int -> "int"
        | Bigint -> "bigint"
        | Smallint -> "smallint"
        | Tinyint -> "tinyint"
        | Char None -> "char"
        | Char (Some l) -> sprintf "char(%i)" l
        | Varchar l -> sprintf "varchar(%i)" l
        | NChar None -> "nchar"
        | NChar (Some l) -> sprintf "nchar(%i)" l
        | NVarchar l -> sprintf "nvarchar(%i)" l
        | Text -> "text"
        | NText -> "ntext"
        | Decimal (p, Some s) -> sprintf "decimal(%i,%i)" p s
        | Decimal (p, None) -> sprintf "decimal(%i)" p
        | Money -> "money"
        | Smallmoney -> "smallmoney"
        | DateTime -> "datetime"
        | Date -> "date"
        | Time -> "time"
        | Guid -> "uniqueidentifier"

type Field =
    { Name: string
      Type: Type }
with
    override self.ToString() =
        sprintf "[%s] %O" self.Name self.Type

type IndexType =
    | Clustered
    | NonClustered

type Statistics =
    { Inserts: int; Updates: int; Deletes: int }
with override self.ToString() =
        sprintf "Inserts: %i, Updates: %i, Deletes: %i"
                self.Inserts self.Updates self.Deletes
     static member (+) (stat1, stat2) =
        { Inserts = stat1.Inserts + stat2.Inserts
          Updates = stat1.Updates + stat2.Updates
          Deletes = stat1.Deletes + stat2.Deletes }

[<AutoOpen>]
module Extensions =
    open System.Data
    open System.Data.Common

    let cancellationTokenOrDefault cancellationToken =
        defaultArg cancellationToken Async.DefaultCancellationToken

    type IDbCommand with
        member self.AsyncExecuteScalar(?cancellationToken) =
            Async.AwaitTask (self.ExecuteScalarAsync(cancellationTokenOrDefault cancellationToken))
        member self.AsyncExecuteNonQuery(?cancellationToken) =
            Async.AwaitTask (self.ExecuteNonQueryAsync(cancellationTokenOrDefault cancellationToken))
        member self.AsyncExecuteReader(?cancellationToken) =
            Async.AwaitTask (self.ExecuteReaderAsync(cancellationTokenOrDefault cancellationToken))

    type DbDataReader with
        member self.AsyncRead(?cancellationToken) =
            Async.AwaitTask (self.ReadAsync(cancellationTokenOrDefault cancellationToken))
