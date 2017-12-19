namespace FSharp.Sql

[<RequireQualifiedAccess>]
module Sql =

    open FSharp.Sql
    open System.Data
    open System.Data.Common
    open FSharp.Reflection

    let run conn (SqlAction action) =
        action conn

    let ok x =
        SqlAction (fun _ -> Async.singleton (Ok x))

    let fail exn =
        SqlAction (fun _ -> Async.singleton (Error exn))

    let failWithMessage msg = fail (FSharp.Sql.SqlException msg)

    let bind (f: 'a -> SqlAction<'b>) (action: SqlAction<'a>): SqlAction<'b> =
        let newAction ctx = async {
            let! result = run ctx action
            match result with
            | Ok r ->
                let action = f r
                let! result = run ctx action
                return result
            | Error xs ->
                return Error xs
        }
        SqlAction newAction

    let apply (fAction: SqlAction<'a -> 'b>) (xAction: SqlAction<'a>): SqlAction<'b> =
        let newAction conn = async {
            let! fa = run conn fAction
            let! xa = run conn xAction
            return Result.map2 (fun f a -> f a) fa xa
        }
        SqlAction newAction

    let map (f: 'a -> 'b) (action: SqlAction<'a>): SqlAction<'b> =
        let newAction conn =
            run conn action
            |> Async.map (Result.bind (Ok << f))
        SqlAction newAction

    let defaultCtx conn =
        { Connection = conn
          Transaction = null
          CommandTimeout = 180 }

    /// Execute the SqlAction.
    let execute (createConnection: unit -> #DbConnection) action = async {
        use conn = createConnection ()
        conn.Open()
        let ctx = defaultCtx conn
        let! result = run ctx action
        conn.Close()
        return result
    }

    /// Execute the SqlAction in a transaction provided by initTransaction.
    let executeWithInitTransaction (createConnection: unit -> #DbConnection) initTransaction action = async {
        use conn = createConnection ()
        conn.Open()
        use transaction = initTransaction conn
        let ctx =
            { defaultCtx conn with
                Transaction = transaction }
        let! result = run ctx action
        match result with
        | Ok _ -> transaction.Commit()
        | Error _ -> transaction.Rollback()
        conn.Close()
        return result
    }

    /// <summary>Execute the SqlAction in a transaction.</summary>
    /// <remarks>The transaction isolation level is "read uncommitted", which mimics the NOSQL behavior.</remarks>
    let executeWithTransaction createConnection action =
        executeWithInitTransaction createConnection
            (fun conn -> conn.BeginTransaction(IsolationLevel.ReadUncommitted))
            action

    let (|AggregateException|_|) (exn: exn) =
        match exn with
        | :? System.AggregateException as exn ->
            Some (Seq.toList (exn.InnerExceptions))
        | _ -> None

    let (|SqlException|_|) (exn: exn) =
        match exn with
        | :? SqlClient.SqlException as exn ->
            Some exn
        | _ -> None

    let tryExecute f =
        let execute ctx: Async<Result<_, exn>> = async {
            try
                let! r = f ctx
                return Ok r
            with
            | AggregateException [SqlException exn]
            | SqlException exn ->
                return Core.Error (upcast (FSharp.Sql.SqlException(exn.Message, exn)))
        }
        SqlAction execute

    let logQuery sql =
        sprintf "#### BEGIN QUERY ####\n%s\n#### END QUERY ####" sql

    let commandWith (parameters: (string * obj) list) sql (ctx: SqlContext) =
        let cmd = ctx.Connection.CreateCommand()
        cmd.CommandText <- sql
        cmd.Transaction <- ctx.Transaction
        cmd.CommandTimeout  <- ctx.CommandTimeout
        parameters |> List.iter (ignore << cmd.Parameters.Add)
        cmd

    let executeNonQueryWith parameters sql =
        tryExecute (fun ctx ->
            use cmd = commandWith parameters sql ctx
            cmd.AsyncExecuteNonQuery())

    let executeNonQuery =
        executeNonQueryWith []

    let executeScalarWith<'T> parameters sql: SqlAction<'T> =
        tryExecute (fun ctx -> async {
            use cmd = commandWith parameters sql ctx
            let! res = cmd.AsyncExecuteScalar()
            return unbox<'T> res
        })

    let executeScalar<'T> =
        executeScalarWith<'T> []

    let field name typ = { Name = name; Type = typ }

    let createTableQuery (Table (schema, table)) (fields: Field list) =
        let fields = String.concat ", " (List.map string fields)
        sprintf "CREATE TABLE [%s].[%s] (%s)" schema table fields

    let create table fields =
        createTableQuery table fields
        |> executeNonQuery

    let count (Table (schema, table)) =
        sprintf "SELECT COUNT(*) FROM [%s].[%s]" schema table
        |> executeScalar

    let createIndex (indexType: IndexType) idxName (Table (schema, table)) (fields: Field list) =
        let fields = String.concat ", " (List.map (fun f -> sprintf "[%s]" f.Name) fields)
        let indexType =
            match indexType with
            | Clustered -> "CLUSTERED"
            | NonClustered -> "NONCLUSTERED"
        sprintf """CREATE %s INDEX [%s] ON [%s].[%s] (%s)""" indexType idxName schema table fields
        |> executeNonQuery

    let toSqlDataReader (enumerable: seq<'T>) =
        if FSharpType.IsRecord typeof<'T> then
            let fields =
                FSharpType.GetRecordFields(typeof<'T>)
                |> Array.map (fun p -> p.Name)
            FastMember.ObjectReader.Create(enumerable, fields)
        else
            FastMember.ObjectReader.Create(enumerable)

    let inline fromDBNull (x: obj) =
        if obj.Equals(x, System.DBNull.Value)
        then Unchecked.defaultof<_>
        else x

    let readAllWith<'T> parameters sql =
        let readAll ctx = seq {
            if not (FSharpType.IsRecord(typeof<'T>)) then
                invalidOp (sprintf "'%s' is not an F# record" typeof<'T>.FullName)
            let init = FSharpValue.PreComputeRecordConstructor(typeof<'T>)
            let fieldCount = FSharpType.GetRecordFields(typeof<'T>).Length
            let cmd = commandWith parameters sql ctx
            use rdr = cmd.ExecuteReader()
            while rdr.Read() do
                let arr = Array.zeroCreate fieldCount
                rdr.GetValues(arr) |> ignore
                for i in 0..arr.Length-1 do
                    arr.[i] <- fromDBNull arr.[i]
                yield unbox<'T> (init arr)
        }
        tryExecute (Async.singleton << readAll)

    let readAllBy<'T> =
        readAllWith<'T> []

    let getFields<'T> =
        FSharpType.GetRecordFields(typeof<'T>)
        |> Array.map (fun f -> sprintf "[%s]" f.Name)
        |> String.concat ", "

    let readAll<'T> (Table (schema, table)) =
        sprintf "SELECT %s FROM [%s].[%s]" getFields<'T> schema table
        |> readAllBy<'T>

    let drop (Table (schema, table)) =
        sprintf "DROP TABLE [%s].[%s]" schema table
        |> executeNonQuery

    let rename (Table (oschema, otable)) (Table (nschema, ntable)) =
        sprintf "EXEC sp_rename '[%s].[%s]', '[%s].[%s]'" oschema otable nschema ntable
        |> executeNonQuery

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
        |> readAllWith<Statistics> parameters
        |> map (Seq.toList >> Seq.head)
