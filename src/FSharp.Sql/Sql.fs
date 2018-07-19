namespace FSharp.Sql

[<RequireQualifiedAccess>]
module Sql =

    open FSharp.Sql
    open System.Data
    open System.Data.Common

    let inline run ctx (SqlAction action) =
        action ctx

    let inline ok x =
        SqlAction (fun _ -> async.Return (Ok x))

    let inline fail exn =
        SqlAction (fun _ -> async.Return (Error exn))

    let failWithMessage msg = fail (SqlException msg)

    let inline bind (f: 'a -> SqlAction<'b>) (action: SqlAction<'a>): SqlAction<'b> =
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

    let inline apply (fAction: SqlAction<'a -> 'b>) (xAction: SqlAction<'a>): SqlAction<'b> =
        let newAction conn = async {
            let! fa = run conn fAction
            let! xa = run conn xAction
            return Result.map2 (fun f a -> f a) fa xa
        }
        SqlAction newAction

    let inline map (f: 'a -> 'b) (action: SqlAction<'a>): SqlAction<'b> =
        let newAction conn =
            run conn action
            |> Async.map (Result.bind (Ok << f))
        SqlAction newAction

    let defaultCtx conn =
        { Connection = conn
          Transaction = null
          CommandTimeout = Some 180 }

    let private withContext f action =
        SqlAction (fun ctx -> run (f ctx) action)

    let withTransaction transaction =
        withContext (fun ctx ->
            { ctx with Transaction = transaction })

    let withCommandTimeout timeout =
        withContext (fun ctx ->
            { ctx with CommandTimeout = timeout })

    /// Execute the SqlAction.
    let execute (createConnection: unit -> #IDbConnection) action = async {
        use conn = createConnection ()
        conn.Open()
        let ctx = defaultCtx conn
        let! result = run ctx action
        conn.Close()
        return result
    }

    /// Execute the SqlAction in a transaction provided by initTransaction.
    let executeWithInitTransaction (createConnection: unit -> #IDbConnection) initTransaction action = async {
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
        | :? DbException as exn ->
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
                return Error (upcast (SqlException(exn.Message, exn)))
        }
        SqlAction execute
