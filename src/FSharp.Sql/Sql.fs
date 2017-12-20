namespace FSharp.Sql

[<RequireQualifiedAccess>]
module Sql =

    open FSharp.Sql
    open System.Data
    open System.Data.Common

    let run conn (SqlAction action) =
        action conn

    let ok x =
        SqlAction (fun _ -> Async.singleton (Ok x))

    let fail exn =
        SqlAction (fun _ -> Async.singleton (Error exn))

    let failWithMessage msg = fail (SqlException msg)

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
