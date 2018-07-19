namespace FSharp.Sql

type SqlActionBuilder() =
    member __.Zero(): SqlAction<unit> =
        Sql.ok ()
    member __.Delay(generator: unit -> SqlAction<'T>): SqlAction<'T> =
        SqlAction (fun ctx ->
            async.Delay(fun () ->
                Sql.run ctx (generator ())))
    member inline __.Return(value: 'T): SqlAction<'T> =
        Sql.ok value
    member inline __.ReturnFrom(computation: SqlAction<'T>): SqlAction<'T> =
        computation
    member inline __.Bind(computation: SqlAction<'T>, binder: 'T -> SqlAction<'U>): SqlAction<'U> =
        Sql.bind binder computation
    member inline __.TryFinally(SqlAction asyncResult, compensation : unit -> unit) : SqlAction<'a> =
        SqlAction (fun ctx -> async.TryFinally(asyncResult ctx, compensation))
    member self.Using(resource : 'T, binder: 'T -> SqlAction<'a>): SqlAction<'a> when 'T :> System.IDisposable =
        self.TryFinally(binder resource, fun _ -> resource.Dispose())
    member self.While(guard: unit -> bool, computation: SqlAction<unit>): SqlAction<unit> =
        if guard () then
            let mutable whileSql = Unchecked.defaultof<_>
            whileSql <-
                self.Bind(computation, (fun () ->
                    if guard () then
                        whileSql
                    else
                        self.Zero()))
            whileSql
        else
            self.Zero()
    member self.For(sequence: seq<'T>, body: 'T -> SqlAction<unit>): SqlAction<unit> =
        self.Using(
            sequence.GetEnumerator(),
            (fun ie ->
                self.While(
                    (fun () -> ie.MoveNext()),
                    self.Delay(fun () -> body ie.Current))) )
    member inline __.Combine(computation1: SqlAction<'T>, computation2: SqlAction<'U>): SqlAction<'U> =
        computation1 |> Sql.bind (fun _ -> computation2 |> Sql.map id)
    member inline __.TryWith(SqlAction asyncResult, catchHandler: exn -> SqlAction<'a>): SqlAction<'a> =
        SqlAction (fun ctx -> async.TryWith(asyncResult ctx, fun exn ->
            Sql.run ctx (catchHandler exn)))

[<AutoOpen>]
module SqlActionBuilderImpl =
    let sql = SqlActionBuilder()

