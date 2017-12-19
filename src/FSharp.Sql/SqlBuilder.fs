namespace FSharp.Sql

type SqlActionBuilder() =
    member __.Bind(m, f) = Sql.bind f m
    member __.Return(v) = Sql.ok v
    member __.ReturnFrom(m) = m
    member __.Delay(f) = f
    member __.Run(f) = f()
    member __.Zero() = Sql.ok ()
    member __.TryFinally(SqlAction asyncResult, compensation : unit -> unit) : SqlAction<_, _, 'a> =
        SqlAction (fun ctx -> async.TryFinally(asyncResult ctx, compensation))
    member self.Using(resource : 'T when 'T :> System.IDisposable, binder : 'T -> SqlAction<_, _, 'a>) : SqlAction<_, _, 'a> =
        self.TryFinally(binder resource, fun _ -> resource.Dispose())

[<AutoOpen>]
module Builder =
    let sql = SqlActionBuilder()
