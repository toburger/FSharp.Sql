namespace FSharp.Sql

[<RequireQualifiedAccess>]
module SqlExtras =

    open FSharp.Sql

    let startChild (sqlAction: SqlAction<'a>): SqlAction<SqlAction<_>> =
        let newAction (ctx: SqlContext) = async {
            let! res = Sql.run ctx sqlAction |> Async.StartChild
            return Ok (SqlAction (fun _ -> res))
        }
        SqlAction newAction

    let memoize f =
        let dict = System.Collections.Concurrent.ConcurrentDictionary()
        fun x -> sql {
            match dict.TryGetValue x with
            | true, result -> return result
            | false, _ ->
                let! result = f x
                dict.TryAdd(x, result) |> ignore
                return result
        }

    let traverse f (xs: seq<SqlAction<_>>) =
        let innerFn ctx = async {
            let ys = ResizeArray()
            for x in xs do
                let (SqlAction action) = x
                let! r = action ctx
                ys.Add (f r)
            return Ok (ys :> seq<_>)
        }
        SqlAction innerFn

    let sequence x = traverse id x

    let Parallel (xs: SqlAction<_> list) =
        SqlAction (fun ctx -> async {
            let! result = Async.Parallel (List.map (Sql.run ctx) xs)
            let errors = Array.choose (function Error err -> Some err | _ -> None) result
            if errors.Length > 0 then
                return Error (upcast System.AggregateException("Failed to run SQL actions in parallel", errors))
            else
                let succeeds = Array.choose (function Ok v -> Some v | _ -> None) result
                return Ok succeeds
        })

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
                |> sequence
                |> Sql.map Seq.toList
            let! _ = Sql.executeNonQuery "SET SHOWPLAN_XML OFF"
            return results
        }
