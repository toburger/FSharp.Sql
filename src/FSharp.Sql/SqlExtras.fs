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

    /// <remarks>
    /// Not tail recursive!
    /// </remarks>
    let rec traverse f ls =
        match ls with
        | [] -> Sql.ok []
        | x::xs ->
            let hd = f x
            let rest = traverse f xs
            Sql.bind (fun h -> Sql.map (fun ls -> h::ls) rest) hd

    /// <remarks>
    /// Not tail recursive!
    /// </remarks>
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
